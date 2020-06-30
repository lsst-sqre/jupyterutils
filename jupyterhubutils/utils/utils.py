'''
Shared utility functions.
'''

import hashlib
import inspect
import logging
import os
import random
import requests

from collections import defaultdict
from eliot.stdlib import EliotHandler


def rreplace(s, old, new, occurrence):
    '''Convenience function from:
    https://stackoverflow.com/questions/2556108/\
    rreplace-how-to-replace-the-last-occurrence-of-an-expression-in-a-string
    '''
    li = s.rsplit(old, occurrence)
    return new.join(li)


def sanitize_dict(input_dict, sensitive_fields):
    '''Remove sensitive content.  Useful for logging.
    '''
    retval = {}
    if not input_dict:
        return retval
    retval.update(input_dict)
    for field in sensitive_fields:
        if retval.get(field):
            retval[field] = "[redacted]"
    return retval


def get_execution_namespace():
    '''Return Kubernetes namespace of this container.
    '''
    ns_path = '/var/run/secrets/kubernetes.io/serviceaccount/namespace'
    if os.path.exists(ns_path):
        with open(ns_path) as f:
            return f.read().strip()
    return None


def make_logger(name=None, level=None):
    '''Create a logger with LSST-appropriate characteristics.
    '''
    if name is None:
        # Get name of caller's class.
        #  From https://stackoverflow.com/questions/17065086/
        frame = inspect.stack()[1][0]
        name = _get_classname_from_frame(frame)
    logger = logging.getLogger(name)
    logger.propagate = False
    if level is None:
        level = logging.getLogger().getEffectiveLevel()
    logger.setLevel(level)
    logger.handlers = [EliotHandler()]
    logger.info("Created logger object for class '{}'.".format(name))
    return logger


def _get_classname_from_frame(fr):
    args, _, _, value_dict = inspect.getargvalues(fr)
    # we check the first parameter for the frame function is
    # named 'self'
    if len(args) and args[0] == 'self':
        # in that case, 'self' will be referenced in value_dict
        instance = value_dict.get('self', None)
        if instance:
            # return its classname
            cl = getattr(instance, '__class__', None)
            if cl:
                return "{}.{}".format(cl.__module__, cl.__name__)
    # If it wasn't a class....
    return '<unknown>'


def str_bool(s):
    '''Make a sane guess for whether a value represents true or false.
    Intended for strings, mostly in the context of environment variables,
    but if you pass it something that's not a string that is falsy, like
    an empty list, it will cheerfully return False.
    '''
    if not s:
        return False
    if type(s) != str:
        # It's not a string and it's not falsy, soooo....
        return True
    s = s.lower()
    if s in ['false', '0', 'no', 'n']:
        return False
    return True


def str_true(v):
    '''The string representation of a true value will be 'TRUE'.  False will
    be the empty string.
    '''
    if v:
        return 'TRUE'
    else:
        return ''


def listify(item, delimiter=','):
    '''Used for taking character (usually comma)-separated string lists
    and returning an actual list, or the empty list.
    Useful for environment parsing.

    Sure, you could pass it integer zero and get [] back.  Don't.
    '''
    if not item:
        return []
    if type(item) is str:
        item = item.split(delimiter)
    if type(item) is not list:
        raise TypeError("'listify' must take None, str, or list!")
    return item


def floatify(item, default=0.0):
    '''Another environment-parser: the empty string should be treated as
    None, and return the default, rather than the empty string (which
    does not become an integer).  Default can be either a float or string
    that float() works on.  Note that numeric zero (or string '0') returns
    0.0, not the default.  This is intentional.
    '''
    if item is None:
        return default
    if item == '':
        return default
    return float(item)


def intify(item, default=0):
    '''floatify, but for ints.
    '''
    return int(floatify(item, default))


def list_duplicates(seq):
    '''List duplicate items from a sequence.
    '''
    # https://stackoverflow.com/questions/5419204
    tally = defaultdict(list)
    for i, item in enumerate(seq):
        tally[item].append(i)
    return ((key, locs) for key, locs in tally.items()
            if len(locs) > 1)


def list_digest(inp_list):
    '''Return a digest to uniquely identify a list.
    '''
    if type(inp_list) is not list:
        raise TypeError("list_digest only works on lists!")
    if not inp_list:
        raise ValueError("input must be a non-empty list!")
    # If we can rely on python >= 3.8, shlex.join is better
    return hashlib.sha256(' '.join(inp_list).encode('utf-8')).hexdigest()


def get_access_token(tokenfile=None):
    '''Determine the access token from the mounted secret or environment.
    '''
    tok = None
    hdir = os.environ.get('HOME', None)
    if hdir:
        if not tokenfile:
            # FIXME we should make this instance-dependent
            tokfile = hdir + "/.access_token"
        try:
            with open(tokfile, 'r') as f:
                tok = f.read().replace('\n', '')
        except Exception as exc:
            log = make_logger()
            log.warn("Could not read tokenfile '{}': {}".format(tokfile, exc))
    if not tok:
        tok = os.environ.get('ACCESS_TOKEN', None)
    return tok


def parse_access_token(endpoint=None, tokenfile=None, token=None, timeout=15):
    '''Rely on gafaelfawr to validate and parse the access token.
    '''
    if not token:
        token = get_access_token(tokenfile=tokenfile)
    if not token:
        raise RuntimeError("Cannot determine access token!")
    # Endpoint is constant in an ArgoCD-deployed cluster
    if not endpoint:
        endpoint = "http://gafaelfawr-service.gafaelfawr:8080/auth/analyze"
    resp = requests.post(endpoint, data={'token': token}, timeout=timeout)
    rj = resp.json()
    p_resp = rj["token"]
    if not p_resp["valid"]:
        raise RuntimeError("Access token invalid: '{}'!".format(str(resp)))
    # Force to lowercase username (should no longer be necessary)
    p_tok = p_resp["data"]
    uname = p_tok["uid"]
    p_tok["uid"] = uname.lower()
    return p_tok


def assemble_gids(groupinfo):
    '''Take the group data (in the format of a CILogon "isMemberOf" claim)
    and return the string to be used for provisioning the user.

    Simply ignore names without corresponding ids.
    '''
    gidlist = ["{}:{}".format(x['name'], x['id'])
               for x in groupinfo if 'id' in x]
    return ','.join(gidlist)


def get_fake_gid():
    '''Use if we have strict_ldap_groups off, to assign GIDs to names
    with no matching Unix GID.  Since these will not appear as filesystem
    groups, being consistent with them isn't important.  We just need
    to make their GIDs something likely to not match anything real.

    There is a chance of collision, but it doesn't really matter.

    We do need to keep the no-GID groups around, though, because we might
    be using them to make options form or quota decisions (if we know we
    don't, we should turn on strict_ldap_groups).
    '''
    grpbase = 3E7
    grprange = 1E7
    igrp = random.randint(grpbase, (grpbase + grprange))
    return igrp
