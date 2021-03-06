'''
Shared utility functions.
'''

import hashlib
import inspect
import logging
import os
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
    if name is None:
        logger.info('jupyterhubutils make_logger() called for root logger.')
        logger.info('not eliotify-ing root logger.')
        return logger
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


def get_fake_gid(grpname):
    '''Use if we have strict_ldap_groups off, to assign GIDs to names
    with no matching Unix GID.  We would like them to be consistent, so
    we will use a hash of the group name, modulo some large-ish constant,
    added to another large-ish constant.

    There is a chance of collision, but it doesn't really matter.

    We do need to keep the no-GID groups around, though, because we might
    be using them to make options form or quota decisions (if we know we
    don't, we should turn on strict_ldap_groups).
    '''
    grpbase = 3E7
    grprange = 1E7
    grphash = hashlib.sha256(grpname.encode('utf-8')).hexdigest()
    grpint = int(grphash, 16)
    igrp = int(grpbase + (grpint % grprange))
    return igrp


def make_passwd_line(claims):
    '''Create an entry for /etc/passwd based on our claims.  Returns a
    newline-terminated string.
    '''
    uname = claims['uid']
    uid = claims['uidNumber']
    pwline = "{}:x:{}:{}::/home/{}:/bin/bash\n".format(
        uname, uid, uid, uname)
    return pwline


def assemble_gids(claims, strict_ldap=False):
    '''Take the claims data and return the string to be used for be used
    for provisioning the user and groups (in sudo mode).
    '''
    glist = _map_supplemental_gids(claims, strict_ldap=strict_ldap)
    gidlist = ["{}:{}".format(x[0], x[1]) for x in glist]
    return ','.join(gidlist)


def make_group_lines(claims, strict_ldap=False):
    '''Create a list of newline-terminated strings representing group
    entries suitable for appending to /etc/group.
    '''
    uname = claims['uid']
    uid = claims['uidNumber']
    # Add individual group; don't put user in it (implicit from group in
    #  passwd)
    glines = ["{}:x:{}:\n".format(uname, uid)]
    glist = _map_supplemental_gids(claims, strict_ldap=strict_ldap)
    glines.extend(["{}:x:{}:{}\n".format(x[0], x[1], uname) for x in glist])
    return glines


def get_supplemental_gids(claims, strict_ldap=False):
    '''Create a list of gids suitable to paste into the supplemental_gids
    the container can run with (in sudoless mode).'''
    glist = _map_supplemental_gids(claims, strict_ldap=strict_ldap)
    return [x[1] for x in glist]


def resolve_groups(claims, strict_ldap=False):
    '''Returns groupmap suitable for insertion into auth_state;
    group values are strings.
    '''
    glist = _map_supplemental_gids(claims, strict_ldap=strict_ldap)
    groupmap = {}
    for gt in glist:
        groupmap[gt[0]] = str(gt[1])
    return groupmap


def _map_supplemental_gids(claims, strict_ldap=False):
    '''Helper function to deal with group manipulation.  Returns a list of
    tuples (groupname, gid).

    If a name has no id, omit the entry if strict_ldap is True.  Otherwise
    generate a fake gid for it and use that.
    '''
    uname = claims['uid']
    groups = claims['isMemberOf']
    retval = []
    for grp in groups:
        gname = grp['name']
        if gname == uname:
            continue  # We already have private group as runAsGid
        gid = grp.get('id', None)
        if not gid:
            if not strict_ldap:
                gid = get_fake_gid(gname)
        if gid:
            retval.append((gname, gid))
    return retval


def add_user_to_groups(uname, grpstr, groups=['lsst_lcl', 'jovyan']):
    '''Take a user name (a string) and a base group file (as a string) and
    inject the user into the appropriate groups, given in the groups
    parameter (defaults to 'lsst_lcl' and 'jovyan').  Returns a string.'''
    glines = grpstr.split('\n')
    g_str = ''
    for grp in glines:
        s_line = grp.strip()
        if not s_line:
            continue
        grpname = s_line.split(':')[0]
        if grpname in groups:
            if s_line.endswith(':'):
                s_line = s_line + uname
            else:
                s_line = s_line + ',' + uname
        g_str = g_str + s_line + '\n'
    return g_str
