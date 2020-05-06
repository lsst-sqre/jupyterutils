'''
Shared utility functions.
'''

import hashlib
import inspect
import logging
import os

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
                return cl.__qualname__
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
    if type(inp_list) is not list:
        raise TypeError("list_digest only works on lists!")
    if not inp_list:
        raise ValueError("input must be a non-empty list!")
    # If we can rely on python >= 3.8, shlex.join is better
    return hashlib.sha256(' '.join(inp_list).encode('utf-8')).hexdigest()
