'''
Shared utility functions.
'''

import logging
import os

from collections import defaultdict
from .dummyobject import DummyObject


def get_execution_namespace():
    '''Return Kubernetes namespace of this container.
    '''
    ns_path = '/var/run/secrets/kubernetes.io/serviceaccount/namespace'
    if os.path.exists(ns_path):
        with open(ns_path) as f:
            return f.read().strip()
    return None


def get_dummy_user():
    '''Create a dummy user object.
    '''
    user = DummyObject()
    user.name = "anonymous"
    user.get_auth_state = user.dummyMethod
    user.escaped_name = user.dummyMethod
    return user


def make_logger(name=__name__, debug=False):
    '''Create a logger with a specific output format.
    '''
    logger = logging.getLogger(name)
    oldlevel = logger.getEffectiveLevel()
    fstr = '%(levelname)s | %(asctime)s | %(module)s:%(funcName)s:%(lineno)d'
    fstr += ' | %(message)s'
    ch = logging.StreamHandler()
    fmt = logging.Formatter(fstr)
    ch.setFormatter(fmt)
    # Remove default handlers, if any
    logger.handlers = []
    logger.addHandler(ch)
    if debug:
        logger.setLevel(logging.DEBUG)
        if oldlevel != logging.DEBUG:
            # Only comment on level when it changes.
            logger.debug("Debug logging enabled for '{}'.".format(name))
    return logger


def github_api_headers(access_token):
    '''Generate API headers for communicating with GitHub.
    '''
    return {"Accept": "application/json",
            "User-Agent": "JupyterHub",
            "Authorization": "token {}".format(access_token)
            }


def str_bool(s):
    '''Make a sane guess for whether a string represents a true or false
    value.
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


def list_duplicates(seq):
    '''List duplicate items from a sequence.
    '''
    # https://stackoverflow.com/questions/5419204
    tally = defaultdict(list)
    for i, item in enumerate(seq):
        tally[item].append(i)
    return ((key, locs) for key, locs in tally.items()
            if len(locs) > 1)
