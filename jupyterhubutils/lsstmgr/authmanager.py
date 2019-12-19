'''This contains both the LSSTAuthManager class and a handful of
functions that are basically slightly-tailored versions of set
manipulation, designed for doing things with user group membership.
'''

import random
from tornado import gen
from ..utils import make_logger, list_duplicates, sanitize_dict


def check_membership(groups, allowed, forbidden, log=None):
    '''False if any of 'groups' are in 'forbidden'.  Otherwise true if
    either: 'allowed' is empty or at least one member of 'groups' is
    also in 'allowed'.  Pass a logger object in if you want it to emit
    debug messages.
    '''
    if forbidden:
        deny = list(set(forbidden) & set(groups))
        if deny:
            _maybe_log_debug(
                "User in forbidden group(s) '{}'".format(deny), log)
            return False
    if not allowed:
        _maybe_log_debug(
            "No list of allowed groups; allowing all groups.", log)
        return True
    intersection = list(set(allowed) & set(groups))
    if intersection:
        _maybe_log_debug(
            "User in allowed group(s) '{}'".format(intersection), log)
        return True
    return False


def _maybe_log_debug(message, log=None):
    if not log:
        return
    log.debug(message)


def group_merge(self, groups_1, groups_2):
    '''Merge two groups, renaming collisions.
    '''
    # First just merge them
    grps = []
    grps.extend(groups_1)
    grps.extend(groups_2)
    # Naive deduplication
    grps = list(set(grps))
    grpnames = [x.split(':', 1)[0] for x in grps]
    gnset = set(grpnames)
    # Check for need to do less-naive dedupe
    if len(gnset) != len(grpnames):
        # We have a collision
        grps = self._deduplicate_groups(grps, grpnames)
    return grps


def deduplicate_groups(grps):
    '''Rename groups with colliding names.
    '''
    grpsplits = [x.split(':', 1) for x in grps]
    grpnames = [x[0] for x in grpsplits]
    grpnums = [x[1] for x in grpsplits]
    flist = list_duplicates(grpnames)
    for name, positions in flist:
        i = 1
        for p in positions[1:]:
            # start with group_2 (leave the first one alone)
            i += 1
            grps[p] = grpnames[p] + "_" + str(i) + ":" + grpnums[p]
    return grps


class LSSTAuthManager(object):
    '''Class to hold LSST-specific authentication/authorization details
    and methods.

    Most of this was formerly held in the JupyterHub config as classes defined
    in '10-authenticator.py'.
    '''
    authenticator = None
    uid = None
    group_map = {}        # key is group name, value is group id
    auth_state = {}
    pod_env = {}

    def __init__(self, *args, **kwargs):
        self.log = make_logger()
        self.log.debug("Creating LSSTAuthManager.")
        self.parent = kwargs.pop('parent')
        self.authenticator = self.parent.authenticator

    def get_fake_gid(self):
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

    def get_group_string(self):
        '''Convenience function for retrieving the group name-to-uid mapping
        list as a string suitable for passing to the spawned pod.
        '''
        return ','.join(["{}:{}".format(x, self.group_map[x])
                         for x in self.group_map])

    def get_pod_env(self):
        '''Return the authenticator-specific fields.
        '''
        return self.pod_env

    def parse_auth_state(self):
        '''Take the auth_state attribute and extract:
            * UID
            * Group/gid mappings
            * Possibly-authenticator-specific fields for pod environment

        and then store them in pod_env for the pod to pick up at spawn
         time. Our groups will be set if we authenticated, but if we got to
         the spawner page via a user that was already authenticated in the
         session DB (that is, you killed your pod and went away, but didn't
         log out, and then came back later while your session was still valid
         but the Hub had restarted), the authenticate() method in the spawner
         won't be run (since your user is still valid) but the fields won't
         be set (because the LSST managers are not persisted).  Hence the
         group mapping re-check, because to get around exactly this problem,
         each authenticator stores the group string in auth_state.
        '''
        self.log.debug("Parsing authentication state.")
        pod_env = {}
        ast = self.auth_state
        if not ast:
            raise RuntimeError(
                "Authenticator did not set manager's auth state!")
        cfg = self.parent.config
        authtype = cfg.authenticator_type
        if authtype == "github":
            gh_user = ast["github_user"]
            token = ast.get("access_token")
            gh_email = gh_user.get("email")
            gh_login = gh_user.get("login")
            gh_name = gh_user.get("name") or gh_login
            if gh_name:
                pod_env['GITHUB_NAME'] = gh_name
            if gh_login:
                pod_env['GITHUB_LOGIN'] = gh_login
            if gh_email:
                pod_env['GITHUB_EMAIL'] = gh_email
            pod_env['GITHUB_ACCESS_TOKEN'] = token
        elif authtype == "cilogon":
            email = ast.cilogon_user.get("email")  # ?
            if email:
                pod_env['GITHUB_EMAIL'] = email
        elif authtype == "jwt":
            claims = ast["claims"]
            token = ast["access_token"]
            email = claims.get("email")
            pod_env['ACCESS_TOKEN'] = token
            pod_env['GITHUB_EMAIL'] = email
        else:
            self.log.error("Authentication type {} unknown!".format(authtype))
        # These are generic
        self.uid = ast["uid"]
        if not self.uid:
            raise RuntimeError("Cannot determine user UID for pod spawn!")
        self.group_map = ast["group_map"]
        if not self.group_map:
            raise RuntimeError("Cannot determine user groups for pod spawn!")
        groupstr = self.get_group_string()
        pod_env['EXTERNAL_UID'] = str(self.uid)
        pod_env['EXTERNAL_GROUPS'] = groupstr
        self.pod_env = pod_env

    def dump(self):
        '''Return dict of contents for pretty-printing.
        '''
        pd = {"parent": str(self.parent),
              "uid": self.uid,
              "group_map": self.group_map,
              "auth_state": sanitize_dict(self.auth_state, ['access_token']),
              "pod_env": sanitize_dict(self.pod_env,
                                       ['ACCESS_TOKEN',
                                        'GITHUB_ACCESS_TOKEN'])}

        return pd
