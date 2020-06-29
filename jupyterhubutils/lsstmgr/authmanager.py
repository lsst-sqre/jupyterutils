'''This contains both the LSSTAuthManager class and a handful of
functions that are basically slightly-tailored versions of set
manipulation, designed for doing things with user group membership.
'''

import json
import random
from asgiref.sync import async_to_sync
from eliot import start_action
from .. import LoggableChild
from ..utils import sanitize_dict


class LSSTAuthManager(LoggableChild):
    '''Class to hold LSST-specific authentication/authorization details
    and methods.

    Most of this was formerly held in the JupyterHub config as classes defined
    in '10-authenticator.py'.
    '''

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.authenticator = self.parent.authenticator
        self.uid = None
        self.group_map = {}
        self.pod_env = {}
        # Do not set self.auth_state.  It gets incorrectly cached.

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
        with start_action(action_type="get_fake_gid"):
            grpbase = 3E7
            grprange = 1E7
            igrp = random.randint(grpbase, (grpbase + grprange))
            return igrp

    def get_group_string(self):
        '''Convenience function for retrieving the group name-to-uid mapping
        list as a string suitable for passing to the spawned pod.
        '''
        with start_action(action_type="get_group_string"):
            return ','.join(["{}:{}".format(x, self.group_map[x])
                             for x in self.group_map])

    def get_pod_env(self):
        '''Return the authenticator-specific fields.
        '''
        with start_action(action_type="get_pod_env"):
            return self.pod_env

    def parse_auth_state(self):
        '''Take the auth_state from parent.spawner and extract:
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
        with start_action(action_type="parse_auth_state"):
            self.log.debug("Parsing authentication state.")
            pod_env = {}
            ast = async_to_sync(
                self.parent.spawner.user.get_auth_state)()
            if not ast:
                raise RuntimeError(
                    "Could not determine current user auth state!")
            claims = ast["claims"]
            token = ast["access_token"]
            email = claims.get("email") or ''
            pod_env['ACCESS_TOKEN'] = token
            pod_env['GITHUB_EMAIL'] = email
            # These are generic
            self.uid = ast["uid"]
            if not self.uid:
                raise RuntimeError("Cannot determine user UID for pod spawn!")
            self.group_map = ast["group_map"]
            if not self.group_map:
                raise RuntimeError("Cannot determine user GIDs for pod spawn!")
            groupstr = self.get_group_string()
            pod_env['EXTERNAL_UID'] = str(self.uid)
            pod_env['EXTERNAL_GROUPS'] = groupstr
            self.pod_env = pod_env

    def dump(self):
        '''Return dict of contents for pretty-printing.
        '''
        ast = async_to_sync(self.parent.spawner.user.get_auth_state)()
        pd = {"parent": str(self.parent),
              "uid": self.uid,
              "group_map": self.group_map,
              "auth_state": sanitize_dict(ast, ['access_token']),
              "pod_env": sanitize_dict(self.pod_env,
                                       ['ACCESS_TOKEN',
                                        'GITHUB_ACCESS_TOKEN'])}

        return pd

    def toJSON(self):
        return json.dumps(self.dump())
