'''This is a mixin class for authenticators for common LSST functionality.
'''
import asyncio
import json
from eliot import start_action
from jupyterhub.auth import Authenticator
from .. import LSSTMiddleManager
from ..config import LSSTConfig


class LSSTAuthenticator(Authenticator):
    '''We create an LSST Manager structure on startup; this is an
    LSSTMiddleManager controlling a set of other managers: auth, env,
    namespace, quota, and volume.

    All LSSTAuthenticator subclasses are expected to create two new fields in
     auth_state:
      * auth_state['group_map'], which contains a dict mapping group name
        (the key) to a group ID number (the value).  GIDs may be strings or
        integers.
      * auth_state['uid'], which contains a string or an integer with the
        user's numeric UID.
    '''

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.lsst_mgr = LSSTMiddleManager(parent=self,
                                          authenticator=self,
                                          config=LSSTConfig())
        self.enable_auth_state = True
        self.delete_invalid_users = True
        self.token = None

    def resolve_cilogon(self, membership):
        '''Shared between CILogon and JWT (which uses CILogon as its backing
        store) and thus appearing here.  Returns a uid, groupmap pair
        suitable for insertion into auth_state; both uid and group
        values are strings.
        '''
        with start_action(action_type="resolve_cilogon"):
            am = self.lsst_mgr.auth_mgr
            cfg = self.lsst_mgr.config
            groupmap = {}
            for grp in membership['isMemberOf']:
                name = grp['name']
                gid = grp.get('id')
                if not id and not cfg.strict_ldap_groups:
                    gid = am.get_fake_gid()
                if gid:
                    groupmap[name] = str(gid)
            uid = str(membership['uidNumber'])
            return uid, groupmap

    def dump(self):
        '''Return dict suitable for pretty-printing.
        '''
        ad = {"enable_auth_state": self.enable_auth_state,
              "delete_invalid_users": self.delete_invalid_users,
              "login_handler": str(self.login_handler),
              "lsst_mgr": self.lsst_mgr.dump()
              }
        return ad

    def toJSON(self):
        return json.dumps(self.dump())

    async def refresh_user(self, user, handler=None):
        '''On each refresh_user, clear the options form cache, thus
        forcing it to be rebuilt on next display.  Otherwise it is built once
        per user session, which is not frequent enough to display new images
        in a timely fashion.
        '''
        with start_action(action_type="refresh_user"):
            self.log.debug(
                "User name in refresh_user is '{}'.".format(user.escaped_name))
            self.lsst_mgr.optionsform_mgr.options_form_data = None
            retval = await super().refresh_user(user, handler)
            return retval
