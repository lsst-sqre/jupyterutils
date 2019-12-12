'''LSST-specific Github OAuthenticator class, delegating its LSST-specific
authentication logic to its auth_mgr.
'''
import json
import oauthenticator
from tornado import gen
from .. import LSSTMiddleManager
from ..config import LSSTConfig
from ..utils import make_logger, sanitize_dict


class LSSTCILogonOAuthenticator(oauthenticator.CILogonOAuthenticator):
    enable_auth_state = True
    login_handler = oauthenticator.CILogonLoginHandler
    groups = []
    _default_domain = None

    def __init__(self, *args, **kwargs):
        self.log = make_logger()
        self.log.debug("Creating LSSTCILogonOAuthenticator")
        super().__init__(*args, **kwargs)
        self.lsst_mgr = LSSTMiddleManager(parent=self, config=LSSTConfig())

    @gen.coroutine
    def authenticate(self, handler, data=None):
        self.log.info("Authenticating user against CILogon.")
        userdict = yield super().authenticate(handler, data)
        if userdict:
            ast = yield self.user.get_auth_state()
            self._set_group_records(ast)
            membership = self.lsst_mgr.auth_mgr.check_membership()
            if not membership:
                userdict = None
                self.groups = []
                self.lsst_mgr.auth_mgr.group_map = {}
        if userdict and "cilogon_user" in userdict["auth_state"]:
            user_rec = userdict["auth_state"]["cilogon_user"]
            username = user_rec["uid"]
            if "eppn" in user_rec:
                username, domain = user_rec["eppn"].split("@")
            else:
                domain = ""
            if (domain and self._default_domain and
                    domain != self._default_domain):
                username = username + "." + domain
            userdict["name"] = username
        uid = ast["cilogon_user"]["uidNumber"]
        self.lsst_mgr.uid = uid
        return userdict

    def _set_group_records(self, auth_state):
        membership = auth_state["cilogon_user"]["isMemberOf"]
        gnames = []
        groupmap = {}
        for rec in membership:
            name = rec["name"]
            gnames.append(name)
            gid = rec.get("id")
            if not gid and not self.lsst_mgr.config.strict_ldap_groups:
                gid = self.lsst_mgr.auth_mgr.get_fake_gid()
            if gid:
                groupmap[name] = gid
        self.groups = gnames
        self.lsst_mgr.auth_mgr.group_map = groupmap

    def _check_cilogon_group_membership(self, userdict):
        if ("auth_state" not in userdict or not userdict["auth_state"]):
            self.log.warn("User doesn't have auth_state")
            return False
        ast = userdict["auth_state"]
        cu = ast["cilogon_user"]
        if "isMemberOf" in cu:
            has_member = yield self._check_member_of(cu["isMemberOf"])
            if not has_member:
                return False
        if ("token_response" not in ast or not ast["token_response"] or
            "id_token" not in ast["token_response"] or not
                ast["token_response"]["id_token"]):
            self.log.warn("User doesn't have ID token!")
            return False
        return True

    @gen.coroutine
    def pre_spawn_start(self, user, spawner):
        update_env = {}
        auth_state = yield self.user.get_auth_state()
        user_rec = auth_state["cilogon_user"]
        # Get UID and GIDs from OAuth reply
        uid = user_rec.get("uidNumber")
        if not uid:
            raise ValueError("Could not get UID from user record!")
        uid = str(uid)
        update_env['EXTERNAL_UID'] = uid
        membership = user_rec.get("isMemberOf")
        grplist = self.lsst_mgr.auth_mgr.map_groups(membership, update_env)
        update_env['EXTERNAL_GROUPS'] = grplist
        sanitized = sanitize_dict(
            auth_state, ['token_response', 'access_token'])
        self.log.debug("auth_state: %s", json.dumps(sanitized,
                                                    indent=4,
                                                    sort_keys=True))
        self.lsst_mgr.env_mgr.update_env(update_env)
        yield self.lsst_mgr.pre_spawn_start(user, spawner)
