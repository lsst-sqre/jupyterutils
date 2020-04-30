'''LSST-specific CILogon OAuthenticator class, delegating its LSST-specific
authentication logic to its auth_mgr.
'''
import oauthenticator
#from eliot import start_action
from tornado import gen
from .lsstauth import LSSTAuthenticator
from ..lsstmgr import check_membership
from ..utils import make_logger


class LSSTCILogonOAuthenticator(LSSTAuthenticator,
                                oauthenticator.CILogonOAuthenticator):
    login_handler = oauthenticator.CILogonLoginHandler
    # This one really is a class variable.

    def __init__(self, *args, **kwargs):
        self.log = make_logger()
        self.log.debug("Creating LSSTCILogonOAuthenticator")
        super().__init__(*args, **kwargs)
        self._default_domain = None

    @gen.coroutine
    def authenticate(self, handler, data=None):
        '''Authenticate and set UID/groups on auth_mgr.
        '''
        # start_action in Authenticate gets cross-Context stuff
        # with start_action(action_type="authenticate"):
        self.log.info("Authenticating user against CILogon.")
        cfg = self.lsst_mgr.config
        userdict = yield super().authenticate(handler, data)
        if not userdict:
            return None
        ast = yield self.user.get_auth_state()
        uid, groupmap = self.resolve_cilogon(ast['cilogon_user'])
        groups = groupmap.keys()
        membership = check_membership(groups, cfg.allowed_groups,
                                      cfg.forbidden_groups, log=self.log)
        if not membership:
            self.log.warning("Group membership check failed.")
            return None
        ast['group_map'] = groupmap
        user_rec = ast["cilogon_user"]
        username = user_rec["uid"]
        if "eppn" in user_rec:
            username, domain = user_rec["eppn"].split("@")
        else:
            domain = ""
        if (domain and self._default_domain and
                domain != self._default_domain):
            username = username + "." + domain
            userdict["name"] = username
        return userdict
