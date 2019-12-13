'''LSST Authenticator to use JWT token present in request headers.
'''
from jwtauthenticator.jwtauthenticator import JSONWebTokenAuthenticator
from tornado import gen
from .lsstjwtloginhandler import LSSTJWTLoginHandler
from .lsstlogouthandler import LSSTLogoutHandler
from ..config import LSSTConfig
from ..utils import make_logger
from .. import LSSTMiddleManager


class LSSTJWTAuthenticator(JSONWebTokenAuthenticator):
    auth_refresh_age = 900
    enable_auth_state = True
    header_name = "X-Portal-Authorization"
    header_is_authorization = True
    groups = []
    allowed_groups = []
    forbidden_groups = []

    def __init__(self, *args, **kwargs):
        '''Add LSST Manager structure to hold LSST-specific logic.
        '''
        self.log = make_logger()
        self.log.debug("Creating LSSTJWTAuthenticator")
        super().__init__(*args, **kwargs)
        self.lsst_mgr = LSSTMiddleManager(parent=self, config=LSSTConfig())
        auth_refresh = kwargs.pop('auth_refresh_age', None)
        if auth_refresh is not None:
            self.auth_refresh_age = auth_refresh

    def get_handlers(self, app):
        '''Install custom handlers.
        '''
        return [
            (r'/login', LSSTJWTLoginHandler),
            (r'/logout', LSSTLogoutHandler)
        ]

    @gen.coroutine
    def pre_spawn_start(self, user, spawner):
        update_env = {}
        auth_state = yield user.get_auth_state()
        token = auth_state.get("access_token")
        update_env["ACCESS_TOKEN"] = token
        claims = auth_state.get("claims")
        uid = claims.get("uidNumber")
        if uid:
            uid = str(uid)
        else:
            raise ValueError("Could not get UID from JWT!")
        self.lsst_mgr.uid = uid
        update_env['EXTERNAL_UID'] = uid
        email = claims.get("email")
        if email:
            update_env['GITHUB_EMAIL'] = email
        membership = claims.get("isMemberOf")
        self.log.debug("Membership: {}".format(membership))
        am = self.lsst_mgr.auth_mgr
        group_map = {}
        for grp in membership:
            name = grp['name']
            gid = grp.get('id')
            if not id and not self.lsst_mgr.config.strict_ldap_groups:
                gid = am.get_fake_gid()
            if gid:
                group_map[name] = gid
        update_env['EXTERNAL_GROUPS'] = am.get_group_string()
        self.lsst_mgr.env_mgr.update_env(update_env)
        yield self.lsst_mgr.pre_spawn_start(user, spawner)

    def logout_url(self, base_url):
        '''Returns the logout URL for JWT.  Assumes the LSST OAuth2
        JWT proxy.  Yes, it currently is 'sign_in'.  Blame BVan.
        '''
        return '/oauth2/sign_in'
