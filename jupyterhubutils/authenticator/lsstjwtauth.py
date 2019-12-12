'''LSST Authenticator to use JWT token present in request headers.
'''
import logging
import os
from jwtauthenticator.jwtauthenticator import JSONWebTokenAuthenticator
from tornado import gen
from .lsstjwtloginhandler import LSSTJWTLoginHandler
from .lsstlogouthandler import LSSTLogoutHandler
from ..config import LSSTConfig
from ..utils import make_logger, str_bool
from .. import LSSTMiddleManager


class LSSTJWTAuthenticator(JSONWebTokenAuthenticator):
    auth_refresh_age = 900
    enable_auth_state = True
    header_name = "X-Portal-Authorization"
    header_is_authorization = True
    groups = []

    def __init__(self, *args, **kwargs):
        '''Add LSST Manager structure to hold LSST-specific logic.
        '''
        debug = str_bool(os.getenv('DEBUG'))
        if debug:
            logging.basicConfig(level=logging.DEBUG)
        self.log = make_logger()
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
        update_env['EXTERNAL_UID'] = uid
        email = claims.get("email")
        if email:
            update_env['GITHUB_EMAIL'] = email
        membership = claims.get("isMemberOf")
        grplist = self.map_groups(membership, update_env)
        update_env['EXTERNAL_GROUPS'] = grplist
        self.lsst_mgr.env_mgr.update_env(update_env)

    def logout_url(self, base_url):
        '''Returns the logout URL for JWT.  Assumes the LSST OAuth2
        JWT proxy.  Yes, it currently is 'sign_in'.  Blame BVan.
        '''
        return '/oauth2/sign_in'

    @gen.coroutine
    def get_uid(self):
        ast = yield self.user.get_auth_state()
        uid = ast["claims"]["uidNumber"]
        return uid
