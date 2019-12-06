'''LSST Authenticator to use JWT token present in request headers.
'''
import os
from jwtauthenticator.jwtauthenticator import JSONWebTokenAuthenticator
from tornado import gen
from .lsstjwtloginhandler import LSSTJWTLoginHandler
from .lsstlogouthandler import LSSTLogoutHandler
from ..utils import make_logger, str_bool
from .. import LSSTMiddleManager


class LSSTJWTAuthenticator(JSONWebTokenAuthenticator):
    auth_refresh_age = 900
    enable_auth_state = True
    header_name = "X-Portal-Authorization"
    user = {}

    def __init__(self, *args, **kwargs):
        '''Add LSST Manager structure to hold LSST-specific logic.
        '''
        _mock = kwargs.get('_mock'), False
        super().__init__(*args, **kwargs)
        self.debug = kwargs.pop('debug', str_bool(os.getenv('DEBUG')) or False)
        self.log = make_logger(name=__name__, debug=self.debug)
        user = None
        if hasattr(self, 'user'):
            user = self.user
        self.lsst_mgr = LSSTMiddleManager(
            parent=self,
            user=user,
            authenticator=self,
            defer_user=True,
            debug=self.debug,
            _mock=_mock
        )
        lm = self.lsst_mgr
        am = lm.auth_mgr
        self.user = am.user
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
        '''Delegate to auth manager method.
        '''
        am = self.lsst_mgr.auth_mgr
        rv = yield am.pre_spawn_start(user, spawner)
        return rv

    def logout_url(self, base_url):
        '''Returns the logout URL for JWT.  Assumes the LSST OAuth2
        JWT proxy.
        '''
        return '/oauth2/sign_in'
