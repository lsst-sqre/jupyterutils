'''LSST Authenticator to use JWT token present in request headers.
'''
from jwtauthenticator.jwtauthenticator import JSONWebTokenAuthenticator
from .lsstauth import LSSTAuthenticator
from .lsstjwtloginhandler import LSSTJWTLoginHandler
from ..utils import make_logger


class LSSTJWTAuthenticator(LSSTAuthenticator, JSONWebTokenAuthenticator):
    auth_refresh_age = 900
    header_name = "X-Portal-Authorization"
    header_is_authorization = True
    username_claim_field = 'uid'

    def __init__(self, *args, **kwargs):
        '''Add LSST Manager structure to hold LSST-specific logic.
        '''
        self.log = make_logger()
        self.log.debug("Creating LSSTJWTAuthenticator")
        super().__init__(*args, **kwargs)

    def get_handlers(self, app):
        '''Install custom handlers.
        '''
        return [
            (r'/login', LSSTJWTLoginHandler),
        ]

    def logout_url(self, base_url):
        '''Returns the logout URL for JWT.
        '''
        return self.lsst_mgr.config.jwt_logout_url
