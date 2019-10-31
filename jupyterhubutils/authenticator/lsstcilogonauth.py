'''LSST-specific Github OAuthenticator class.
'''
import oauthenticator
import os
from .lsstoauth import LSSTOAuthenticator
from ..utils import make_logger, str_bool


class LSSTCILogonOAuthenticator(oauthenticator.CILogonOAuthenticator,
                                LSSTOAuthenticator):
    enable_auth_state = True
    login_handler = oauthenticator.CILogonLoginHandler

    def __init__(self, *args, **kwargs):
        self.debug = kwargs.get('debug', str_bool(os.getenv('DEBUG')) or False)
        self.log = make_logger(name=__name__, debug=self.debug)
        super().__init__(*args, **kwargs)
        self.authenticate_method = self.lsst_mgr.auth_mgr._cilogon_authenticate
