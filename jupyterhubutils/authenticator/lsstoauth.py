'''LSST-specific OAuthenticator abstract class.  This needs to be subclassed
by a specific OAuthenticator implementation.  Most of the LSST-specific logic
is encapsulated in the LSSTMiddleManager (and its subordinate manager)
attributes.
'''
import asyncio
import oauthenticator
import os
from tornado import gen

from .lsstlogouthandler import LSSTLogoutHandler

from ..utils import make_logger, str_bool
from .. import LSSTMiddleManager


class LSSTOAuthenticator(oauthenticator.OAuthenticator):
    enable_auth_state = True
    lsst_mgr = None
    # These must be overridden in a subclass.
    authenticate_method = None
    logout_handler = LSSTLogoutHandler

    def __init__(self, *args, **kwargs):
        _mock = kwargs.get('_mock', False)
        super().__init__(*args, **kwargs)
        self.debug = kwargs.pop('debug', str_bool(os.getenv('DEBUG')) or False)
        self.log = make_logger(name=__name__, debug=self.debug)
        # Should be empty at __init__()
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
        self.login_handler.refresh_auth = self.refresh_auth

    @gen.coroutine
    def authenticate(self, handler, data=None):
        '''Authenticate via superclass, then propagate authenticated user
        through LSST managers.'''
        userdict = yield super().authenticate(handler, data)
        self.log.debug(
            "Superclass authentication yielded: '{}'".format(userdict))
        # Call our specific implementation's auth method
        checked_user = self.authenticate_method(userdict)
        self.log.debug("User authenticated; propagating new user data")
        # Push updated user through
        self.lsst_mgr.propagate_user(self.user)
        return checked_user

    @gen.coroutine
    def pre_spawn_start(self, user, spawner):
        '''Delegate to authentication/authorization manager.
        '''
        am = self.lsst_mgr.auth_mgr
        rv = yield am.pre_spawn_start(user, spawner)
        return rv

    async def refresh_auth(self):
        '''Call superclass refresh_auth, and then propagate self.user
        through LSST Managers.'''
        await super().refresh_auth()
        self.lsst_mgr.propagate_user(self.user)
        return self.user
