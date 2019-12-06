'''LSST Login Handler to use JWT token present in request headers.
'''
import datetime
from tornado import gen, web
from jupyterhub.utils import url_path_join
from jwtauthenticator.jwtauthenticator import JSONWebTokenLoginHandler


class LSSTJWTLoginHandler(JSONWebTokenLoginHandler):

    @gen.coroutine
    def get(self):
        '''Authenticate on get() via reading the token from HTTP headers.
        '''
        # This is taken from https://github.com/mogthesprog/jwtauthenticator
        #  but with our additional claim information checked and stuffed
        #  into auth_state, and allow/deny lists checked.
        # We only work with an LSSTJWTAuthenticator, which has an lsst_mgr
        lm = self.authenticator.lsst_mgr
        am = lm.auth_mgr
        claims, token = self._check_auth_header()
        username_claim_field = self.authenticator.username_claim_field
        username = self.retrieve_username(claims, username_claim_field)
        user = self.user_from_username(username)
        # Here is where we deviate from the vanilla JWT authenticator.
        # We simply store all the JWT claims in auth_state, although we also
        #  choose our field names to make the spawner reusable from the
        #  OAuthenticator implementation.
        auth_state = yield self.refresh_user(user)
        _ = yield user.save_auth_state(auth_state)
        # Push the refreshed user through the managers
        lm.propagate_user(user)
        if not am._check_groups_jwt(claims):
            # We're either in a forbidden group, or not in any allowed group
            self.log.error("User did not validate from claims groups.")
            raise web.HTTPError(403)
        self.set_login_cookie(user)
        _url = url_path_join(self.hub.server.base_url, 'home')
        next_url = self.get_argument('next', default=False)
        if next_url:
            _url = next_url
        self.redirect(_url)

    @gen.coroutine
    def refresh_user(self, user, handler=None):
        '''Validate the token and force re-auth if the claims are not
        (presumably no longer) valid.
        '''
        self.log.debug("Refreshing user data.")
        try:
            claims, token = self._check_auth_header()
        except web.HTTPError:
            # Force re-login
            return False
        username_claim_field = self.authenticator.username_claim_field
        username = self.retrieve_username(claims, username_claim_field)
        auth_state = {"id": username,
                      "access_token": token,
                      "claims": claims}
        return auth_state

    def _check_auth_header(self):
        # Either returns (valid) claims and token,
        #  or throws a web error of some type.
        self.log.debug("Checking authentication header.")
        header_name = self.authenticator.header_name
        param_name = self.authenticator.param_name
        header_is_authorization = self.authenticator.header_is_authorization
        auth_header_content = self.request.headers.get(header_name, "")
        auth_cookie_content = self.get_cookie("XSRF-TOKEN", "")
        signing_certificate = self.authenticator.signing_certificate
        secret = self.authenticator.secret
        audience = self.authenticator.expected_audience
        tokenParam = self.get_argument(param_name, default=False)
        if auth_header_content and tokenParam:
            self.log.error("Authentication: both an authentication header " +
                           "and tokenParam")
            raise web.HTTPError(400)
        elif auth_header_content:
            if header_is_authorization:
                # We should not see "token" as first word in the
                #  AUTHORIZATION header.  If we do it could mean someone
                #  coming in with a stale API token
                if auth_header_content.split()[0].lower() != "bearer":
                    self.log.error("Authorization header is not 'bearer'.")
                    raise web.HTTPError(403)
                token = auth_header_content.split()[1]
            else:
                token = auth_header_content
        elif auth_cookie_content:
            token = auth_cookie_content
        elif tokenParam:
            token = tokenParam
        else:
            self.log.error("Could not determine authentication token.")
            raise web.HTTPError(401)

        claims = ""
        if secret:
            claims = self.verify_jwt_using_secret(token, secret, audience)
        elif signing_certificate:
            claims = self.verify_jwt_with_claims(token, signing_certificate,
                                                 audience)
        else:
            self.log.error("Could not verify JWT.")
            raise web.HTTPError(401)

        # Check expiration
        expiry = int(claims['exp'])
        now = int(datetime.datetime.utcnow().timestamp())
        if now > expiry:
            self.log.error("JWT has expired!")
            raise web.HTTPError(401)
        return claims, token
