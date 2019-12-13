from .authenticator import (LSSTGitHubOAuthenticator,
                            LSSTCILogonOAuthenticator, LSSTJWTAuthenticator)
from .spawner import LSSTSpawner


def lsst_configure(config):
    '''Do all the LSST-specific configuration based on the authenticator
    type and environment variables.
    '''
    config.spawner_class = LSSTSpawner
    authclass = None
    authtype = config.authenticator_type
    if authtype == 'jwt':
        authclass = LSSTJWTAuthenticator
    elif authtype == 'cilogon':
        authclass = LSSTCILogonOAuthenticator
    else:
        authclass = LSSTGitHubOAuthenticator
    config.authenticator_class = authclass
    authclass.oauth_callback_url = config.oauth_callback_url
    if authtype == 'jwt':
        authclass.signing_certificate = '/opt/jwt/signing-certificate.pem'
        authclass.username_claim_field = 'uid'
        authclass.expected_audience = config.audience
    else:
        client_id = config.oauth_client_id
        secret = config.oauth_client_secret
        if not client_id:
            config.log.warning("OAuth client ID missing!")
        if not secret:
            config.log.warning("OAuth secret missing!")
        authclass.client_id = client_id
        authclass.client_secret = secret
    if authtype == 'cilogon':
        authclass.scope = ['openid', 'org.cilogon.userinfo']
        authclass.skin = config.cilogon_skin
    idp = config.cilogon_idp
    if idp:
        authclass.idp = config.cilogon_idp
