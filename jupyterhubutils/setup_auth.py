from .authenticator import (LSSTGitHubOAuthenticator,
                            LSSTCILogonOAuthenticator,
                            LSSTJWTAuthenticator)
from .spawner import LSSTSpawner


def configure_auth_and_spawner(config):
    '''Do all the LSST-specific configuration based on the authenticator
    type and environment variables.
    '''
    config.spawner_class = LSSTSpawner
    authclass = None
    authtype = config.authenticator_type
    if authtype == 'jwt':
        authclass = LSSTJWTAuthenticator
        # CILogon is the backing store for JWT at NCSA
        config.allowed_groups = config.cilogon_allowlist
        config.forbidden_groups = config.cilogon_denylist
    elif authtype == 'cilogon':
        authclass = LSSTCILogonOAuthenticator
        authclass.scope = ['openid', 'org.cilogon.userinfo']
        authclass.skin = config.cilogon_skin
        idp = config.cilogon_idp
        if idp:
            authclass.idp = config.cilogon_idp
        config.allowed_groups = config.cilogon_allowlist
        config.forbidden_groups = config.cilogon_denylist
    else:
        authclass = LSSTGitHubOAuthenticator
        config.allowed_groups = config.github_allowlist
        config.forbidden_groups = config.github_denylist
    config.authenticator_class = authclass
    authclass.oauth_callback_url = config.oauth_callback_url
    client_id = config.oauth_client_id
    secret = config.oauth_client_secret
    authclass.client_id = client_id
    authclass.client_secret = secret
    if not client_id:
        config.log.warning("OAuth client ID missing!")
    if not secret:
        config.log.warning("OAuth secret missing!")
