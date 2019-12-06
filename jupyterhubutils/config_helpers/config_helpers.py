'''
Functions to aid with runtime configuration settings.
'''

import os

from jupyter_client.localinterfaces import public_ips
from urllib.parse import urlparse
from ..authenticator import (LSSTGitHubOAuthenticator,
                             LSSTCILogonOAuthenticator, LSSTJWTAuthenticator)
from ..utils import get_execution_namespace


def get_authenticator_type():
    '''Return the authenticator discriminator string: 'github', 'cilogon, or
    'jwt'.
    '''
    return (os.environ.get('AUTH_PROVIDER') or
            os.environ.get('OAUTH_PROVIDER') or
            "github")


def get_authenticator_class():
    '''Determine the authenticator type, and then return the appropriate
    authenticator class.
    '''
    authclass = None
    authtype = get_authenticator_type()
    if authtype == "github":
        authclass = LSSTGitHubOAuthenticator
    elif authtype == "cilogon":
        authclass = LSSTCILogonOAuthenticator
    elif authtype == "jwt":
        authclass = LSSTJWTAuthenticator
    else:
        raise ValueError("Auth type '{}' none of 'github'".format(authtype) +
                         ", 'cilogon', or 'jwt'!")
    return authclass


def get_callback_url():
    '''Return the OAuth callback URL, set in the environment.'''
    return os.getenv('OAUTH_CALLBACK_URL')


def get_audience():
    '''Return the audience for the JWT.'''
    callback_url = get_callback_url()
    if callback_url:
        netloc = urlparse(callback_url).netloc
        scheme = urlparse(callback_url).scheme
        audience = None
        if netloc and scheme:
            audience = scheme + "://" + netloc
    return audience or os.getenv('OAUTH_CLIENT_ID') or ''


def get_oauth_parameters():
    '''Return client ID and client secret for OAuth.
    '''
    id = os.getenv('OAUTH_CLIENT_ID')
    secret = os.getenv('OAUTH_CLIENT_SECRET')
    if not id:
        raise ValueError("Environment variable 'OAUTH_CLIENT_ID' not set!")
    if not secret:
        raise ValueError("Environment variable 'OAUTH_CLIENT_SECRET' not set!")
    return id, secret


def configure_authenticator():
    '''Do all the LSST-specific configuration based on the authenticator
    type and environment variables.
    '''
    authtype = get_authenticator_type()
    authclass = get_authenticator_class()
    callback_url = get_callback_url()
    authclass.oauth_callback_url = callback_url
    if authtype == 'jwt':
        authclass.signing_certificate = '/opt/jwt/signing-certificate.pem'
        authclass.username_claim_field = 'uid'
        authclass.expected_audience = get_audience()
    else:
        client_id, secret = get_oauth_parameters()
        authclass.client_id = client_id
        authclass.client_secret = secret
    if authtype == 'cilogon':
        authclass.scope = ['openid', 'org.cilogon.userinfo']
        skin = os.getenv("CILOGON_SKIN") or "LSST"
        authclass.skin = skin
        idp = os.getenv("CILOGON_IDP_SELECTION")
        if idp:
            authclass.idp = idp


def get_db_url():
    '''Return session database connection URL, set in the environment.
    '''
    return os.getenv('SESSION_DB_URL')


def get_hub_route():
    '''Return the internal context root for the Hub, set in the environment.
    Defaults to '/'.
    '''
    return os.getenv('HUB_ROUTE') or "/"


def get_hub_parameters():
    '''Return the Hub service address, port, and route, determined from the
    environment and the Kubernetes namespace.
    '''
    hub_svc_address = None
    hub_route = get_hub_route()
    ns = get_execution_namespace()
    hub_name, hub_env = get_helmed_name_and_env("hub")
    if ns:
        hub_svc_address = "{}.{}.svc.cluster.local".format(hub_name, ns)
    else:
        hub_svc_address = (os.getenv(hub_env + '_SERVICE_HOST')
                           or public_ips()[0])
    hub_api_port = os.getenv(hub_env + '_SERVICE_PORT_API') or 8081
    return {"route": hub_route,
            "svc": hub_svc_address,
            "port": hub_api_port}


def get_proxy_url():
    '''Return the URL for the Hub proxy.
    '''
    _, proxy_env = get_helmed_name_and_env("proxy")
    proxy_host = os.getenv(proxy_env + '_SERVICE_HOST') or '127.0.0.1'
    proxy_port = os.getenv(proxy_env + '_SERVICE_PORT_API') or '8001'
    return "http://" + proxy_host + ":" + proxy_port


def get_helmed_name_and_env(name):
    '''If the Helm tag is set, prepend that and a dash to the given name.
    The corresponding environment variable set by Kubernetes will be that,
    in uppercase, with dashes replaced by underscores.
    '''
    helm_tag = os.getenv('HELM_TAG')
    if helm_tag:
        name = "{}-{}".format(helm_tag, name)
    env_name = name.replace('-', '_').upper()
    return name, env_name
