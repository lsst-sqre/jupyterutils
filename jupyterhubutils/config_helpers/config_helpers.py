"""
Functions to aid with runtime configuration settings.
"""

import os

from jupyter_client.localinterfaces import public_ips
from urllib.parse import urlparse
from ..authenticator import (LSSTGitHubOAuthenticator,
                             LSSTCILogonOAuthenticator, LSSTJWTAuthenticator)
from ..utils import get_execution_namespace


def get_authenticator_type():
    return (os.environ.get('AUTH_PROVIDER') or
            os.environ.get('OAUTH_PROVIDER') or
            "github")


def get_authenticator_class():
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
    return os.getenv('OAUTH_CALLBACK_URL')


def get_audience():
    callback_url = get_callback_url()
    if callback_url:
        netloc = urlparse(callback_url).netloc
        scheme = urlparse(callback_url).scheme
        audience = None
        if netloc and scheme:
            audience = scheme + "://" + netloc
    return audience or os.getenv('OAUTH_CLIENT_ID') or ''


def get_oauth_parameters():
    id = os.getenv('OAUTH_CLIENT_ID')
    secret = os.getenv('OAUTH_CLIENT_SECRET')
    if not id:
        raise ValueError("Environment variable 'OAUTH_CLIENT_ID' not set!")
    if not secret:
        raise ValueError("Environment variable 'OAUTH_CLIENT_SECRET' not set!")
    return id, secret


def configure_authenticator():
    authtype = get_authenticator_type()
    authclass = get_authenticator_class()
    callback_url = get_callback_url()
    authclass.oauth_callback_url = callback_url
    if authtype == 'jwt':
        authclass.signing_certificate = '/opt/jwt/signing-certificate.pem'
        authclass.username_claim_field = 'uid'
        authclass.expected_audience = get_audience()
        authclass.logout_url = custom_logout_url
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


def custom_logout_url(base_url):
    callback_url = get_callback_url()
    netloc = urlparse(callback_url).netloc
    def_lo_url = None
    if netloc:
        def_lo_url = netloc + "/oauth2/sign_in"
    logout_url = (os.getenv('LOGOUT_URL') or def_lo_url or
                  base_url + "/logout")
    return logout_url


def get_db_url():
    return os.getenv('SESSION_DB_URL')


def get_hub_route():
    return os.getenv('HUB_ROUTE') or "/"


def get_hub_parameters():
    hub_name = "hub"
    hub_svc_address = None
    hub_route = get_hub_route()
    ns = get_execution_namespace()
    helm_tag = os.getenv('HELM_TAG')
    if helm_tag:
        hub_name = "{}-hub".format(helm_tag)
    hub_env = hub_name.replace('-', '_').upper()
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
    proxy_host = os.getenv('PROXY_SERVICE_HOST') or '127.0.0.1'
    proxy_port = os.getenv('PROXY_SERVICE_PORT_API') or '8001'
    return "http://" + proxy_host + ":" + proxy_port
