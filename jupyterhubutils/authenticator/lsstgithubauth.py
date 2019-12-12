'''LSST-specific Github OAuthenticator class, delegating its LSST-specific
authentication to its auth_mgr.
'''
import json
import logging
import oauthenticator
import os
from oauthenticator.common import next_page_from_links
from tornado import gen
from tornado.httpclient import HTTPRequest, AsyncHTTPClient, HTTPError
from .. import LSSTMiddleManager
from ..config import LSSTConfig
from ..utils import make_logger, str_bool, sanitize_dict


def github_api_headers(access_token):
    '''Generate API headers for communicating with GitHub.
    '''
    return {"Accept": "application/json",
            "User-Agent": "JupyterHub",
            "Authorization": "token {}".format(access_token)
            }


class LSSTGitHubOAuthenticator(oauthenticator.GitHubOAuthenticator):
    enable_auth_state = True
    login_handler = oauthenticator.GitHubLoginHandler
    user_groups = []

    def __init__(self, *args, **kwargs):
        debug = str_bool(os.getenv('DEBUG'))
        if debug:
            logging.basicConfig(level=logging.DEBUG)
        self.log = make_logger()
        super().__init__(*args, **kwargs)
        self.lsst_mgr = LSSTMiddleManager(parent=self, config=LSSTConfig())

    def authenticate(self, handler, data=None):
        self.log.info("Authenticating user against GitHub.")
        userdict = yield super().authenticate(handler, data)
        try:
            token = userdict["auth_state"]["access_token"]
        except (KeyError, TypeError):
            self.log.warning("Could not extract access token.")
        if token:
            self.log.debug("Setting authenticator groups from token.")
            self._set_groups_from_github_token(token)
        else:
            self.log.debug("No token found.")
        denylist = os.environ.get('GITHUB_ORGANIZATION_DENYLIST')
        if denylist:
            if not token:
                self.log.warning("User does not have access token.")
                userdict = None
            else:
                self.log.debug("Denylist `%s` found." % denylist)
                denylist = denylist.split(',')
                denied = yield self._check_denylist(userdict, denylist)
            if denied:
                self.log.warning("Rejecting user: denylisted")
                userdict = None
        return userdict

    @gen.coroutine
    def get_uid(self):
        ast = yield self.user.get_auth_state()
        uid = ast["github_user"]["id"]
        return uid

    @gen.coroutine
    def _set_groups_from_github_token(self, token):
        self.log.debug("Acquiring list of user organizations.")
        gh_org = yield self._get_github_user_organizations(token)
        if not gh_org:
            self.log.warning("Could not get list of user organizations.")
        self.user_groups = list(gh_org.keys())
        self.lsst_mgr.auth_mgr.group_map = gh_org
        self.log.debug("Set user organizations to '{}'.".format(gh_org))

    @gen.coroutine
    def _check_github_denylist(self, userdict, denylist):
        if ("auth_state" not in userdict or not userdict["auth_state"]):
            self.log.warning("User doesn't have auth_state: rejecting.")
            return True
        ast = userdict["auth_state"]
        if ("access_token" not in ast or not ast["access_token"]):
            self.log.warning("User doesn't have access token: rejecting.")
            return True
        tok = ast["access_token"]
        gh_org = yield self._get_github_user_organizations(tok)
        if not gh_org:
            self.log.warning("Could not get list of GH user orgs: rejecting.")
            return True
        deny = list(set(gh_org) & set(denylist))
        if deny:
            self.log.warning("User in denylist %s: rejecting." % str(deny))
            return True
        self.log.debug("User not in denylist %s" % str(denylist))
        return False

    @gen.coroutine
    def _get_github_user_organizations(self, access_token):
        # Requires 'read:org' token scope.
        http_client = AsyncHTTPClient()
        headers = github_api_headers(access_token)
        next_page = "https://%s/user/orgs" % (self.github_api)
        orgmap = {}
        while next_page:
            req = HTTPRequest(next_page, method="GET", headers=headers)
            try:
                resp = yield http_client.fetch(req)
            except HTTPError:
                return None
            resp_json = json.loads(resp.body.decode('utf8', 'replace'))
            next_page = next_page_from_links(resp)
            for entry in resp_json:
                # This could result in non-unique groups, if the first 32
                #  characters of the group names are the same.
                normalized_group = entry["login"][:32]
                orgmap[normalized_group] = entry["id"]
        return orgmap

    @gen.coroutine
    def _get_github_user_email(self, access_token):
        # Determine even private email, if the token has 'user:email'
        #  scope
        http_client = AsyncHTTPClient()
        headers = github_api_headers(access_token)
        next_page = "https://%s/user/emails" % (self.github_api)
        while next_page:
            req = HTTPRequest(next_page, method="GET", headers=headers)
            resp = yield http_client.fetch(req)
            resp_json = json.loads(resp.body.decode('utf8', 'replace'))
            next_page = next_page_from_links(resp)
            for entry in resp_json:
                if "email" in entry:
                    if "primary" in entry and entry["primary"]:
                        return entry["email"]
        return None

    @gen.coroutine
    def pre_spawn_start(self, user=None, spawner=None):
        update_env = {}
        # Github fields
        auth_state = yield user.get_auth_state()
        gh_user = auth_state.get("github_user")
        gh_token = auth_state.get("access_token")
        gh_id = gh_user.get("id")
        gh_org = yield self._get_github_user_organizations(gh_token)
        self.log.debug("GitHub organizations: {}".format(gh_org))
        gh_email = gh_user.get("email")
        if not gh_email:
            gh_email = yield self._get_github_user_email(gh_token)
        gh_login = gh_user.get("login")
        gh_name = gh_user.get("name") or gh_login
        update_env['EXTERNAL_UID'] = str(gh_id)
        orglstr = ','.join(["{}:{}".format(k, gh_org[k])
                            for k in list(gh_org.keys())])
        update_env['EXTERNAL_GROUPS'] = orglstr
        if gh_name:
            update_env['GITHUB_NAME'] = gh_name
        if gh_login:
            update_env['GITHUB_LOGIN'] = gh_login
        if gh_token:
            update_env['GITHUB_ACCESS_TOKEN'] = gh_token
        if gh_email:
            update_env['GITHUB_EMAIL'] = gh_email
        sanitized = sanitize_dict(
            auth_state, ['token_response', 'access_token'])
        self.log.debug("auth_state: %s", json.dumps(sanitized,
                                                    indent=4,
                                                    sort_keys=True))
        self.lsst_mgr.env_mgr.update_env(update_env)
