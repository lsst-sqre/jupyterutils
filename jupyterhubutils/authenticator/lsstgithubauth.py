'''LSST-specific Github OAuthenticator class, delegating its LSST-specific
authentication to its auth_mgr.
'''
import json
import oauthenticator
from oauthenticator.common import next_page_from_links
from tornado import gen
from tornado.httpclient import HTTPRequest, AsyncHTTPClient, HTTPError
from .lsstauth import LSSTAuthenticator
from ..lsstmgr import check_membership
from ..utils import make_logger


def github_api_headers(access_token):
    '''Generate API headers for communicating with GitHub.
    '''
    return {"Accept": "application/json",
            "User-Agent": "JupyterHub",
            "Authorization": "token {}".format(access_token)
            }


class LSSTGitHubOAuthenticator(LSSTAuthenticator,
                               oauthenticator.GitHubOAuthenticator):
    login_handler = oauthenticator.GitHubLoginHandler

    def __init__(self, *args, **kwargs):
        self.log = make_logger()
        self.log.debug("Creating LSSTGitHubOAuthenticator.")
        super().__init__(*args, **kwargs)

    @gen.coroutine
    def authenticate(self, handler, data=None):
        '''Authenticate and store data on auth_mgr.
        '''
        self.log.info("Authenticating user against GitHub.")
        cfg = self.lsst_mgr.config
        # This must be set for the superclass authentication
        self.github_organization_whitelist = cfg.github_allowlist
        userdict = yield super().authenticate(handler, data)
        if userdict is None:
            return None
        ast = userdict["auth_state"]
        token = ast["access_token"]
        self.log.debug("Setting authenticator groups from token.")
        groupmap = yield self._get_github_user_organizations(token)
        groups = list(groupmap.keys())
        ok = check_membership(groups, cfg.allowed_groups, cfg.forbidden_groups,
                              log=self.log)
        if not ok:
            self.log.warning("Group membership check failed.")
            return None
        _ = yield self._set_github_user_email(ast, token)
        ast['group_map'] = groupmap
        ast['uid'] = ast['github_user']['id']
        return userdict

    @gen.coroutine
    def _get_github_user_organizations(self, access_token):
        # Requires 'read:org' token scope.
        http_client = AsyncHTTPClient()
        headers = github_api_headers(access_token)
        gh_api = self.lsst_mgr.config.github_api
        next_page = "https://{}/user/orgs".format(gh_api)
        orgmap = {}
        while next_page:
            req = HTTPRequest(next_page, method="GET", headers=headers)
            try:
                resp = yield http_client.fetch(req)
            except HTTPError as exc:
                self.log.error("{} -> {}".format(next_page, exc))
                return None
            resp_json = json.loads(resp.body.decode('utf8', 'replace'))
            next_page = next_page_from_links(resp)
            for entry in resp_json:
                group = entry["login"]
                orgmap[group] = entry["id"]
        return orgmap

    @gen.coroutine
    def _set_github_user_email(self, auth_state, access_token):
        gh_user = auth_state["github_user"]
        gh_email = gh_user.get("email")
        if gh_email:
            # Nothing further to do.
            return
        if not gh_email:
            gh_email = yield self._get_github_user_email(access_token)
        if not gh_email:
            # Oh well.
            return
        gh_user["email"] = gh_email
        # And now it will be stored with auth_state

    @gen.coroutine
    def _get_github_user_email(self, access_token):
        # Determine even private email, if the token has 'user:email'
        #  scope
        self.log.debug("Attempting alternate means of determining user email.")
        http_client = AsyncHTTPClient()
        headers = github_api_headers(access_token)
        gh_api = self.lsst_mgr.config.github_api
        next_page = "https://{}/user/emails".format(gh_api)
        while next_page:
            req = HTTPRequest(next_page, method="GET", headers=headers)
            resp = yield http_client.fetch(req)
            resp_json = json.loads(resp.body.decode('utf8', 'replace'))
            next_page = next_page_from_links(resp)
            for entry in resp_json:
                if "email" in entry:
                    if "primary" in entry and entry["primary"]:
                        self.log.debug(
                            "Found email: {}".format(entry['email']))
                        return entry["email"]
        self.log.debug("Could not determine user email.")
        return None
