'''Class to hold LSST-specific authentication/authorization details
and methods.

Most of this was formerly held in the JupyterHub config as classes defined
in '10-authenticator.py'.
'''
import json
import os
import random

from jupyterhub.utils import maybe_future
from kubernetes import client
from oauthenticator.common import next_page_from_links
from tornado import gen
from tornado.httpclient import HTTPRequest, AsyncHTTPClient, HTTPError
from ..utils import (make_logger, github_api_headers,
                     str_bool, list_duplicates, get_dummy_user)


class LSSTAuthManager(object):
    spawner = None
    authenticator = None

    def __init__(self, *args, **kwargs):
        self.debug = kwargs.pop('debug', str_bool(os.getenv('DEBUG')) or False)
        self.log = make_logger(name=__name__, debug=self.debug)
        self.log.debug("Creating LSSTAuthManager.")
        self._mock = kwargs.pop('_mock', False)
        self.defer_user = kwargs.pop('defer_user', False)
        self.parent = kwargs.pop('parent', None)
        self.api = kwargs.pop('api', client.CoreV1Api())
        self.authenticator = kwargs.pop('authenticator', None)
        if not self.authenticator and self.parent and hasattr(self.parent,
                                                              'authenticator'):
            self.authenticator = self.parent.authenticator
        self.spawner = kwargs.pop('spawner', None)
        if not self.spawner and self.parent and hasattr(self.parent,
                                                        'spawner'):
            self.spawner = self.parent.spawner
        self.user = kwargs.pop('user', None)
        if self.user is None:
            if self.parent and hasattr(self.parent, 'user'):
                self.user = self.parent.user
        if not self.user and not self.defer_user:
            if self._mock:
                self.log.info("Mocking out user.")
                self.user = get_dummy_user()
        self.auth_provider = kwargs.pop('auth_provider',
                                        (os.environ.get('AUTH_PROVIDER') or
                                         os.environ.get('OAUTH_PROVIDER') or
                                         "github"))
        self.groups = kwargs.pop('groups', None)
        if not self.groups:
            if self.authenticator and hasattr(self.authenticator, 'groups'):
                self.groups = self.authenticator.groups
        self.github_host = kwargs.pop('github_host',
                                      (os.environ.get('GITHUB_HOST') or
                                       'github.com'))
        if self.github_host == 'github.com':
            self.github_api = 'api.github.com'
        else:
            self.github_api = "{}/api/v3".format(self.github_host)
        self.cilogon_host = kwargs.pop(
            'cilogon_host', (os.environ.get('CILOGON_HOST') or 'cilogon.org'))
        self.strict_ldap_groups = kwargs.pop(
            'strict_ldap_groups',
            os.environ.get('STRICT_LDAP_GROUPS') or False)

    @gen.coroutine
    def _get_uid_from_authenticator(self):
        def_uid = 69105
        w_msg = "using default external_uid '{}'".format(def_uid)
        if not self.authenticator:
            if not self._mock:
                raise ValueError("No authenticator/not mocking; no UID!")
            else:
                self.log.warning("Mocking out auth: {}".format(w_msg))
                return def_uid
        if not self.user:
            if self.defer_user:
                self.log.info("Deferring user; {}".format(w_msg))
                return def_uid
            if self._mock:
                self.log.warning("Mocking out auth: {}".format(w_msg))
                return def_uid
            raise ValueError("No user, not deferred, not mocked!")
        prov = self.auth_provider
        uid = None
        user = yield maybe_future(self.user)
        # Get the correct field; throw an error if busted
        ast = user.get_auth_state()
        if not ast:
            raise ValueError(
                "Auth state for user '{}' is empty; {}".format(user, w_msg))
        else:
            try:
                if prov == "jwt":
                    uid = ast["claims"]["uidNumber"]
                elif prov == "cilogon":
                    uid = ast["cilogon_user"]["uidNumber"]
                else:
                    uid = ast["github_user"]["id"]
            except (KeyError, TypeError) as exc:
                tr = json.dumps(ast, sort_keys=True, indent=4)
                self.log.error("Getting UID failed: {}/{}".format(prov, exc))
                self.log.error(
                    "Auth state: {}".format(tr))
                raise
        self.log.debug("Got uid '{}' from authenticator.".format(uid))
        return uid

    def _github_authenticate(self, userdict):
        self.log.info("Authenticating user against GitHub.")
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
    def _set_groups_from_github_token(self, token):
        self.log.debug("Acquiring list of user organizations.")
        gh_org = yield self._get_github_user_organizations(token)
        if not gh_org:
            self.log.warning("Could not get list of user organizations.")
        self.groups = gh_org
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

    def _group_merge(self, gh_groups, ci_groups):
        # First just merge them
        grps = []
        grps.extend(gh_groups)
        grps.extend(ci_groups)
        # Naive deduplication
        grps = list(set(grps))
        grpnames = [x.split(':', 1)[0] for x in grps]
        gnset = set(grpnames)
        # Check for need to do less-naive dedupe
        if len(gnset) != len(grpnames):
            # We have a collision
            grps = self._deduplicate_groups(grps, grpnames)
        return grps

    def _deduplicate_groups(self, grps):
        grpsplits = [x.split(':', 1) for x in grps]
        grpnames = [x[0] for x in grpsplits]
        grpnums = [x[1] for x in grpsplits]
        flist = list_duplicates(grpnames)
        for name, positions in flist:
            i = 1
            for p in positions[1:]:
                # start with group_2 (leave the first one alone)
                i += 1
                grps[p] = grpnames[p] + "_" + str(i) + ":" + grpnums[p]
        return grps

    def _cilogon_authenticate(self, userdict=None):
        self.log.info("Authenticating user against CILogon.")
        if userdict:
            membership = self._check_cilogon_group_membership(userdict)
            if not membership:
                userdict = None
        if userdict and "cilogon_user" in userdict["auth_state"]:
            user_rec = userdict["auth_state"]["cilogon_user"]
            if "eppn" in user_rec:
                username, domain = user_rec["eppn"].split("@")
            if "uid" in user_rec:
                username = user_rec["uid"]
                domain = ""
            if domain and domain != self._default_domain:
                username = username + "." + domain
            userdict["name"] = username
        return userdict

    def _check_cilogon_group_membership(self, userdict):
        if ("auth_state" not in userdict or not userdict["auth_state"]):
            self.log.warn("User doesn't have auth_state")
            return False
        ast = userdict["auth_state"]
        cu = ast["cilogon_user"]
        if "isMemberOf" in cu:
            has_member = yield self._check_member_of(cu["isMemberOf"])
            if not has_member:
                return False
        if ("token_response" not in ast or not ast["token_response"] or
            "id_token" not in ast["token_response"] or not
                ast["token_response"]["id_token"]):
            self.log.warn("User doesn't have ID token!")
            return False
        self.log.debug("Auth State: %s" % json.dumps(ast, sort_keys=True,
                                                     indent=4))
        return True

    def _set_groups(self, grouplist):
        grps = [x["name"] for x in grouplist]
        self.log.debug("Groups: %s" % str(grps))
        self.groups = grps

    def _check_member_of(self, grouplist):
        allowed_groups = self.authenticator.allowed_groups.split(",")
        forbidden_groups = self.authenticator.forbidden_groups.split(",")
        self._set_groups(grouplist)
        user_groups = self.groups
        deny = list(set(forbidden_groups) & set(user_groups))
        if deny:
            self.log.warning("User in forbidden group: %s" % str(deny))
            return False
        self.log.debug("User not in forbidden groups: %s" %
                       str(forbidden_groups))
        intersection = list(set(allowed_groups) &
                            set(user_groups))
        if intersection:
            self.log.debug("User in groups: %s" % str(intersection))
            return True
        self.log.warning("User not in any groups %s" % str(allowed_groups))
        return False

    def _check_groups_jwt(self, claims):
        # Here is where we deviate from the vanilla JWT authenticator.
        # We simply store all the JWT claims in auth_state, although we also
        #  choose our field names to make the spawner reusable from the
        #  OAuthenticator implementation.
        # We will already have pulled the claims and token from the
        #  auth header.
        if not self._jwt_validate_user_from_claims_groups(claims):
            # We're either in a forbidden group, or not in any allowed group
            self.log.error("User did not validate from claims groups.")
            return False
        self.log.debug("Claims for user: {}".format(claims))
        self.log.debug("Membership: {}".format(claims["isMemberOf"]))
        gnames = [x["name"] for x in claims["isMemberOf"]]
        self.log.debug("Setting authenticator groups: {}.".format(gnames))
        self.authenticator.groups = gnames
        self.groups = gnames
        return True

    def _jwt_validate_user_from_claims_groups(self, claims):
        alist = os.getenv('CILOGON_GROUP_WHITELIST').split(',')
        dlist = os.getenv('CILOGON_GROUP_DENYLIST').split(',')
        membership = [x["name"] for x in claims["isMemberOf"]]
        intersection = list(set(dlist) & set(membership))
        if intersection:
            # User is in at least one forbidden group.
            return False
        intersection = list(set(alist) & set(membership))
        if not intersection:
            # User is not in at least one allowed group.
            return False
        return True

    @gen.coroutine
    def pre_spawn_start(self, user=None, spawner=None):
        '''LSST logic to set up Lab pod spawning parameters.
        '''
        if not user:
            user = self.user
        if not spawner:
            spawner = self.spawner
        authenticator = self.authenticator
        if (not authenticator or
            not hasattr(authenticator, 'enable_auth_state') or
                not authenticator.enable_auth_state):
            return
        auth_state = yield user.get_auth_state()
        if not auth_state:
            self.log.warning("Auth state is enabled, but empty.")
            return
        update_env = {}
        # Github fields
        gh_user = auth_state.get("github_user")
        gh_token = auth_state.get("access_token")
        if gh_user:
            gh_id = gh_user.get("id")
            gh_org = yield self._get_github_user_organizations(gh_token)
            self.log.debug("GitHub organizations: {}".format(gh_org))
            gh_email = gh_user.get("email")
            if not gh_email:
                gh_email = yield self._get_github_user_email(gh_token)
            if gh_email:
                update_env['GITHUB_EMAIL'] = gh_email
            gh_login = gh_user.get("login")
            gh_name = gh_user.get("name") or gh_login
            if gh_id:
                update_env['EXTERNAL_UID'] = str(gh_id)
            if gh_org:
                orglstr = ""
                for k in gh_org:
                    if orglstr:
                        orglstr += ","
                        orglstr += k + ":" + str(gh_org[k])
                update_env['EXTERNAL_GROUPS'] = orglstr
            if gh_name:
                update_env['GITHUB_NAME'] = gh_name
            if gh_login:
                update_env['GITHUB_LOGIN'] = gh_login
            if gh_token:
                update_env['GITHUB_ACCESS_TOKEN'] = "[secret]"
                self.log.info("Updated environment: %s", json.dumps(
                    update_env, sort_keys=True, indent=4))
                update_env['GITHUB_ACCESS_TOKEN'] = gh_token
        if "cilogon_user" in auth_state:
            user_rec = auth_state["cilogon_user"]
            # Get UID and GIDs from OAuth reply
            uid = user_rec.get("uidNumber")
            if uid:
                uid = str(uid)
            else:
                # Fake it
                sub = user_rec.get("sub")
                if sub:
                    uid = sub.split("/")[-1]  # Pretend last field is UID
            update_env['EXTERNAL_UID'] = uid
            membership = user_rec.get("isMemberOf")
            if membership:
                grplist = self.map_groups(membership, update_env)
            if not update_env.get('EXTERNAL_GROUPS'):
                update_env['EXTERNAL_GROUPS'] = grplist
            else:
                update_env['EXTERNAL_GROUPS'] = self._group_merge(
                    update_env['EXTERNAL_GROUPS'], grplist)
        # JWT Fields
        token = auth_state.get("access_token")
        if token:
            update_env["ACCESS_TOKEN"] = token
            claims = auth_state.get("claims")
            if claims:
                # Get UID and GIDs from OAuth reply
                uid = claims.get("uidNumber")
                if uid:
                    uid = str(uid)
                else:
                    # Fake it
                    sub = claims.get("sub")
                    if sub:
                        uid = sub.split("/")[-1]  # Pretend last field is UID
                update_env['EXTERNAL_UID'] = uid
                email = claims.get("email")
                if email:
                    update_env['GITHUB_EMAIL'] = email
                membership = claims.get("isMemberOf")
                if membership:
                    grplist = self.map_groups(membership, update_env)
                    if not update_env.get('EXTERNAL_GROUPS'):
                        update_env['EXTERNAL_GROUPS'] = grplist
                    else:
                        update_env['EXTERNAL_GROUPS'] = self._group_merge(
                            update_env['EXTERNAL_GROUPS'], grplist)
        # Mask sensitive fields for logging
        save_rtoken = auth_state.get('token_response')
        if save_rtoken:
            auth_state['token_response'] = '[secret]'
        save_atoken = auth_state.get('access_token')
        if save_atoken:
            auth_state['access_token'] = '[secret]'
        self.log.info("auth_state: %s", json.dumps(auth_state,
                                                   indent=4,
                                                   sort_keys=True))
        if save_rtoken:
            auth_state["token_response"] = save_rtoken
        if save_atoken:
            auth_state["access_token"] = save_atoken
        # State restored
        # Whew!
        # Update spawner environment
        if (spawner and
            hasattr(spawner, "environment") and
                type(update_env) == dict):
            spawner.environment.update(update_env)
        # Do the update of the LSST manager stuff.
        lsst_mgr = self.parent
        if not lsst_mgr:
            self.log.error("No parent LSST Manager!")
            return
        lsst_mgr.propagate_user(user)
        lsst_mgr.ensure_resources()

    def map_groups(self, membership, update_env):
        '''Create a map from group names to gid numbers.
        '''
        # We use a fake number if there is no matching 'id'
        # Pick something outside of 16 bits, way under 32,
        #  and high enough that we are unlikely to have
        #  collisions.  Turn on strict_ldap_groups by
        #  setting the environment variable if you want to
        #  just skip those.
        gidlist = []
        grpbase = 3E7
        grprange = 1E7
        igrp = random.randint(grpbase, (grpbase + grprange))
        for group in membership:
            gname = group["name"]
            if "id" in group:
                gid = group["id"]
            else:
                # Skip if strict groups and no GID
                if self.strict_ldap_groups:
                    continue
                gid = igrp
                igrp = igrp + 1
                gidlist.append(gname + ":" + str(gid))
            grplist = ",".join(gidlist)
        return grplist

    def get_groups(self):
        '''Convenience function for group retrieval.
        '''
        return self.groups
