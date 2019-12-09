'''The LSSTMiddleManager is a class that holds references to various
LSST-specific management objects and delegates requests to them.  The
idea is that an LSST Spawner, or an LSST Workflow Manager, could
instantiate a single LSSTMiddleManager, which would then be empowered
to perform all LSST-specific operations, reducing configuration
complexity.
'''

import os

from jupyterhub.auth import Authenticator
from jupyterhub.spawner import Spawner
from kubernetes import client, config

from ..utils import get_dummy_user, make_logger, str_bool

from .authmanager import LSSTAuthManager
from .envmanager import LSSTEnvironmentManager
from .namespacemanager import LSSTNamespaceManager
from .optionsformmanager import LSSTOptionsFormManager
from .quotamanager import LSSTQuotaManager
from .volumemanager import LSSTVolumeManager


class LSSTMiddleManager(object):
    authenticator = None
    spawner = None
    parent = None

    def __init__(self, *args, **kwargs):
        debug = kwargs.pop('debug', str_bool(os.getenv('DEBUG')) or False)
        self.debug = debug
        self.log = make_logger(name=__name__, debug=self.debug)
        self.log.debug("Creating LSSTMiddleManager")
        self.parent = kwargs.pop('parent', None)
        self.log.info(
            "Parent of LSST Middle Manager is '{}'".format(self.parent))
        _mock = kwargs.pop('_mock', False)
        self._mock = _mock
        api = kwargs.pop('api', None)
        if not api:
            if not self._mock:
                config.load_incluster_config()
                api = client.CoreV1Api()
            else:
                self.log.debug("No API, but _mock is set.  Leaving 'None'.")
        self.api = api
        rbac_api = kwargs.pop('rbac_api', None)
        if not rbac_api:
            if not self._mock:
                config.load_incluster_config()
                rbac_api = client.RbacAuthorizationV1Api()
            else:
                self.log.debug("No RBAC_API, but _mock is set -> 'None'.")
        self.rbac_api = rbac_api
        defer_user = kwargs.pop('defer_user', False)
        self.defer_user = defer_user
        user = kwargs.pop('user', None)
        if user is None:
            if hasattr(self.parent, 'user'):
                user = self.parent.user
        if user is None:
            if self.defer_user:
                self.log.debug("No user; deferring as requested.")
            else:
                if self._mock:
                    user = get_dummy_user()
                else:
                    raise ValueError("No user, not deferred, '_mock' False!")
        self.user = user
        authenticator = kwargs.pop('authenticator', None)
        self.authenticator = authenticator
        spawner = kwargs.pop('spawner', None)
        self.spawner = spawner
        self._update_authenticator_and_spawner()
        self.auth_mgr = LSSTAuthManager(parent=self,
                                        user=user,
                                        authenticator=authenticator,
                                        spawner=spawner,
                                        debug=debug,
                                        defer_user=defer_user,
                                        _mock=_mock)
        self.env_mgr = LSSTEnvironmentManager(parent=self,
                                              _mock=_mock,
                                              defer_user=defer_user,
                                              debug=debug)
        self.namespace_mgr = LSSTNamespaceManager(parent=self,
                                                  user=user,
                                                  defer_user=defer_user,
                                                  api=api,
                                                  rbac_api=api,
                                                  _mock=_mock,
                                                  debug=debug)
        self.optionsform_mgr = LSSTOptionsFormManager(parent=self,
                                                      user=user,
                                                      defer_user=defer_user,
                                                      _mock=_mock,
                                                      debug=debug)
        self.quota_mgr = LSSTQuotaManager(parent=self,
                                          user=user,
                                          defer_user=defer_user,
                                          _mock=_mock,
                                          api=api,
                                          debug=debug)
        self.volume_mgr = LSSTVolumeManager(parent=self,
                                            user=user,
                                            _mock=_mock,
                                            defer_user=defer_user,
                                            api=api,
                                            debug=self.debug)
        self._link_managers()

    def _link_managers(self):
        self.log.debug("Relinking managers.")
        self.auth_mgr.spawner = self.spawner
        self.auth_mgr.authenticator = self.authenticator
        self.auth_mgr.env_mgr = self.env_mgr
        self.env_mgr.quota_mgr = self.quota_mgr
        self.env_mgr.volume_mgr = self.volume_mgr
        self.namespace_mgr.quota_mgr = self.quota_mgr
        self.namespace_mgr.volume_mgr = self.volume_mgr
        self.optionsform_mgr.quota_mgr = self.quota_mgr
        self.quota_mgr.auth_mgr = self.auth_mgr
        self.quota_mgr.namespace_mgr = self.namespace_mgr
        self.volume_mgr.namespace_mgr = self.namespace_mgr

    def _update_authenticator_and_spawner(self):
        authenticator = None
        spawner = None
        self.log.debug(
            "Checking various fields for spawner and authenticator.")
        # First try our 'user' attribute
        user = self.user
        if user:
            self.log.debug("Found 'user'; using its versions if set.")
            if hasattr(user, 'authenticator') and user.authenticator:
                self.log.debug("Using user.authenticator.")
                authenticator = user.authenticator
            else:
                self.log.debug("'user' did not have authenticator set.")
            if hasattr(user, 'spawner') and user.spawner:
                self.log.debug("Using user.spawner.")
                spawner = user.spawner
            else:
                self.log.debug("'user' did not have spawner set.")
        # No?  See if our parent is either of them.
        if not authenticator:
            if self.parent and isinstance(self.parent, Authenticator):
                self.log.debug("Parent is Authenticator.")
                authenticator = self.parent
        if not spawner:
            if self.parent and isinstance(self.parent, Spawner):
                self.log.debug("Parent is Spawner.")
                spawner = self.parent
        # OK, what about spawner->authenticator or vice versa?
        if not authenticator:
            if hasattr(spawner, 'authenticator') and spawner.authenticator:
                self.log.debug("Spawner has 'authenticator'.")
                authenticator = spawner.authenticator
        if not spawner:
            if hasattr(authenticator, 'spawner') and authenticator.spawner:
                self.log.debug("Authenticator has 'spawner'.")
                spawner = authenticator.spawner
        # Did we have one coming in?
        if not authenticator:
            if self.authenticator:
                self.log.debug("Keeping current authenticator.")
                authenticator = self.authenticator
        if not spawner:
            if self.spawner:
                self.log.debug("Keeping current spawner.")
                spawner = self.spawner
        # Still no?
        if not authenticator:
            # Are we mocking it out?
            if self._mock:
                self.log.debug("_mock is set; leaving authenticator empty.")
            # Are we deferring the user (at initial object creation)?
            elif self.defer_user:
                self.log.debug("defer_user set: leaving authenticator empty.")
            else:
                self.log.warn("No authenticator found!")
        if not spawner:
            # Are we mocking it out?
            if self._mock:
                self.log.debug("_mock is set; leaving spawner empty.")
            # Are we deferring the user (at initial object creation)?
            elif self.defer_user:
                self.log.debug("defer_user set: leaving spawner empty.")
            else:
                self.log.warn("No spawner found!")
        # That's all we know how to try.
        self.authenticator = authenticator
        self.spawner = spawner
        # Update API clients from spawner, if we have them.
        if spawner:
            self.log.debug("Attempting to set API and RBAC_API from spawner.")
            if hasattr(spawner, "api") and spawner.api:
                self.log.debug("Setting API to spawner's API.")
                self.api = spawner.api
            if hasattr(spawner, "rbac_api") and spawner.rbac_api:
                self.log.debug("Setting RBAC_API to spawner's RBAC_API.")
                self.rbac_api = spawner.rbac_api

    def propagate_user(self, user):
        '''Given a user, propagate it to all the subsidary managers, relink
        them, and run a few update methods to set their attributes.
        '''
        if not user:
            self.log.error("Cannot propagate empty user!")
            return
        self.log.info(
            "Propagating new user record '{}' to managers.".format(user))
        self.user = user
        self.log.info("Updating authenticator and spawner.")
        self._update_authenticator_and_spawner()
        self.log.info("Propagating user through subsidary managers.")
        # Auth
        self.auth_mgr.user = user
        self.auth_mgr.defer_user = False
        self.auth_mgr.authenticator = self.authenticator
        self.auth_mgr.spawner = self.spawner
        # Nothing for Env
        # Namespace
        nm = self.namespace_mgr
        nm.user = user
        nm.defer_user = False
        if self.spawner:
            sp = self.spawner
            nm.delete_namespace_on_stop = sp.delete_namespace_on_stop
            nm.delete_namespaced_pvs_on_stop = sp.delete_namespaced_pvs_on_stop
            nm.duplicate_nfs_pvs_to_namespace = \
                sp.duplicate_nfs_pvs_to_namespace
            nm.enable_namespace_quotas = sp.enable_namespace_quotas
        nm.api = self.api
        nm.rbac_api = self.rbac_api
        # Quota
        self.quota_mgr.user = user
        self.quota_mgr.defer_user = False
        self.quota_mgr.api = self.api
        # Volumes
        self.volume_mgr.user = user
        self.volume_mgr.defer_user = False
        self.volume_mgr.api = self.api
        # Relink
        self._link_managers()
        # Run update methods on managers
        self.env_mgr.create_pod_env()
        nm.update_namespace()
        self.quota_mgr.set_custom_user_resources()

    def ensure_resources(self):
        '''Delegate to namespace manager (it in turn delegates to volume
        manager for PV manipulation).
        '''
        self.namespace_mgr.ensure_namespace()
