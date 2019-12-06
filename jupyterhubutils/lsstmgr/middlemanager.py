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
        self._check_auth_and_spawner()
        self.auth_mgr = LSSTAuthManager(parent=self,
                                        user=user,
                                        authenticator=authenticator,
                                        spawner=spawner,
                                        debug=debug,
                                        defer_user=defer_user,
                                        _mock=_mock)
        self.env_mgr = LSSTEnvironmentManager(parent=self,
                                              _mock=_mock,
                                              debug=debug)
        self.namespace_mgr = LSSTNamespaceManager(parent=self,
                                                  user=user,
                                                  defer_user=defer_user,
                                                  api=api,
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
        self.auth_mgr.spawner = self.spawner
        self.auth_mgr.authenticator = self.authenticator
        self.env_mgr.quota_mgr = self.quota_mgr
        self.env_mgr.volume_mgr = self.volume_mgr
        self.namespace_mgr.quota_mgr = self.quota_mgr
        self.namespace_mgr.volume_mgr = self.volume_mgr
        self.optionsform_mgr.quota_mgr = self.quota_mgr
        self.quota_mgr.auth_mgr = self.auth_mgr
        self.quota_mgr.namespace_mgr = self.namespace_mgr
        self.volume_mgr.namespace_mgr = self.namespace_mgr

    def _check_auth_and_spawner(self):
        authenticator = self.authenticator
        if not authenticator:
            self.log.debug("Checking if parent is authenticator.")
            if self.parent and isinstance(self.parent, Authenticator):
                self.log.debug("Parent is authenticator.")
                authenticator = self.parent
            elif hasattr(self.parent, 'authenticator'):
                self.log.debug("Parent authenticator found.")
                authenticator = self.parent.authenticator
            else:
                self.log.debug("Authenticator not present.")
        else:
            self.log.debug("Authenticator already present.")
        self.log.debug("Setting authenticator to '{}'".format(authenticator))
        self.authenticator = authenticator
        spawner = self.spawner
        if not spawner:
            self.log.debug("Checking if parent is spawner.")
            if self.parent and isinstance(self.parent, Spawner):
                self.log.debug("Parent is spawner.")
                spawner = self.parent
            elif hasattr(self.parent, 'spawner'):
                self.log.debug("Parent spawner found.")
                spawner = self.parent.spawner
            else:
                self.log.debug("Spawner not present.")
        self.log.debug("Setting spawner to '{}'".format(spawner))
        self.spawner = spawner

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
        self.log.info("Checking authenticator and spawner.")
        self._check_auth_and_spawner()
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
        # Quota
        self.quota_mgr.user = user
        self.quota_mgr.defer_user = False
        # Volumes
        self.volume_mgr.user = user
        self.volume_mgr.defer_user = False
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
