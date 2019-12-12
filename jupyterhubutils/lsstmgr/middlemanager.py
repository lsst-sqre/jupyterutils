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
        self.log = make_logger()
        self.log.debug("Creating LSSTMiddleManager")
        self.parent = kwargs.pop('parent')
        self.log.info(
            "Parent of LSST Middle Manager is '{}'".format(self.parent))
        self.config = kwargs.pop('config')
        self.user = self.parent.user
        self.authenticator = self.parent
        self.spawner = self.parent.spawner
        if self.spawner:
            self.log.debug("Attempting to set API and RBAC_API from spawner.")
            self.api = self.spawner.api
            self.rbac_api = self.spawner.rbac_api
        self.auth_mgr = LSSTAuthManager(parent=self)
        self.env_mgr = LSSTEnvironmentManager(parent=self)
        self.namespace_mgr = LSSTNamespaceManager(parent=self)
        self.optionsform_mgr = LSSTOptionsFormManager(parent=self)
        self.quota_mgr = LSSTQuotaManager(parent=self)
        self.volume_mgr = LSSTVolumeManager(parent=self)

    def ensure_resources(self):
        '''Delegate to namespace manager (it in turn delegates to volume
        manager for PV manipulation).
        '''
        self.namespace_mgr.ensure_namespace()
