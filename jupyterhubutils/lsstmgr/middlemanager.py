'''The LSSTMiddleManager is a class that holds references to various
LSST-specific management objects and delegates requests to them.  The
idea is that an LSST Spawner, or an LSST Workflow Manager, could
instantiate a single LSSTMiddleManager, which would then be empowered
to perform all LSST-specific operations, reducing configuration
complexity.
'''

from tornado import gen
from ..utils import make_logger

from .authmanager import LSSTAuthManager
from .envmanager import LSSTEnvironmentManager
from .namespacemanager import LSSTNamespaceManager
from .optionsformmanager import LSSTOptionsFormManager
from .quotamanager import LSSTQuotaManager
from .volumemanager import LSSTVolumeManager


class LSSTMiddleManager(object):
    parent = None
    authenticator = None
    spawner = None
    user = None
    username = None
    uid = None
    api = None
    rbacapi = None

    def __init__(self, *args, **kwargs):
        self.log = make_logger()
        self.log.debug("Creating LSSTMiddleManager")
        self.parent = kwargs.pop('parent')
        self.log.info(
            "Parent of LSST Middle Manager is '{}'".format(self.parent))
        self.config = kwargs.pop('config')
        self.authenticator = self.parent
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

    @gen.coroutine
    def pre_spawn_start(self, user, spawner):
        '''Update manager attributes now that we have user and spawner.
        '''
        # Run methods that depend on the managers having all been
        #  initialized and then having been given user/spawner info
        self.log.debug("Updating subordinate managers.")
        self.volume_mgr.make_volumes_from_config()
        self.env_mgr.refresh_pod_env()
        self.namespace_mgr.update_namespace_name()
        self.spawner.namespace = self.namespace_mgr.namespace
        if self.config.allow_dask_spawn:
            self.namespace_mgr.service_account = "dask"

    @gen.coroutine
    def get_uid(self):
        return self.uid
