from ..utils import make_logger
from .authmanager import LSSTAuthManager
from .envmanager import LSSTEnvironmentManager
from .namespacemanager import LSSTNamespaceManager
from .optionsformmanager import LSSTOptionsFormManager
from .quotamanager import LSSTQuotaManager
from .volumemanager import LSSTVolumeManager


class LSSTMiddleManager(object):
    '''The LSSTMiddleManager is a class that holds references to various
    LSST-specific management objects and delegates requests to them.
    The idea is that an LSST Spawner, or an LSST Workflow Manager,
    could instantiate a single LSSTMiddleManager, which would then be
    empowered to perform all LSST-specific operations, reducing
    configuration complexity.
    '''
    parent = None
    config = None
    authenticator = None
    spawner = None
    user = None
    api = None
    rbac_api = None

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
        '''Delegate to namespace manager.
        '''
        self.namespace_mgr.ensure_namespace()

    def dump(self):
        '''Return contents dict to pretty-print.
        '''
        md = {"parent": str(self.parent),
              "authenticator": str(self.authenticator),
              "spawner": str(self.spawner),
              "user": str(self.user),
              "api": str(self.api),
              "rbac_api": str(self.rbac_api),
              "config": self.config.dump(),
              "auth_mgr": self.auth_mgr.dump(),
              "env_mgr": self.env_mgr.dump(),
              "optionsform_mgr": self.optionsform_mgr.dump(),
              "quota_mgr": self.quota_mgr.dump(),
              "volume_mgr": self.volume_mgr.dump()
              }
        return md
