import json
from eliot import start_action
from .. import Loggable
from .apimanager import LSSTAPIManager
from .authmanager import LSSTAuthManager
from .envmanager import LSSTEnvironmentManager
from .namespacemanager import LSSTNamespaceManager
from .optionsformmanager import LSSTOptionsFormManager
from .quotamanager import LSSTQuotaManager
from .volumemanager import LSSTVolumeManager


class LSSTMiddleManager(Loggable):
    '''The LSSTMiddleManager is a class that holds references to various
    LSST-specific management objects and delegates requests to them.
    The idea is that an LSST Spawner could instantiate a single
    LSSTMiddleManager, which would then be empowered to perform all
    LSST-specific operations, reducing configuration complexity.

    '''

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = kwargs.pop('config', None)
        self.parent = kwargs.pop('parent', None)
        self.log.info(
            "Parent of LSST Middle Manager is '{}'".format(self.parent))
        self.authenticator = kwargs.pop('authenticator', None)
        self.spawner = kwargs.pop('spawner', None)
        self.user = kwargs.pop('user', None)
        self.api_mgr = LSSTAPIManager(parent=self)
        self.auth_mgr = LSSTAuthManager(parent=self)
        self.env_mgr = LSSTEnvironmentManager(parent=self)
        self.namespace_mgr = LSSTNamespaceManager(parent=self)
        self.optionsform_mgr = LSSTOptionsFormManager(parent=self)
        self.quota_mgr = LSSTQuotaManager(parent=self)
        self.volume_mgr = LSSTVolumeManager(parent=self)
        self.api = self.api_mgr.api
        self.rbac_api = self.api_mgr.rbac_api

    def ensure_resources(self):
        '''Delegate to namespace manager.
        '''
        with start_action(action_type="ensure_resources"):
            self.namespace_mgr.ensure_namespace()

    def dump(self):
        '''Return contents dict to pretty-print.
        '''
        md = {"parent": str(self.parent),
              "authenticator": str(self.authenticator),
              "api": str(self.api),
              "rbac_api": str(self.rbac_api),
              "config": self.config.dump(),
              "api_mgr": self.api_mgr.dump(),
              "auth_mgr": self.auth_mgr.dump(),
              "env_mgr": self.env_mgr.dump(),
              "optionsform_mgr": self.optionsform_mgr.dump(),
              "quota_mgr": self.quota_mgr.dump(),
              "volume_mgr": self.volume_mgr.dump()
              }
        if self.user:
            md["user"] = "".format(self.user)
        if self.spawner:
            md["spawner"] = self.spawner.dump()
        return md

    def toJSON(self):
        return json.dumps(self.dump())
