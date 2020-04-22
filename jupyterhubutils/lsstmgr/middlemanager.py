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
from .workflowmanager import LSSTWorkflowManager


class LSSTMiddleManager(Loggable):
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
    wf_api = None
    api_mgr = None
    auth_mgr = None
    namespace_mgr = None
    optionsform_mgr = None
    quota_mgr = None
    volume_mgr = None
    workflow_mgr = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.log.info(
            "Parent of LSST Middle Manager is '{}'".format(self.parent))
        self.config = kwargs.pop('config')
        parent = kwargs.pop('parent', self.parent)
        self.parent = parent
        authenticator = kwargs.pop('authenticator', self.authenticator)
        self.authenticator = authenticator
        self.authenticator = self.parent
        self.api_mgr = LSSTAPIManager(parent=self)
        self.auth_mgr = LSSTAuthManager(parent=self)
        self.env_mgr = LSSTEnvironmentManager(parent=self)
        self.namespace_mgr = LSSTNamespaceManager(parent=self)
        self.optionsform_mgr = LSSTOptionsFormManager(parent=self)
        self.quota_mgr = LSSTQuotaManager(parent=self)
        self.volume_mgr = LSSTVolumeManager(parent=self)
        self.workflow_mgr = LSSTWorkflowManager(parent=self)
        self.api = self.api_mgr.api
        self.rbac_api = self.api_mgr.rbac_api
        self.wf_api = self.api_mgr.wf_api

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
              "spawner": str(self.spawner),
              "user": str(self.user),
              "api": str(self.api),
              "rbac_api": str(self.rbac_api),
              "wf_api": str(self.wf_api),
              "config": self.config.dump(),
              "api_mgr": self.api_mgr.dump(),
              "auth_mgr": self.auth_mgr.dump(),
              "env_mgr": self.env_mgr.dump(),
              "optionsform_mgr": self.optionsform_mgr.dump(),
              "quota_mgr": self.quota_mgr.dump(),
              "volume_mgr": self.volume_mgr.dump(),
              "workflow_mgr": self.workflow_mgr.dump()
              }
        return md

    def toJSON(self):
        return json.dumps(self.dump())
