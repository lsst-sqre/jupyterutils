from .. import LoggableChild
from .. import Singleton
from kubespawner.clients import shared_client
from kubernetes.config import load_incluster_config, load_kube_config
from kubernetes.config.config_exception import ConfigException
from argo.workflows.client import V1alpha1Api
import argo.workflows.config


class LSSTAPIManager(LoggableChild, metaclass=Singleton):
    wf_api = None
    rbac_api = None
    api = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        try:
            load_incluster_config()
            argo.workflows.config.load_incluster_config()
        except ConfigException:
            self.log.warning("In-cluster config failed! Falling back.")
            try:
                load_kube_config()
                argo.workflows.config.load_kube_config()
            except ValueError as exc:
                self.log.error("Still errored: {}".format(exc))
        wf_api = kwargs.pop('wf_api', self.wf_api)
        if not wf_api:
            wf_api = V1alpha1Api()
        self.wf_api = wf_api
        rbac_api = kwargs.pop('rbac_api', self.rbac_api)
        if not rbac_api:
            rbac_api = shared_client('RbacAuthorizationV1Api')
        self.rbac_api = rbac_api
        api = kwargs.pop('api', self.api)
        if not api:
            api = shared_client('CoreV1Api')
        self.api = api

    def dump(self):
        '''Return contents dict for aggregation and pretty-printing.
        '''
        ad = {"parent": str(self.parent),
              "api": str(self.api),
              "rbac_api": str(self.rbac_api),
              "wf_api": str(self.wf_api)
              }
        return ad
