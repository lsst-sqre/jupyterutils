'''Quota support for LSST LSP Jupyterlab and Dask pods.
'''
import json
import os
from kubernetes.client import V1ResourceQuotaSpec
from kubernetes.client.rest import ApiException
from kubernetes import client

from ..utils import make_logger


class LSSTQuotaManager(object):
    log = None
    auth_mgr = None
    namespace_mgr = None
    optionsform_mgr = None
    user = None
    api = None
    groups = None
    quota = {}
    _custom_resources = {}

    def __init__(self, *args, **kwargs):
        self.log = make_logger()
        self.log.debug("Creating LSSTQuotaManager")
        self.parent = kwargs.pop('parent')
        self.user = self.parent.user
        self.groups = self.parent.spawner.groups
        self.api = self.parent.api
        self.resourcemap = self._read_resource_map()

    def _read_resource_map(self, resource_file=None):
        rfile = "/opt/lsst/software/jupyterhub/resources/resourcemap.json"
        if not resource_file:
            resource_file = rfile
        if not os.path.exists(resource_file):
            nf_msg = ("Could not find resource definition file" +
                      " at '{}'".format(resource_file))
            if self._mock:
                self.log.debug(nf_msg + ", but _mock is set.")
            else:
                self.log.warning(nf_msg)
            return None
        with open(resource_file, "r") as rf:
            resmap = json.load(rf)
        return resmap

    def set_custom_user_resources(self):
        '''Create custom resource definitions for user.
        '''
        if not self.resourcemap:
            self.log.warning("No resources map found.")
            return
        resources = {
            "size_index": 0,
            "cpu_quota": 0,
            "mem_quota": 0
        }
        gnames = self.groups
        uname = self.user.name
        for resdef in self.resourcemap:
            apply = False
            if resdef.get("disabled"):
                continue
            candidate = resdef.get("resources")
            if not candidate:
                continue
            self.log.debug(
                "Considering candidate resource map {}".format(resdef))
            ruser = resdef.get("user")
            rgroup = resdef.get("group")
            if ruser and ruser == uname:
                self.log.debug("User resource map match.")
                apply = True
            if rgroup and rgroup in gnames:
                self.log.debug("Group resource map match.")
                apply = True
            if apply:
                for fld in ["size_index", "cpu_quota", "mem_quota"]:
                    vv = candidate.get(fld)
                    if vv and vv > resources[fld]:
                        resources[fld] = vv
                    self.log.info(
                        "Setting custom resources '{}'".format(resources))
                    self._custom_resources = resources

    def get_resource_quota_spec(self):
        '''We're going to return a resource quota spec that checks whether we
        have a custom resource map and uses that information.  If we do not
        then we use the quota from our parent's config object.

        Note that you could get a lot fancier, and check the user group
        memberships to determine what class a user belonged to, or some other
        more-sophisticated-than-one-size-fits-all quota mechanism.
        '''
        self.log.debug("Entering get_resource_quota_spec()")
        self.log.info("Calculating default resource quotas.")
        big_multiplier = 2 ** (len(self.parent.optionsform_mgr.sizelist) - 1)
        cfg = self.parent.config
        max_dask_workers = cfg.max_dask_workers
        tiny_cpu = cfg.tiny_cpu
        mem_per_cpu = cfg.mb_per_cpu
        total_cpu = (1 + max_dask_workers) * big_multiplier * tiny_cpu
        total_mem = str(int(total_cpu * mem_per_cpu + 0.5)) + "Mi"
        total_cpu = str(int(total_cpu + 0.5))
        self.log.debug("Default quota sizes: CPU %r, mem %r" % (
            total_cpu, total_mem))
        if self._custom_resources:
            self.log.debug("Have custom resources.")
            cpuq = self._custom_resources.get("cpu_quota")
            if cpuq:
                self.log.debug("Overriding CPU quota.")
                total_cpu = str(cpuq)
            memq = self._custom_resources.get("mem_quota")
            if memq:
                self.log.debug("Overriding memory quota.")
                total_mem = str(memq) + "Mi"
        self.log.info("Determined quota sizes: CPU %r, mem %r" % (
            total_cpu, total_mem))
        qs = V1ResourceQuotaSpec(
            hard={"limits.cpu": total_cpu,
                  "limits.memory": total_mem})
        self.quota = qs.hard
        return qs

    # Brought in from namespacedkubespawner
    def ensure_namespaced_resource_quota(self, quotaspec):
        '''Create K8s quota object if necessary.
        '''
        self.log.info("Entering ensure_namespaced_resource_quota()")
        namespace = self.parent.namespace_mgr.namespace
        if namespace == "default":
            self.log.error("Will not create quota for default namespace!")
            return
        quota = client.V1ResourceQuota(
            metadata=client.V1ObjectMeta(
                name="quota",
            ),
            spec=quotaspec
        )
        self.log.info("Creating quota: %r" % quota)
        try:
            self.api.create_namespaced_resource_quota(namespace, quota)
        except ApiException as e:
            if e.status != 409:
                self.log.exception("Create resourcequota '%s'" % quota +
                                   "in namespace '%s' " % namespace +
                                   "failed: %s", str(e))
                raise
            else:
                self.log.info("Resourcequota '%r' " % quota +
                              "already exists in '%s'." % namespace)

    def _destroy_namespaced_resource_quota(self):
        # You don't usually have to call this, since it will get
        #  cleaned up as part of namespace deletion.
        namespace = self.parent.namespace_mgr.namespace
        qname = "quota-" + namespace
        dopts = client.V1DeleteOptions()
        self.log.info("Deleting resourcequota '%s'" % qname)
        self.api.delete_namespaced_resource_quota(qname, namespace, dopts)
