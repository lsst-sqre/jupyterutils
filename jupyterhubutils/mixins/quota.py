import json
import os
from kubernetes.client import V1ResourceQuotaSpec
from kubernetes.client.rest import ApiException
from kubernetes import client

from .logobject import LSSTLogObject


class LSSTQuota(LSSTLogObject):
    """Mixin class to provide quota support for LSST LSP pods.
    """

    enable_namespace_quotas = True
    log = None
    _quota = {}
    _custom_resources = {}

    def __init__(self, args, **kwargs):
        self.super().__init__(args, kwargs)

    def _get_user_groupnames(self):
        if not hasattr(self, 'authenticator'):
            self.log.error("No 'authenticator' attribute.")
            return []
        if not hasattr(self.authenticator, 'groups'):
            self.log.error("Authenticator has no 'groups' attribute.")
            return []
        return self.authenticator.groups

    def _set_custom_user_resources(self):
        if self._custom_resources:
            return
        rfile = "/opt/lsst/software/jupyterhub/resources/resourcemap.json"
        resources = {
            "size_index": 0,
            "cpu_quota": 0,
            "mem_quota": 0
        }
        try:
            gnames = self._get_user_groupnames()
            uname = self.user.name
            with open(rfile, "r") as rf:
                resmap = json.load(rf)
            for resdef in resmap:
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
                    if hasattr(self, 'log') and self.log:
                        self.log.info(
                            "Setting custom resources '{}'".format(resources))
                        self._custom_resources = resources
        except Exception as exc:
            self.log.error(
                "Custom resource check got exception '{}'".format(exc))

    def get_resource_quota_spec(self):
        '''We're going to return a resource quota spec that checks whether we
        have a custom resource map and uses that information.  If we do not
        then our default quota allows a maximum of MAX_DASK_WORKERS or
        25 (chosen arbitrarily) of the largest-size machines available to the
        user.

        Note that you could get a lot fancier, and check the user group
        memberships to determine what class a user belonged to, or some other
        more-sophisticated-than-one-size-fits-all quota mechanism.
        '''
        self.log.info("Entering get_resource_quota_spec()")
        self.log.info("Calculating default resource quotas.")
        sizes = self.sizelist
        max_dask_workers = os.environ.get('MAX_DASK_WORKERS')
        if max_dask_workers is None or '':
            max_dask_workers = '25'
        max_machines = int(max_dask_workers) + 1  # (the 1 is the Lab)
        big_multiplier = 2 ** (len(sizes) - 1)
        tiny_cpu = os.environ.get('TINY_MAX_CPU') or 0.5
        if type(tiny_cpu) is str:
            tiny_cpu = float(tiny_cpu)
        mem_per_cpu = os.environ.get('MB_PER_CPU') or 2048
        if type(mem_per_cpu) is str:
            mem_per_cpu = int(mem_per_cpu)
        total_cpu = max_machines * big_multiplier * tiny_cpu
        total_mem = str(int(total_cpu * mem_per_cpu + 0.5)) + "Mi"
        total_cpu = str(int(total_cpu + 0.5))
        self.log.debug("Default quota sizes: CPU %r, mem %r" % (
            total_cpu, total_mem))
        if self._custom_resources:
            if hasattr(self, 'log') and self.log:
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
        self.log.info("Resource quota spec: %r" % qs)
        self._quota = qs.hard
        return qs

    # Brought in from namespacedkubespawner
    def _ensure_namespaced_resource_quota(self, quotaspec):
        self.log.info("Entering ensure_namespaced_resource_quota()")
        namespace = self.get_user_namespace()
        qname = "quota-" + namespace
        quota = client.V1ResourceQuota(
            metadata=client.V1ObjectMeta(
                name=qname
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
        namespace = self.get_user_namespace()
        qname = "quota-" + namespace
        dopts = client.V1DeleteOptions()
        self.log.info("Deleting resourcequota '%s'" % qname)
        self.api.delete_namespaced_resource_quota(qname, namespace, dopts)
