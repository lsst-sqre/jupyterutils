'''Class to provide support for document-driven Volume assignment.
'''

import base64
import json
import os

from ..utils import get_dummy_user, make_logger

from kubernetes import client
from kubernetes.client.rest import ApiException


class LSSTVolumeManager(object):
    namespace_mgr = None
    volume_list = []
    _nfs_volumes = []

    def __init__(self, *args, **kwargs):
        self.debug = kwargs.pop('debug', os.getenv('DEBUG') or False)
        config_file = kwargs.pop('config_file', None)
        self.log = make_logger(name=__name__, debug=self.debug)
        self.log.debug("Creating LSSTVolumeManager")
        self._mock = kwargs.pop('_mock', False)
        self.defer_user = kwargs.pop('defer_user', False)
        parent = kwargs.pop('parent', None)
        self.parent = parent
        user = kwargs.pop('user', None)
        if not user:
            if parent:
                if hasattr(parent, 'user'):
                    user = parent.user
        if not user and self._mock:
            self.log.info("Mocking out user.")
            user = get_dummy_user()
        if not user and not self.defer_user:
            self.log.error("No user, and defer_user not set!")
        self.user = user
        # Get namespace manager from parent, if there is one
        namespace_mgr = None
        if self.parent and hasattr(self.parent, "namespace_mgr"):
            namespace_mgr = self.parent.namespace_mgr
        self.namespace_mgr = namespace_mgr
        self.api = kwargs.pop('api', client.CoreV1Api())
        self.make_volumes_from_config(config_file=config_file)

    def make_volumes_from_config(self, config_file=None):
        '''Create volume definition representation from document.
        Override this in a subclass if you like.
        '''
        vollist = []
        config = []
        if not config_file:
            config_file = (
                "/opt/lsst/software/jupyterhub/mounts/mountpoints.json")
        if not os.path.exists(config_file):
            return vollist
        with open(config_file, "r") as fp:
            config = json.load(fp)
        for mtpt in config:
            self.log.debug("mtpt: %r" % mtpt)
            mountpoint = mtpt["mountpoint"]  # Fatal error if it doesn't exist
            if mtpt.get("disabled"):
                self.log.debug("Skipping disabled mountpoint %s" % mountpoint)
                continue
            if mountpoint[0] != "/":
                mountpoint = "/" + mountpoint
            host = mtpt.get("fileserver-host") or os.getenv(
                "EXTERNAL_FILESERVER_IP") or os.getenv(
                "FILESERVER_SERVICE_HOST")
            export = mtpt.get("fileserver-export") or (
                "/exports" + mountpoint)
            mode = (mtpt.get("mode") or "ro").lower()
            options = mtpt.get("options")  # Doesn't work yet.
            k8s_vol = mtpt.get("kubernetes-volume")
            hostpath = mtpt.get("hostpath")
            vollist.append({
                "mountpoint": mountpoint,
                "hostpath": hostpath,
                "k8s_vol": k8s_vol,
                "host": host,
                "export": export,
                "mode": mode,
                "options": options
            })
        self.log.debug("Volume list: %r" % vollist)
        self.volume_list = vollist
        volumes, mtpts = self._make_k8s_object_representations()
        self.volumes = volumes
        self.volume_mounts = mtpts

    def _make_k8s_object_representations(self):
        volumes = []
        mtpts = []
        for vol in self.volume_list:
            k8svol, k8smt = self._make_k8s_vol_objs(vol)
            if k8svol:
                volumes.append(k8svol)
                mtpts.append(k8smt)
        return (volumes, mtpts)

    def _make_k8s_vol_objs(self, vol):
        k8svol = None
        k8smt = None
        if vol.get("hostpath"):
            k8svol = self._make_k8s_hostpath_vol(vol)
            k8smt = self._make_k8s_hostpath_mt(vol)
        elif vol.get("k8s_vol"):
            k8svol = self._make_k8s_pv_vol(vol)
            k8smt = self._make_k8s_pvc_mt(vol)
        else:
            k8svol = self._make_k8s_nfs_vol(vol)
            k8smt = self._make_k8s_nfs_mt(vol)
        return k8svol, k8smt

    def _make_k8s_hostpath_vol(self, vol):
        return client.V1Volume(
            name=self._get_volume_name_for_mountpoint(vol["mountpoint"]),
            host_path=client.V1HostPath(
                path=vol["mountpoint"]
            )
        )

    def _make_k8s_pv_vol(self, vol):
        pvcname = vol["k8s_vol"]
        kpv = client.V1PersistentVolumeClaimVolume(
            claim_name=pvcname
        )
        if vol["mode"] == "ro":
            kpv.read_only = True
        return client.V1Volume(
            name=self._get_namespaced_volume_name_for_mountpoint(
                vol["mountpoint"]),
            persistent_volume_claim=kpv
        )

    def _make_k8s_nfs_vol(self, vol):
        knf = client.V1NFSVolumeSource(
            path=vol["export"],
            server=vol["host"]
        )
        if vol["mode"] == "ro":
            knf.read_only = True
        return client.V1Volume(
            name=self._get_volume_name_for_mountpoint(vol["mountpoint"]),
            nfs=knf
        )

    def _make_k8s_hostpath_mt(self, vol):
        mt = client.V1VolumeMount(
            mount_path=vol["mountpoint"],
            name=self._get_volume_name_for_mountpoint(vol["mountpoint"]),
        )
        if vol["mode"] == "ro":
            mt.read_only = True
        return mt

    def _make_k8s_pvc_mt(self, vol):
        mt = client.V1VolumeMount(
            mount_path=vol["mountpoint"],
            name=self._get_namespaced_volume_name_for_mountpoint(
                vol["mountpoint"])
        )
        if vol["mode"] == "ro":
            mt.read_only = True
        return mt

    def _make_k8s_nfs_mt(self, vol):
        return self._make_k8s_hostpath_mt(vol)

    def _get_volume_name_for_mountpoint(self, mountpoint):
        return mountpoint[1:].replace('/', '-')

    def _get_namespaced_volume_name_for_mountpoint(self, mountpoint):
        mtname = self._get_volume_name_for_mountpoint(mountpoint)
        namespace = None
        if self.namespace_mgr:
            namespace = self.namespace_mgr.namespace
        if namespace:
            return "{}-{}".format(mtname, namespace)
        return mtname

    def _get_nfs_volume(self, name):
        # Get an NFS volume by name.
        pvlist = self.api.list_persistent_volume()
        if pvlist and pvlist.items and len(pvlist.items) > 0:
            for pv in pvlist.items:
                if (pv and pv.metadata and hasattr(pv.metadata, "name")):
                    if pv.metadata.name == name:
                        return pv
        return None

    def _get_nfs_volumes(self, suffix=""):
        # This may be LSST-specific.  We're building a list of all NFS-
        #  mounted PVs, so we can later create namespaced PVCs for each of
        #  them.
        #
        # Suffix allows us to only categorize the PVs of a particular form;
        #  see the comment on _replicate_nfs_pvs() for the rationale.
        self.log.info("Refreshing NFS volume list")
        pvlist = self.api.list_persistent_volume()
        vols = []
        if pvlist and pvlist.items and len(pvlist.items) > 0:
            for pv in pvlist.items:
                if (pv and pv.spec and hasattr(pv.spec, "nfs") and
                        pv.spec.nfs):
                    if suffix:
                        if not pv.metadata.name.endswith(suffix):
                            continue
                    vols.append(pv)
                    self.log.debug("Found NFS volume '%s'" % pv.metadata.name)
        return vols

    def _refresh_nfs_volumes(self, suffix=""):
        vols = self._get_nfs_volumes(suffix)
        self._nfs_volumes = vols

    def _replicate_nfs_pv_with_suffix(self, vol, suffix):
        # A Note on Namespaces
        #
        # PersistentVolumes binds are exclusive,
        #  and since PersistentVolumeClaims are namespaced objects,
        #  mounting claims with “Many” modes (ROX, RWX) is only
        #  possible within one namespace.
        #
        # (https://kubernetes.io/docs/concepts/storage/persistent-volumes)
        #
        # The way we do this at LSST is that an NFS PV is statically created
        #  with the name suffixed with the same namespace as the Hub
        #  (e.g. "projects-gkenublado")
        #
        # Then when a new user namespace is created, we duplicate the NFS
        #  PV to one with the new namespace appended
        #  (e.g. "projects-gkenublado-athornton")
        #
        # Then we can bind PVCs to the new (effectively, namespaced) PVs
        #  and everything works.
        #
        # If you can use NFSv4, you don't need local locks, and therefore
        #  you don't need non-default NFS options, and therefore you can
        #  just specify the volumes in the pod spec as type NFS, and none
        #  of this overly-fragile stuff is required.
        #
        # So use NFSv4, as an "nfs" type volume, rather than a
        #  PersistentVolumeClaim, if you can.
        if not suffix:
            self.log.warning("Cannot create namespaced PV without suffix.")
            return None
        pname = vol.metadata.name
        mtkey = "volume.beta.kubernetes.io/mount-options"
        mtopts = None
        if vol.metadata.annotations:
            mtopts = vol.metadata.annotations.get(mtkey)
        ns_name = pname + "-" + suffix
        anno = {}
        if mtopts:
            anno[mtkey] = mtopts
        pv = client.V1PersistentVolume(
            spec=vol.spec,
            metadata=client.V1ObjectMeta(
                annotations=anno,
                name=ns_name,
                labels={"name": ns_name}
            )
        )
        # It is new, therefore unclaimed.
        pv.spec.claim_ref = None
        self.log.info("Creating PV '{}'.".format(ns_name))
        try:
            self.api.create_persistent_volume(pv)
        except ApiException as e:
            if e.status != 409:
                self.log.exception("Create PV '%s' " % ns_name +
                                   "failed: %s" % str(e))
                raise
            else:
                self.log.info("PV '%s' already exists." % ns_name)
        return pv

    def replicate_nfs_pvs(self):
        '''Create shadow PVs for a namespaced environment.  Since PVs are
        not namespaced, and since a PV can have only one PVC binding it, you
        need to create shadow PVs if you are mounting NFS volumes via PVCs,
        which you need to do if you want non-default options.  For NFSv3,
        you're going to need local locking in the spawned pods, so that's why
        you might do it.  Use NFSv4 and don't do this, if you can.
        '''
        self.log.info("Replicating NFS PVs")
        if self.namespace_mgr:
            namespace = self.namespace_mgr.namespace
        if not namespace:
            self.log.error("No namespace found; cannot replicate PVs.")
            return
        # FIXME if we ever have multiple simultaneous labs per user
        mns = self.user.name
        if mns:
            suffix = "-" + mns
        self._refresh_nfs_volumes(suffix=suffix)
        for vol in self._nfs_volumes:
            _ = self._replicate_nfs_pv_with_suffix(vol=vol, suffix=suffix)

    def destroy_namespaced_pvs(self):
        '''Destroy shadow PVs corresponding to a specific namespace.
        '''
        namespace = None
        if self.namespace_mgr:
            namespace = self.namespace_mgr.namespace
        if not namespace or namespace == "default":
            self.log.error("Will not destroy PVs for " +
                           "namespace '{}'".format(namespace))
            return
        vols = self._get_nfs_volumes(suffix="-" + namespace)
        for v in vols:
            self.api.delete_persistent_volume(v.metadata.name)

    def _create_pvc_for_pv(self, vol):
        name = vol.metadata.name
        namespace = None
        if self.namespace_mgr:
            namespace = self.namespace_mgr.namespace
        if not namespace or namespace == "default":
            self.log.error("Will not create PVC for " +
                           "namespace '{}'".format(namespace))
            return
        pvcname = name
        pvd = client.V1PersistentVolumeClaim(
            spec=client.V1PersistentVolumeClaimSpec(
                volume_name=name,
                access_modes=vol.spec.access_modes,
                resources=client.V1ResourceRequirements(
                    requests=vol.spec.capacity
                ),
                selector=client.V1LabelSelector(
                    match_labels={"name": name}
                ),
                storage_class_name=vol.spec.storage_class_name
            )
        )
        md = client.V1ObjectMeta(name=pvcname,
                                 labels={"name": pvcname})
        pvd.metadata = md
        self.log.info("Creating PVC '%s' in namespace '%s'" % (pvcname,
                                                               namespace))
        try:
            self.api.create_namespaced_persistent_volume_claim(namespace,
                                                               pvd)
        except ApiException as e:
            if e.status != 409:
                self.log.exception("Create PVC '%s' " % pvcname +
                                   "in namespace '%s' " % namespace +
                                   "failed: %s" % str(e))
                raise
            else:
                self.log.info("PVC '%s' " % pvcname +
                              "in namespace '%s' " % namespace +
                              "already exists.")

    def create_pvcs_for_pvs(self):
        '''Create (namespaced) PVCs for shadow-volume (fake-namespaced) PVs.
        '''
        self.log.info("Creating PVCs for PVs.")
        namespace = None
        mns = None
        suffix = None
        if self.namespace_mgr:
            namespace = self.namespace_mgr.namespace
            mns = self.user.name
        if namespace:
            suffix = "-" + mns
        else:
            self.log.error("Will not create PVCs in namespace " +
                           "'{}'!".format(namespace))
            return
        vols = self._get_nfs_volumes(suffix=suffix)
        for vol in vols:
            self._create_pvc_for_pv(vol)

    def _get_volume_yaml(self, left_pad=0):
        pad = " " * left_pad
        rstr = ""
        vols = self.volumes
        if not vols:
            self.log.warning("No volumes defined.")
            return rstr
        rstr += pad + "volumes:\n"
        for vol in vols:
            rstr += pad + "  - name: " + vol.name + "\n"
            hp = vol.host_path
            pv = vol.persistent_volume_claim
            nf = vol.nfs
            if hp:
                rstr += pad + "    hostPath:\n"
                rstr += pad + "      path: " + hp.path + "\n"
            elif pv:
                rstr += pad + "    persistentVolumeClaim:\n"
                rstr += pad + "      claimName: " + pv.claim_name + "\n"
            else:
                rstr += pad + "    nfs:\n"
                rstr += pad + "      server: " + nf.server + "\n"
                rstr += pad + "      path: " + nf.path + "\n"
                rstr += pad + "      accessMode: "
                if nf.read_only:
                    rstr += "ReadOnlyMany"
                else:
                    rstr += "ReadWriteMany"
                rstr += "\n"
        return rstr

    def _get_volume_mount_yaml(self, left_pad=0):
        pad = " " * left_pad
        rstr = ""
        vms = self.volume_mounts
        if not vms:
            self.log.warning("No volume mounts defined.")
            return rstr
        rstr += pad + "volumeMounts:\n"
        for vm in vms:
            rstr += pad + "  - name: " + vm.name + "\n"
            rstr += pad + "    mountPath: " + vm.mount_path + "\n"
            if vm.read_only:
                rstr += pad + "    readOnly: true\n"
        return rstr

    def get_dask_volume_b64(self):
        '''Return the base-64 encoding of the K8s statements to create
        the pod's mountpoints.  Probably better handled as a ConfigMap.
        '''
        vmt_yaml = self._get_volume_mount_yaml(left_pad=6)
        vol_yaml = self._get_volume_yaml(left_pad=4)
        ystr = vmt_yaml + vol_yaml
        self.log.debug("Dask yaml:\n%s" % ystr)
        benc = base64.b64encode(ystr.encode('utf-8')).decode('utf-8')
        return benc
