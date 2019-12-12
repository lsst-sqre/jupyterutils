'''Class to provide support for document-driven Volume assignment.
'''

import base64
import json
import os
import yaml

from ..utils import make_logger

from kubernetes import client


class LSSTVolumeManager(object):
    volume_list = []
    k8s_volumes = []
    k8s_vol_mts = []

    def __init__(self, *args, **kwargs):
        self.log = make_logger()
        self.log.debug("Creating LSSTVolumeManager")
        self.parent = kwargs.pop('parent')

    def make_volumes_from_config(self):
        '''Create volume definition representation from document.
        Override this in a subclass if you like.
        '''
        vollist = []
        config = []
        config_file = self.parent.config.volume_definition_file
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
            host = (mtpt.get("fileserver-host") or
                    self.parent.config.fileserver_host)
            export = mtpt.get("fileserver-export") or (
                "/exports" + mountpoint)
            mode = (mtpt.get("mode") or "ro").lower()
            k8s_vol = mtpt.get("kubernetes-volume")
            if k8s_vol:
                raise ValueError("Shadow PVs and matching PVCs " +
                                 "are no longer supported!")
            hostpath = mtpt.get("hostpath")
            vollist.append({
                "mountpoint": mountpoint,
                "hostpath": hostpath,
                "host": host,
                "export": export,
                "mode": mode,
            })
        self.log.debug("Volume list: %r" % vollist)
        self.volume_list = vollist
        volumes, mtpts = self._make_k8s_object_representations()
        self.k8s_volumes = volumes
        self.k8s_vol_mts = mtpts

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

    def _make_k8s_nfs_mt(self, vol):
        return self._make_k8s_hostpath_mt(vol)

    def _get_volume_name_for_mountpoint(self, mountpoint):
        return mountpoint[1:].replace('/', '-')

    def _get_volume_yaml(self, left_pad=0):
        pad = " " * left_pad
        vols = self.k8s_volumes
        if not vols:
            self.log.warning("No volumes defined.")
            return ''
        vl = []
        for vol in vols:
            nm = vol.name
            hp = vol.host_path
            nf = vol.nfs
            vo = {"name": nm}
            if hp:
                vo["hostPath"] = {"path": hp.path}
            elif nf:
                am = "ReadWriteMany"
                if nf.read_only:
                    am = "ReadOnlyMany"
                vo["nfs"] = {"server": nf.server,
                             "path": nf.path,
                             "accessMode": am}
            vl.append(vo)
        vs = {"volumes": vl}
        ystr = yaml.dump(vs)
        ylines = ystr.split("\n")
        padlines = [pad + l for l in ylines]
        return "\n".join(padlines)

    def _get_volume_mount_yaml(self, left_pad=0):
        pad = " " * left_pad
        vms = self.k8s_vol_mts
        if not vms:
            self.log.warning("No volume mounts defined.")
            return ''
        vl = []
        for vm in vms:
            vo = {}
            vo["name"] = vm.name
            vo["mountPath"] = vm.mount_path
            if vm.read_only:
                vo["readOnly"] = True
            vl.append(vo)
        vs = {"volumeMounts": vl}
        ystr = yaml.dump(vs)
        ylines = ystr.split("\n")
        padlines = [pad + l for l in ylines]
        return "\n".join(padlines)

    def get_dask_volume_b64(self):
        '''Return the base-64 encoding of the K8s statements to create
        the pod's mountpoints.  Probably better handled as a ConfigMap.
        '''
        vmt_yaml = self._get_volume_mount_yaml(left_pad=4)
        vol_yaml = self._get_volume_yaml(left_pad=2)
        ystr = vmt_yaml + "\n" + vol_yaml
        self.log.debug("Dask yaml:\n%s" % ystr)
        benc = base64.b64encode(ystr.encode('utf-8')).decode('utf-8')
        return benc
