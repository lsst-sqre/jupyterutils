import base64
import json
import os
import yaml
from eliot import start_action
from kubernetes import client
from .. import LoggableChild


class LSSTVolumeManager(LoggableChild):
    '''Class to provide support for document-driven Volume assignment.
    '''

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.volume_list = []
        self.k8s_volumes = []
        self.k8s_vol_mts = []

    def make_volumes_from_config(self):
        '''Create volume definition representation from document.
        Override this in a subclass if you like.
        '''
        with start_action(action_type="make_volumes_from_config"):
            vollist = []
            config = []
            config_file = self.parent.config.volume_definition_file
            if not os.path.exists(config_file):
                return vollist
            with open(config_file, "r") as fp:
                config = json.load(fp)
            for mtpt in config:
                mountpoint = mtpt["mountpoint"]  # Fatal error if it
                # doesn't exist
                if mtpt.get("disabled"):
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
            self.volume_list = vollist
            self.log.debug("Volumes: {}".format(vollist))
            self._define_k8s_object_representations()

    def _define_k8s_object_representations(self):
        with start_action(action_type="_define_k8s_object_representations"):
            self.k8s_volumes = []
            self.k8s_vol_mts = []
            for vol in self.volume_list:
                k8svol = None
                k8smt = None
                if vol.get("hostpath"):
                    k8svol = self._define_k8s_hostpath_vol(vol)
                else:
                    k8svol = self._define_k8s_nfs_vol(vol)
                k8smt = self._define_k8s_mtpt(vol)
                if k8svol and k8smt:
                    self.k8s_volumes.append(k8svol)
                    self.k8s_vol_mts.append(k8smt)

    def _define_k8s_hostpath_vol(self, vol):
        with start_action(action_type="_define_k8s_hostpath_vol"):
            return client.V1Volume(
                name=self._get_volume_name_for_mountpoint(vol["mountpoint"]),
                host_path=client.V1HostPathVolumeSource(
                    path=vol["hostpath"]
                )
            )

    def _define_k8s_nfs_vol(self, vol):
        with start_action(action_type="_define_k8s_nfs_vol"):
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

    def _define_k8s_mtpt(self, vol):
        with start_action(action_type="_define_k8s_mtpt"):
            mt = client.V1VolumeMount(
                mount_path=vol["mountpoint"],
                name=self._get_volume_name_for_mountpoint(vol["mountpoint"]),
            )
            if vol["mode"] == "ro":
                mt.read_only = True
            return mt

    def _get_volume_name_for_mountpoint(self, mountpoint):
        with start_action(action_type="_get_volume_name_for_mountpoint"):
            return mountpoint[1:].replace('/', '-')

    def _get_volume_yaml_str(self, left_pad=0):
        with start_action(action_type="_get_volume_yaml_str"):
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
            return self._left_pad(ystr, left_pad)

    def _get_volume_mount_yaml_str(self, left_pad=0):
        with start_action(action_type="_get_volume_mount_yaml_str"):
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
            return self._left_pad(ystr, left_pad)

    def _left_pad(self, line_str, left_pad=0):
        with start_action(action_type="_left_pad"):
            pad = ' ' * left_pad
            ylines = line_str.split("\n")
            padlines = [pad + l for l in ylines]
            return "\n".join(padlines)

    def get_dask_volume_b64(self):
        '''Return the base-64 encoding of the K8s statements to create
        the pod's mountpoints.  Probably better handled as a ConfigMap.
        '''
        with start_action(action_type="get_dask_volume_b64"):
            vmt_yaml_str = self._get_volume_mount_yaml_str(left_pad=4)
            vol_yaml_str = self._get_volume_yaml_str(left_pad=2)
            ystr = "{}\n{}".format(vmt_yaml_str, vol_yaml_str)
            benc = base64.b64encode(ystr.encode('utf-8')).decode('utf-8')
            return benc

    def dump(self):
        '''Return contents dict for aggregation and pretty-printing.
        '''
        vd = {"parent": str(self.parent),
              "volume_list": self.volume_list,
              "k8s_volumes": [str(x) for x in self.k8s_volumes],
              "k8s_vol_mts": [str(x) for x in self.k8s_vol_mts]}
        return vd

    def toJSON(self):
        return json.dumps(self.dump())
