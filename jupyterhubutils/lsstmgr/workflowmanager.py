import json
from argo.workflows.sdk import Workflow, template
# This is just a K8s V1Container.
from argo.workflows.sdk.templates import V1Container
from kubernetes.client import V1ResourceRequirements, V1SecurityContext
from kubernetes.client.rest import ApiException
from kubernetes import client
from .. import LoggableChild
from ..utils import list_digest, str_true


class LSSTWorkflowManager(LoggableChild):
    '''This is an LSST Manager Class containing LSST (er, Rubin Observatory)-
    specific logic regarding management of Argo Workflows.
    '''

    cmd_vol = None
    cmd_mt = None
    wf_api = None
    workflow = None
    cfg_map = None
    wf_input = None

    def define_configmap(self, data):
        '''This returns a k8s configmap using the data from the new-workflow
        POST.
        '''
        ni_cmd = data['command']
        idkey = list_digest(ni_cmd)
        cm_name = 'command.{}.json'.format(idkey)
        k8s_vol = client.V1Volume(
            name="noninteractive-command",
            config_map=client.V1ConfigMapVolumeSource(
                name=cm_name
            ),
        )
        k8s_mt = client.V1VolumeMount(
            name="noninteractive-command",
            mount_path="/opt/lsst/software/jupyterlab/noninteractive/command/",
            read_only=True
        )
        self.cmd_vol = k8s_vol
        self.cmd_mt = k8s_mt
        # Now the configmap
        cm_data = {}
        cm_data.update(data)
        del cm_data['image']
        del cm_data['size']
        jd = json.dumps(data, sort_keys=True, indent=4)
        k8s_configmap = client.V1ConfigMap(
            metadata=client.V1ObjectMeta(name=cm_name),
            data={'command.json': json.dumps(data)})
        self.log.debug("Created configmap '{}': {}".format(cm_name, jd))
        self.cfg_map = k8s_configmap

    def define_workflow(self, data):
        '''This is basically our equivalent of get_pod_manifest().
        It creates a dict which we will pass to the workflow template
        engine, which will allow it to create an appropriate workflow.
        '''
        wf_input = {}
        # FIXME Right now we can assume data is of type 'cmd'; we need
        # a little tweaking for 'nb' in that the command will be fixed
        # and the execution parameters will differ.
        cfg = self.parent.config
        em = self.parent.env_mgr
        vm = self.parent.volume_mgr
        am = self.parent.auth_mgr
        user = self.parent.parent.auth.user
        em_env = em.get_env()
        size_map = self._resolve_size(data['size'])
        self.log.debug(
            "Size '{}' resolves to '{}'".format(data['size'], size_map))
        ml = size_map['mem']
        cl = size_map['cpu']
        sr = float(cfg.lab_size_range)
        mg = None
        cg = None
        if data['size'] == "tiny":
            # Take the guarantees from the config object
            mg = cfg.mem_guarantee
            mgs = str(mg)
            # I really screwed up these defaults.
            while mgs and not mgs[-1].isdigit():
                mgs = mgs[:-1]
            if not mgs:
                mgs = '1.0'
            mg = float(mgs)
            cg = cfg.cpu_guarantee
        else:
            mg = float(ml / sr)
            cg = float(cl / sr)
        mg = int(mg)
        ml = int(ml)
        wf_input['mem_limit'] = ml
        wf_input['mem_guar'] = mg
        wf_input['cpu_limit'] = cl
        wf_input['cpu_guar'] = cg
        wf_input['image'] = data['image']
        env = {}
        vols = []
        vmts = []
        env.update(em_env)
        vols.extend(vm.k8s_volumes)
        vmts.extend(vm.k8s_vol_mts)
        self.define_configmap(data)
        vols.append(self.cmd_vol)
        vmts.append(self.cmd_mt)
        env['DASK_VOLUME_B64'] = vm.get_dask_volume_b64()
        cname = "wf-{}-{}-{}".format(
            user.escaped_name,
            data['image'].split(':')[-1].replace('_', '-'),
            data['command'][0].split('/')[-1].replace('_', '-'))
        wf_input['name'] = cname
        env['JUPYTERHUB_USER'] = user.escaped_name
        env['NONINTERACTIVE'] = "TRUE"
        env['EXTERNAL_UID'] = str(user.auth_state['uid'])
        env['EXTERNAL_GROUPS'] = am.get_group_string()
        env['DEBUG'] = str_true(cfg.debug)
        e_l = self._d2l(env)
        wf_input['env'] = e_l
        wf_input['username'] = user.escaped_name
        # Volumes and mounts aren't JSON-serializable...
        wf_input['vols'] = '{}'.format(vols)
        wf_input['vmts'] = '{}'.format(vmts)
        self.wf_input = wf_input
        self.log.debug("Input to Workflow Manipulator: {}".format(
            json.dumps(wf_input, indent=4, sort_keys=True)))
        # ...now put the real values back
        wf_input['vols'] = vols
        wf_input['vmts'] = vmts
        self.wf_input = wf_input
        wf = LSSTWorkflow(parms=wf_input)
        self.log.debug("Workflow: {}".format(wf))
        self.workflow = wf

    def _resolve_size(self, size):
        om = self.parent.optionsform_mgr
        return om.sizemap.get(size)

    def _d2l(self, in_d):
        ll = []
        for k in in_d:
            ll.append({"name": k,
                       "value": in_d[k]})
        return ll

    def list_workflows(self):
        namespace = self.parent.namespace_mgr.namespace
        api = self.parent.wf_api
        self.log.debug(
            "Listing workflows in namespace '{}'".format(namespace))
        nl = self.parent.api.list_namespace(timeout_seconds=1)
        found = False
        for ns in nl.items:
            nsname = ns.metadata.name
            if nsname == namespace:
                self.log.debug("Namespace {} found.".format(namespace))
                found = True
                break
        if not found:
            self.log.debug("No namespace {} found.".format(namespace))
            wfs = None
        else:
            wfs = api.list_namespaced_workflows(namespace=namespace)
        return wfs

    def create_workflow(self):
        workflow = self.workflow
        namespace = self.parent.namespace_mgr.namespace
        api = self.parent.wf_api
        self.create_configmap()
        self.log.debug(
            "Creating workflow in namespace {}: '{}'".format(
                namespace, workflow))
        wf = api.create_namespaced_workflow(namespace, workflow)
        return wf

    def create_configmap(self):
        api = self.parent.api  # Core, not Workflow
        namespace = self.parent.namespace_mgr.namespace
        cfgmap = self.cfg_map
        try:
            self.log.info(
                "Attempting to create configmap in {}".format(namespace))
            api.create_namespaced_config_map(namespace, cfgmap)
        except ApiException as e:
            if e.status != 409:
                estr = "Create configmap failed: {}".format(e)
                self.log.exception(estr)
                raise
            else:
                self.log.info("Configmap already exists.")

    def submit_workflow(self, data):
        self.define_workflow(data)
        nm = self.parent.namespace_mgr
        nm.ensure_namespace()
        nm.ensure_namespaced_service_account()
        wf = self.create_workflow()
        return wf

    def delete_workflow(self, wfid):
        namespace = self.parent.namespace_mgr.namespace
        api = self.parent.wf_api
        wf = api.delete_namespaced_workflow(namespace, wfid)
        return wf

    def get_workflow(self, wfid):
        namespace = self.parent.namespace_mgr.namespace
        api = self.parent.wf_api
        wf = api.get_namespaced_workflow(namespace, wfid)
        return wf

    def dump(self):
        '''Return contents dict for aggregation and pretty-printing.
        '''
        wd = {"parent": str(self.parent),
              "cmd_vol": str(self.cmd_vol),
              "workflow": str(self.workflow),
              "cfg_map": self.cfg_map,
              "wf_input": self.wf_input
              }
        return wd


class LSSTWorkflow(Workflow):
    parms = {}
    entrypoint = "noninteractive"
    run_as_user = 769
    run_as_group = 769

    def __init__(self, *args, **kwargs):
        self.parms = kwargs.pop('parms')
        super().__init__(*args, **kwargs)
        self.spec.volumes = self.parms['vols']
        lbl = {'argocd.argoproj.io/instance': 'nublado-users'}
        self.metadata.labels = lbl
        self.metadata.generate_name = self.parms['name'] + '-'
        self.metadata.name = None
        username = self.parms['username']
        account = "{}-svcacct".format(username)
        self.spec.service_account_name = account

    @template
    def noninteractive(self) -> V1Container:
        container = V1Container(
            command=["/opt/lsst/software/jupyterlab/provisionator.bash"],
            args=[],
            image=self.parms["image"],
            name=self.parms["name"],
            env=self.parms["env"],
            image_pull_policy="Always",
            volume_mounts=self.parms["vmts"],
            resources=V1ResourceRequirements(
                limits={"memory": "{}M".format(self.parms['mem_limit']),
                        "cpu": "{}".format(self.parms['cpu_limit'])},
                requests={"memory": "{}M".format(self.parms['mem_guar']),
                          "cpu": "{}".format(self.parms['cpu_guar'])}),
            security_context=V1SecurityContext(
                run_as_group=self.run_as_group,
                run_as_user=self.run_as_user,
            )
        )
        self.volumes = self.parms['vols']
        lbl = {'argocd.argoproj.io/instance': 'nublado-users'}
        self.metadata.labels = lbl
        self.metadata.generate_name = self.parms['name'] + '-'
        self.metadata.name = None
        self.service_account_name = self.parms['username'] + '-svcacct'

        return container
