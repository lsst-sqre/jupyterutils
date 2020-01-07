'''This is a JupyterHub KubeSpawner, extended with the ability to manipulate
namespaces, and with an lsst_mgr attribute.
'''
import json
from .multispawner import MultiNamespacedKubeSpawner
from kubespawner.objects import make_pod
from tornado import gen
from traitlets import Bool
from ..utils import make_logger, sanitize_dict


class LSSTSpawner(MultiNamespacedKubeSpawner):
    '''This, plus the LSST Manager class structure, implements the
    LSST-specific parts of our spawning requirements.
    '''
    lsst_mgr = None
    delete_grace_period = 5
    # In our LSST setup, there is a "provisionator" user, uid/gid 769,
    #  that is who we should start as.
    uid = 769
    gid = 769
    # The fields need to be defined; we don't use them.
    fs_gid = None
    supplemental_gids = []
    extra_labels = {}
    extra_annotations = []
    image_pull_secrets = None
    privileged = False
    working_dir = None
    lifecycle_hooks = {}  # This one will be useful someday.
    init_containers = []
    lab_service_account = None
    extra_container_config = None
    extra_pod_config = None
    extra_containers = []

    delete_namespace_on_stop = Bool(
        True,
        config=True,
        help='''
        If True, the entire namespace will be deleted when the lab pod stops.
        '''
    ).tag(config=True)

    enable_namespace_quotas = Bool(
        True,
        config=True,
        help='''
        If True, will create a ResourceQuota object by calling
        `self.quota_mgr.get_resource_quota_spec()` and create a quota with
        the resulting specification within the namespace.

        A subclass should override the quota manager's
        define_resource_quota_spec() to build a
        situationally-appropriate resource quota spec.
        '''
    ).tag(config=True)

    def __init__(self, *args, **kwargs):
        self.log = make_logger()
        super().__init__(*args, **kwargs)
        self.log.debug("Creating LSSTSpawner.")
        # Our API and our RBAC API are set in the super() __init__()
        # We assume that we're using an LSST Authenticator, which will
        #  therefore have an LSST MiddleManager.
        #
        # This might change with Argo Workflow.
        # self.log.debug("Spawner: {}".format(json.dumps(self.dump())))
        self.log.debug("Initialized {}".format(__name__))

    def auth_state_hook(self, auth_state):
        # Turns out this is in the wrong place.  It should be called
        #  _before_ get_options_form()
        super().auth_state_hook()
        self.log.debug("{} auth_state_hook firing.".format(__name__))

    @gen.coroutine
    def get_options_form(self):
        '''Present an LSST-tailored options form; delegate to options
        form manager.

        This really is stuff that should get set from auth_state_hook...
        But as it happens, that doesn't run until after get_options_form.
        '''

        self.log.debug("Setting LSST Manager from authenticated user.")
        auth = self.user.authenticator
        lm = auth.lsst_mgr
        self.lsst_mgr = lm
        lm.spawner = self
        lm.user = self.user
        lm.api = self.api
        lm.rbac_api = self.rbac_api
        om = lm.optionsform_mgr
        auth_state = yield self.user.get_auth_state()
        if not auth_state:
            raise ValueError("Auth state empty for user {}".format(self.user))
        lm.auth_mgr.auth_state = auth_state
        _ = yield self.asynchronize(lm.auth_mgr.parse_auth_state)
        # Will throw error if no UID
        form = yield self.asynchronize(om.get_options_form)
        return form

    def set_user_namespace(self):
        '''Get namespace and store it here (for spawning) and in
        namespace_mgr.'''
        ns = self.get_user_namespace()
        self.namespace = ns
        self.lsst_mgr.namespace_mgr.set_namespace(ns)

    def get_user_namespace(self):
        '''Return namespace for user pods (and ancillary objects).
        '''
        defname = self._namespace_default()
        # We concatenate the default namespace and the name so that we
        #  can continue having multiple Jupyter instances in the same
        #  k8s cluster in different namespaces.  The user namespaces must
        #  themselves be namespaced, as it were.
        if defname == "default":
            raise ValueError("Won't spawn into default namespace!")
        return "{}-{}".format(defname, self.user.escaped_name)

    def start(self):
        # All we need to do is ensure the namespace and K8s ancillary
        #  resources before we run the superclass method to spawn a pod,
        #  so we have the namespace to spawn into, and the service account
        #  with appropriate roles and rolebindings.
        self.log.debug("Starting; creating namespace and ancillary objects.")
        self.set_user_namespace()  # Set namespace here and in namespace_mgr
        self.lsst_mgr.ensure_resources()
        retval = super().start()
        return retval

    @gen.coroutine
    def stop(self, now=False):
        '''After stopping pod, delete the namespace if that option is set.
        '''
        _ = yield super().stop(now)
        if self.delete_namespace_on_stop:
            nsm = self.lsst_mgr.namespace_mgr
            self.log.debug("Attempting to delete namespace.")
            self.asynchronize(nsm.maybe_delete_namespace)
        else:
            self.log.debug("'delete_namespace_on_stop' not set.")

    def options_from_form(self, formdata=None):
        '''Delegate to form manager.
        '''
        return self.lsst_mgr.optionsform_mgr.options_from_form(formdata)

    @gen.coroutine
    def get_pod_manifest(self):
        # Extend pod manifest.  This is a monster method.
        # Run the superclass version, and then extract the fields
        self.log.debug("Creating pod manifest.")
        orig_pod = yield super().get_pod_manifest()
        labels = orig_pod.metadata.labels.copy()
        annotations = orig_pod.metadata.annotations.copy()
        ctrs = orig_pod.spec.containers
        cmd = None
        if ctrs and len(ctrs) > 0:
            cmd = ctrs[0].args or ctrs[0].command
        # That should be it from the standard get_pod_manifest
        # Now we finally need all that data we have been managing.
        cfg = self.lsst_mgr.config
        em = self.lsst_mgr.env_mgr
        am = self.lsst_mgr.auth_mgr
        nm = self.lsst_mgr.namespace_mgr
        vm = self.lsst_mgr.volume_mgr
        # Get the standard env and then update it with the environment
        # from our environment manager, except that we want the tokens from
        # the standard env
        tokens = {}
        pod_env = self.get_env()
        for fld in ['JUPYTERHUB_API_TOKEN', 'JPY_API_TOKEN']:
            val = pod_env.get(fld)
            if val:
                tokens[fld] = val
        em.create_pod_env()
        pod_env.update(em.get_env())
        pod_env.update(tokens)
        # And now glue in environment information that was stashed at
        #  authentication time (UID/GIDs, authenticator-specific settings)
        #  in the auth_mgr.
        pod_env.update(am.pod_env)
        # Set some constants
        # First pulls can be really slow for the LSST stack containers,
        #  so let's give it a big timeout (this is in seconds)
        self.http_timeout = 60 * 15
        self.start_timeout = 60 * 15
        # We are running the Lab at the far end, not the old Notebook
        self.default_url = '/lab'
        # We always want to check for refreshed images.
        self.image_pull_policy = 'Always'
        # If we can spawn other pods from the Lab, we need a service account.
        self.lab_service_account = nm.service_account
        # Get image name
        pod_name = self.pod_name
        # Get default image name; we will try to replace from options form.
        image = self.image or self.orig_pod.image or cfg.lab_default_image
        # Same with tag.
        tag = "latest"
        # Parse options form result.
        size = None
        image_size = None
        clear_dotlocal = False
        if not self.user_options:
            raise ValueError("No options form data!")
        self.log.debug("User options from form:\n" +
                       json.dumps(self.user_options, sort_keys=True, indent=4))
        if self.user_options.get('kernel_image'):
            image = self.user_options.get('kernel_image')
            om = self.lsst_mgr.optionsform_mgr
            size = self.user_options.get('size')
            if size:
                image_size = om.sizemap[size]
                self.log.debug("Image size: {}".format(
                    json.dumps(image_size, sort_keys=True, indent=4)))
            colon = image.find(':')
            if colon > -1:
                imgname = image[:colon]
                tag = image[(colon + 1):]
                if tag == "recommended" or tag.startswith("latest"):
                    # Resolve convenience tags to real build tags.
                    self.log.debug("Resolving tag '{}'".format(tag))
                    qtag = om.resolve_tag(tag)
                    if qtag:
                        tag = qtag
                        image = imgname + ":" + tag
                    else:
                        self.log.warning(
                            "Failed to resolve tag '{}'".format(tag))
                self.log.debug("Image name: %s ; tag: %s" % (imgname, tag))
                if tag == "__custom":
                    self.log.debug("Tag is __custom: retrieving real value " +
                                   "from drop-down list.")
                    cit = self.user_options.get('image_tag')
                    if cit:
                        tag = cit
                        image = imgname + ":" + cit
            self.log.debug("Replacing image from options form: %s" % image)
            self.image = image
        pod_env['JUPYTER_IMAGE_SPEC'] = image
        pod_env['JUPYTER_IMAGE'] = image
        # Set flag to clear .local if indicated
        clear_dotlocal = self.user_options.get('clear_dotlocal')
        if clear_dotlocal:
            pod_env['CLEAR_DOTLOCAL'] = "TRUE"
        # Set up Lab pod resource constraints (not namespace quotas)
        # These are the defaults from the config
        mem_limit = cfg.mem_limit
        cpu_limit = cfg.cpu_limit
        if image_size:
            mem_limit = str(int(image_size["mem"])) + "M"
            cpu_limit = image_size["cpu"]
        cpu_limit = float(cpu_limit)
        self.mem_limit = mem_limit
        self.cpu_limit = cpu_limit
        mem_guar = cfg.mem_guarantee
        cpu_guar = cfg.cpu_guarantee
        cpu_guar = float(cpu_guar)
        # Tiny gets the configured (almost nothing) guarantee.
        #  All others get 1/LAB_SIZE_RANGE times their maximum,
        #  with a default of 1/4.
        size_range = float(cfg.lab_size_range)
        if image_size and size != 'tiny':
            mem_guar = int(image_size["mem"] / size_range)
            cpu_guar = float(image_size["cpu"] / size_range)
        self.mem_guarantee = mem_guar
        self.cpu_guarantee = cpu_guar
        pod_env['MEM_GUARANTEE'] = str(mem_guar) + "M"
        pod_env['MEM_LIMIT'] = mem_limit
        pod_env['CPU_GUARANTEE'] = str(cpu_guar)
        pod_env['CPU_LIMIT'] = str(cpu_limit)
        # We don't care about the image name anymore: the user pod will
        #  be named "nb" plus the username and tag, to keep the pod name
        #  short.
        rt_tag = tag.replace('_', '-')
        pn_template = "nb-{username}-" + rt_tag
        pod_name = self._expand_user_properties(pn_template)
        self.pod_name = pod_name
        self.log.debug("Replacing pod name from options form: %s" % pod_name)
        # Get quota definitions from quota manager.
        if self.enable_namespace_quotas:
            qmq = self.lsst_mgr.quota_mgr.quota
            if qmq:
                if "limits.cpu" in qmq:
                    pod_env['NAMESPACE_CPU_LIMIT'] = qmq["limits.cpu"]
                if "limits.memory" in qmq:
                    nmlimit = qmq["limits.memory"]
                    if nmlimit[-2:] == "Mi":
                        nmlimit = nmlimit[:-2] + "M"
                    pod_env['NAMESPACE_MEM_LIMIT'] = nmlimit
        # Get volume definitions from volume manager.
        vm.make_volumes_from_config()
        pod_env['DASK_VOLUME_B64'] = vm.get_dask_volume_b64()
        self.volumes = vm.k8s_volumes
        self.volume_mounts = vm.k8s_vol_mts
        # Generate the pod definition.
        sanitized_env = sanitize_dict(pod_env, [
            'ACCESS_TOKEN', 'GITHUB_ACCESS_TOKEN',
            'JUPYTERHUB_API_TOKEN', 'JPY_API_TOKEN'])
        self.log.debug("Pod environment: {}".format(json.dumps(sanitized_env,
                                                               sort_keys=True,
                                                               indent=4)))
        self.log.debug("About to run make_pod()")
        pod = make_pod(
            name=self.pod_name,
            cmd=cmd,
            port=self.port,
            image=self.image,
            image_pull_policy=self.image_pull_policy,
            image_pull_secret=self.image_pull_secrets,
            node_selector=self.node_selector,
            run_as_uid=self.uid,
            run_as_gid=self.gid,
            fs_gid=self.fs_gid,
            supplemental_gids=self.supplemental_gids,
            run_privileged=self.privileged,
            env=pod_env,
            volumes=self._expand_all(self.volumes),
            volume_mounts=self._expand_all(self.volume_mounts),
            working_dir=self.working_dir,
            labels=labels,
            annotations=annotations,
            cpu_limit=self.cpu_limit,
            cpu_guarantee=self.cpu_guarantee,
            mem_limit=self.mem_limit,
            mem_guarantee=self.mem_guarantee,
            extra_resource_limits=self.extra_resource_limits,
            extra_resource_guarantees=self.extra_resource_guarantees,
            lifecycle_hooks=self.lifecycle_hooks,
            init_containers=self._expand_all(self.init_containers),
            service_account=self.lab_service_account,
            extra_container_config=self.extra_container_config,
            extra_pod_config=self.extra_pod_config,
            extra_containers=self.extra_containers,
            node_affinity_preferred=self.node_affinity_preferred,
            node_affinity_required=self.node_affinity_required,
            pod_affinity_preferred=self.pod_affinity_preferred,
            pod_affinity_required=self.pod_affinity_required,
            pod_anti_affinity_preferred=self.pod_anti_affinity_preferred,
            pod_anti_affinity_required=self.pod_anti_affinity_required,
            priority_class_name=self.priority_class_name,
            logger=self.log,
        )
        return pod

    def dump(self):
        '''Return dict representation suitable for pretty-printing.'''
        sd = {"namespace": self.namespace,
              "uid": self.uid,
              "gid": self.gid,
              "fs_gid": self.fs_gid,
              "supplemental_gids": self.supplemental_gids,
              "extra_labels": self.extra_labels,
              "extra_annotations": self.extra_annotations,
              "delete_grace_period": self.delete_grace_period,
              "privileged": self.privileged,
              "working_dir": self.working_dir,
              "lifecycle_hooks": self.lifecycle_hooks,
              "init_containers": self.init_containers,
              "lab_service_account": self.lab_service_account,
              "extra_container_config": self.extra_container_config,
              "extra_pod_config": self.extra_pod_config,
              "extra_containers": self.extra_containers,
              "delete_namespace_on_stop": self.delete_namespace_on_stop,
              "enable_namespace_quotas": self.enable_namespace_quotas,
              "lsst_mgr": self.lsst_mgr.dump()}
        return sd
