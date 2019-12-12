'''This is a JupyterHub KubeSpawner, extended with the ability to manipulate
namespaces, and with an lsst_mgr attribute.
'''
import json
from .multispawner import MultiNamespacedKubeSpawner
from kubespawner.objects import make_pod
from tornado import gen
from traitlets import Bool
from ..utils import make_logger


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
        Set delete_namespaced_pvs_on_stop to True if you also want to
        delete shadow PVs created.
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
        get_resource_quota_spec() to create a
        situationally-appropriate resource quota spec.
        '''
    ).tag(config=True)

    def __init__(self, *args, **kwargs):
        self.log = make_logger()
        self.log.debug("Creating LSSTSpawner.")
        super().__init__(*args, **kwargs)
    # Our API and our RBAC API are set in the super() __init__()
    # We assume that we're using an LSST Authenticator, which will
    #  therefore have an LSST MiddleManager.
    #
    # This might change with Argo Workflow.

    @property
    def options_form(self):
        '''Present an LSST-tailored options form.'''
        # Weird place to stitch up the LSST Manager, huh?
        # This is the last place we can do it, because we need one for
        #  the options form.
        self.log.info("Updating LSSR manager with spawner information.")
        self._set_lsst_mgr()
        # We may want to know about the user's resource limits for the
        #  option form.
        self.lsst_mgr.quota_mgr.set_custom_user_resources()
        return self.lsst_mgr.optionsform_mgr.lsst_options_form()

    def _set_lsst_mgr(self):
        self.log.info("Setting LSST Manager from authenticated user.")
        lm = self.user.authenticator.lsst_mgr
        self.lsst_mgr = lm
        lm.spawner = self
        lm.user = self.user
        lm.username = self.user.escaped_name
        lm.api = self.api
        lm.rbac_api = self.rbac_api

    def get_user_namespace(self):
        '''Return namespace for user pods (and ancillary objects).
        '''
        defname = self._namespace_default()
        # We concatenate the default namespace and the name so that we
        #  can continue having multiple Jupyter instances in the same
        #  k8s cluster in different namespaces.  The user namespaces must
        #  themselves be namespaced, as it were.
        if defname == "default":
            # Or we're just running in the default namespace
            return defname
            return "{}-{}".format(defname, self.user.escaped_name)
        return defname

    def start(self):
        # All we need to do is ensure the resources before we run the
        #  superclass method
        self.log.debug("Starting; creating namespace and ancillary objects.")
        self.lsst_mgr.ensure_resources()
        retval = super().start()
        return retval

    @gen.coroutine
    def stop(self, now=False):
        self.log.debug("Stopping; about to call original stop() method.")
        _ = yield super().stop(now)
        self.log.debug("Returned from original stop().")
        if self.delete_namespace_on_stop:
            nsm = self.lsst_mgr.namespace_mgr
            self.log.debug("Attempting to delete namespace.")
            self.asynchronize(nsm.maybe_delete_namespace)
        else:
            self.log.debug("'delete_namespace_on_stop' not set.")

    def options_from_form(self, formdata=None):
        options = None
        if formdata:
            self.log.debug("Form data: %s", json.dumps(formdata,
                                                       sort_keys=True,
                                                       indent=4))
            options = {}
            if ('kernel_image' in formdata and formdata['kernel_image']):
                options['kernel_image'] = formdata['kernel_image'][0]
            if ('size' in formdata and formdata['size']):
                options['size'] = formdata['size'][0]
            if ('image_tag' in formdata and formdata['image_tag']):
                options['image_tag'] = formdata['image_tag'][0]
            if ('clear_dotlocal' in formdata and formdata['clear_dotlocal']):
                options['clear_dotlocal'] = True
        return options

    @gen.coroutine
    def get_pod_manifest(self):
        # Extend pod manifest.  This is a monster method.
        # Run the superclass version, and then extract the fields
        orig_pod = yield super().get_pod_manifest()
        self.log.debug("Original pod: {}".format(orig_pod))
        labels = orig_pod.metadata.labels.copy()
        annotations = orig_pod.metadata.annotations.copy()
        ctrs = orig_pod.spec.containers
        cmd = None
        if ctrs and len(ctrs) > 0:
            cmd = ctrs[0].args or ctrs[0].command
        # That should be it from the standard get_pod_manifest

        # Get the standard env and then update it with the environment
        # from our environment manager:
        pod_env = self.get_env()
        em = self.lsst_mgr.env_mgr
        cfg = self.lsst_mgr.config
        em.refresh_pod_env()
        pod_env.update(em.get_env())
        self.log.debug("Initial pod env: %s" % json.dumps(pod_env,
                                                          indent=4,
                                                          sort_keys=True))
        # If we do not have a UID for the user by now, we're sunk.  It
        #  should have been set during authentication.
        if not pod_env.get('EXTERNAL_UID'):
            raise ValueError("Cannot determine user uid!")

        # Now we do the custom LSST stuff

        # Get image name
        self.lab_service_account = None
        if cfg.allow_dask_spawn:
            self.lab_service_account = "dask"
        pod_name = self.pod_name
        image = (cfg.lab_default_image or
                 self.image or
                 self.orig_pod.image or
                 "lsstsqre/sciplat-lab:latest")
        tag = "latest"
        size = None
        image_size = None
        # First pulls can be really slow for the LSST stack containers,
        #  so let's give it a big timeout (this is in seconds)
        self.http_timeout = 60 * 15
        self.start_timeout = 60 * 15
        # We are running the Lab at the far end, not the old Notebook
        self.default_url = '/lab'
        # We always want to check for refreshed images.
        self.image_pull_policy = 'Always'
        # Parse options form result.
        clear_dotlocal = False
        if self.user_options:
            self.log.debug("user_options: " +
                           json.dumps(self.user_options, sort_keys=True,
                                      indent=4))
            if self.user_options.get('kernel_image'):
                image = self.user_options.get('kernel_image')
                om = self.lsst_mgr.optionsform_mgr
                size = self.user_options.get('size')
                if size:
                    image_size = om._sizemap[size]
                clear_dotlocal = self.user_options.get('clear_dotlocal')
                colon = image.find(':')
                if colon > -1:
                    imgname = image[:colon]
                    tag = image[(colon + 1):]
                    if tag == "recommended" or tag.startswith("latest"):
                        # Resolve convenience tags to real build tags.
                        self.log.info("Resolving tag '{}'".format(tag))
                        qtag = om.resolve_tag(tag)
                        if qtag:
                            tag = qtag
                            image = imgname + ":" + tag
                        else:
                            self.log.warning(
                                "Failed to resolve tag '{}'".format(tag))
                    self.log.debug("Image name: %s ; tag: %s" % (imgname, tag))
                    if tag == "__custom":
                        cit = self.user_options.get('image_tag')
                        if cit:
                            image = imgname + ":" + cit
                self.log.info("Replacing image from options form: %s" % image)
        else:
            self.log.warning("No user options found.  That seems wrong.")
        if clear_dotlocal:
            pod_env['CLEAR_DOTLOCAL'] = "TRUE"
        # Set up Lab pod resource constraints (not namespace quotas)
        mem_limit = em.get_env_key('LAB_MEM_LIMIT')
        cpu_limit = em.get_env_key('LAB_CPU_LIMIT')
        if image_size:
            mem_limit = str(int(image_size["mem"])) + "M"
            cpu_limit = image_size["cpu"]
        cpu_limit = float(cpu_limit)
        self.mem_limit = mem_limit
        self.cpu_limit = cpu_limit
        mem_guar = em.get_env_key('MEM_GUARANTEE')
        cpu_guar = em.get_env_key('CPU_GUARANTEE')
        cpu_guar = float(cpu_guar)
        # Tiny gets the "basically nothing" above (or the explicit
        #  guarantee).  All others get 1/LAB_SIZE_RANGE times their
        #  maximum, with a default of 1/4.
        size_range = em.get_env_key('LAB_SIZE_RANGE')
        size_range = float(size_range)
        if image_size and size != 'tiny':
            mem_guar = int(image_size["mem"] / size_range)
            cpu_guar = float(image_size["cpu"] / size_range)
        self.mem_guarantee = mem_guar
        self.cpu_guarantee = cpu_guar
        self.log.debug("Image: {}".format(image))
        self.image = image
        pod_env['JUPYTER_IMAGE_SPEC'] = image
        em.update_env(pod_env)
        # We don't care about the image name anymore: the user pod will
        #  be named "nb" plus the username and tag, to keep the pod name
        #  short.
        rt_tag = tag.replace('_', '-')
        pn_template = "nb-{username}-" + rt_tag
        pod_name = self._expand_user_properties(pn_template)
        self.pod_name = pod_name
        self.log.info("Replacing pod name from options form: %s" %
                      pod_name)
        # Get volume definitions from volume manager.
        self.volumes = self.lsst_mgr.volume_mgr.k8s_volumes
        self.volume_mounts = self.lsst_mgr.volume_mgr.k8s_vol_mts
        # Generate the pod definition.
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
