'''This is a JupyterHub KubeSpawner, extended with the ability to manipulate
namespaces, and with an lsst_mgr attribute.
'''
import os

import json
from kubespawner import KubeSpawner
from kubespawner.objects import make_pod
from tornado import gen
from traitlets import Bool

from .. import LSSTMiddleManager
from ..utils import str_bool, make_logger


class LSSTSpawner(KubeSpawner):
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
        help='''
        If True, the entire namespace will be deleted when the lab pod stops.
        Set delete_namespaced_pvs_on_stop to True if you also want to
        delete shadow PVs created.
        '''
    )

    duplicate_nfs_pvs_to_namespace = Bool(
        True,
        help='''
        If true, all NFS PVs in the JupyterHub namespace will be replicated
        to the user namespace.
        '''
    )

    delete_namespaced_pvs_on_stop = Bool(
        True,
        help='''
        If True, and delete_namespace_on_stop is also True, any shadow PVs
        created for the user namespace will be deleted when the lab pod
        stops.
        '''
    )

    enable_namespace_quotas = Bool(
        True,
        help='''
        If True, will create a ResourceQuota object by calling
        `self.quota_mgr.get_resource_quota_spec()` and create a quota with
        the resulting specification within the namespace.

        A subclass should override the quota manager's
        get_resource_quota_spec() to create a
        situationally-appropriate resource quota spec.
        '''
    )

    def __init__(self, *args, **kwargs):
        _mock = kwargs.get('_mock', False)  # Don't pop it; superclass needs it
        super().__init__(*args, **kwargs)
        self.debug = kwargs.pop('debug', str_bool(os.getenv('DEBUG')) or False)
        self.log = make_logger(name=__name__, debug=self.debug)
        auth = None
        if hasattr(self.user, "authenticator"):
            auth = self.user.authenticator
        # If the authenticator is one of ours, it will have an
        #  LSSTMiddleManager associated with it; use that if it exists.
        if hasattr(auth, "lsst_mgr"):
            self.lsst_mgr = auth.lsst_mgr
        else:
            self.lsst_mgr = LSSTMiddleManager(
                parent=self, user=self.user, authenticator=auth,
                debug=self.debug, _mock=_mock)
        # Add the LSST-specific logic by gluing in manager methods
        lm = self.lsst_mgr
        nm = lm.namespace_mgr
        nm.delete_namespace_on_stop = self.delete_namespace_on_stop
        nm.delete_namespaced_pvs_on_stop = self.delete_namespaced_pvs_on_stop
        nm.duplicate_nfs_pvs_to_namespace = self.duplicate_nfs_pvs_to_namespace
        nm.enable_namespace_quotas = self.enable_namespace_quotas
        self.optionsform_mgr = lm.optionsform_mgr
        # Swap in our extended start and stop methods
        #  And also our get_pod_manifest extensions
        self._orig_stop = self.stop
        self.stop = self._new_stop
        self._orig_get_pod_manifest = self.get_pod_manifest
        self.get_pod_manifest = self._new_get_pod_manifest

        # Update fields
        if os.getenv("RESTRICT_LAB_SPAWN"):
            self.extra_labels["jupyterlab"] = "ok"
        if os.getenv("ALLOW_DASK_SPAWN"):
            self.lab_service_account = "dask"
        if _mock:
            # Only do this during testing
            pass
        else:
            # Normal initialization only
            pass

    @property
    def options_form(self):
        return self.optionsform_mgr.lsst_options_form()

    def get_user_namespace(self):
        '''Return namespace for user pods (and ancillary objects).
        '''
        defname = self._namespace_default()
        # We concatenate the default namespace and the name so that we
        #  can continue having multiple Jupyter instances in the same
        #  k8s cluster in different namespaces.  The user namespaces must
        #  themselves be namespaced, as it were.
        if self.user and self.user.name:
            uname = self.user.name
            return defname + "-" + uname
        return defname

    def _refresh_mynamespace(self):
        self._mynamespace = self._get_mynamespace()

    def _get_mynamespace(self):
        ns_path = '/var/run/secrets/kubernetes.io/serviceaccount/namespace'
        if os.path.exists(ns_path):
            with open(ns_path) as f:
                return f.read().strip()
        return None

    @gen.coroutine
    def _new_stop(self, now=False):
        self.log.debug("Stopping; about to call original stop() method.")
        _ = yield self._orig_stop(now)
        self.log.debug("Returned from original stop().")
        if self.delete_namespace_on_stop:
            self.lsst_mgr.propagate_user(self.user)
            nsm = self.lsst_mgr.namespace_mgr
            vlm = self.lsst_mgr.volume_mgr
            self.log.debug("Attempting to delete namespace.")
            self.asynchronize(nsm.maybe_delete_namespace)
            if self.delete_namespaced_pvs_on_stop:
                self.asynchronize(vlm.destroy_namespaced_pvs)
        else:
            self.log.debug("delete_namespace_on_stop not set.")

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
    def _new_get_pod_manifest(self):
        # Extend pod manifest.  This is a monster method.
        # Run the superclass version, and then extract the fields
        orig_pod = yield self._orig_get_pod_manifest()
        sc = orig_pod.spec.security_context
        uid = self.uid
        gid = self.gid
        fs_gid = self.fs_gid
        supplemental_gids = self.supplemental_gids
        if hasattr(sc, "run_as_uid") and sc.run_as_uid is not None:
            uid = sc.run_as_uid
        if hasattr(sc, "run_as_gid") and sc.run_as_gid is not None:
            gid = sc.run_as_gid
        if hasattr(sc, "fs_group") and sc.fs_group is not None:
            fs_gid = sc.fs_group
        if hasattr(sc, "supplemental_groups"):
            if sc.supplemental_groups is not None:
                supplemental_gids = sc.supplemental_groups
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
        em.create_pod_env()
        pod_env.update(em.get_env())
        self.log.debug("Pod env: %s" % json.dumps(pod_env,
                                                  indent=4,
                                                  sort_keys=True))
        # If we do not have a UID for the user by now, we're sunk.
        if not pod_env.get("EXTERNAL_UID"):
            raise ValueError("EXTERNAL_UID is not set!")

        # Now we do the custom LSST stuff

        # Get image name
        if os.getenv("ALLOW_DASK_SPAWN"):
            self.lab_service_account = "dask"
        pod_name = self.pod_name
        image = (os.getenv("LAB_IMAGE") or
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
            om = self.optionsform_mgr
            if self.user_options.get('kernel_image'):
                image = self.user_options.get('kernel_image')
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
                size = self.user_options.get('size')
                if size:
                    image_size = om._sizemap[size]
                clear_dotlocal = self.user_options.get('clear_dotlocal')
        else:
            self.log.warning("No user options found.  That seems wrong.")
        if clear_dotlocal:
            pod_env['CLEAR_DOTLOCAL'] = "TRUE"
            em.set_env("CLEAR_DOTLOCAL", "TRUE")
        # Set up Lab pod resource constraints (not namespace quotas)
        mem_limit = em.get_env_key('LAB_MEM_LIMIT')
        cpu_limit = em.get_env_key('LAB_CPU_LIMIT')
        if image_size:
            mem_limit = str(int(image_size["mem"])) + "M"
            cpu_limit = image_size["cpu"]
        cpu_limit = float(cpu_limit)
        self.mem_limit = mem_limit
        self.cpu_limit = cpu_limit
        mem_guar = em.get_env_key('LAB_MEM_GUARANTEE')
        if mem_guar is None:
            mem_guar = '64K'
        cpu_guar = em.get_env_key('LAB_CPU_GUARANTEE')
        if cpu_guar is None:
            cpu_guar = 0.02
        mem_guar = str(mem_guar)
        cpu_guar = float(cpu_guar)
        # Tiny gets the "basically nothing" above (or the explicit
        #  guarantee).  All others get 1/LAB_SIZE_RANGE times their
        #  maximum, with a default of 1/4.
        size_range = em.get_env_key('LAB_SIZE_RANGE')
        if size_range is None:
            size_range = 4.0
        size_range = float(size_range)
        if image_size and size != 'tiny':
            mem_guar = int(image_size["mem"] / size_range)
            cpu_guar = float(image_size["cpu"] / size_range)
        self.mem_guarantee = mem_guar
        self.cpu_guarantee = cpu_guar
        # Figure out the image and set the pod name from it.
        self.log.debug("Image: {}".format(image))
        self.image = image
        # Parse the image name + tag
        i_l = image.split("/")
        if len(i_l) == 1:
            repo_tag = i_l[0]
        else:
            repo_tag = i_l[1]
        repo = repo_tag.split(":")[0]
        rt_tag = tag.replace('_', '-')
        abbr_pn = repo
        if repo == 'sciplat-lab':
            # Saving characters because tags can be long
            abbr_pn = "nb"
        pn_template = abbr_pn + "-{username}-" + rt_tag
        pod_name = self._expand_user_properties(pn_template)
        self.pod_name = pod_name
        self.log.info("Replacing pod name from options form: %s" %
                      pod_name)
        # Get volume definitions from volume manager.
        self.volumes = self.lsst_mgr.volume_mgr.volumes
        self.volume_mounts = self.lsst_mgr.volume_mgr.volume_mounts
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
            run_as_uid=uid,
            run_as_gid=gid,
            fs_gid=fs_gid,
            supplemental_gids=supplemental_gids,
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
