import json
import logging
import os
import sys
from eliot import to_file, start_action
from eliot.stdlib import EliotHandler
from jupyter_client.localinterfaces import public_ips
from .. import Singleton
from ..utils import (str_bool, listify, intify, floatify, make_logger,
                     get_execution_namespace, sanitize_dict)


class LSSTConfig(metaclass=Singleton):
    '''LSSTConfig is a Singleton bag of attributes to hold
    important-to-the-LSST JupyterHub environment variables and default
    values.
    '''

    def __init__(self, *args, **kwargs):
        default_level = logging.WARNING
        # we can't check this prior to the config existing...
        debug = str_bool(os.getenv('DEBUG', False))
        if debug:
            default_level = logging.DEBUG
        self._setup_logger(level=default_level)
        self.source = kwargs.pop('source', 'environment')
        if self.source != 'environment':
            raise ValueError(
                "'environment' is currently only allowed config source!")
        # To make it easy to read settings from a document later on...
        if self.source == 'environment':
            self.load_from_environment()
        if self.debug:
            # Reset default loglevel
            rootlogger = logging.getLogger()
            level = rootlogger.getEffectiveLevel()
            if level > logging.DEBUG:
                rootlogger.warn("Setting root logging level to DEBUG.")
                rootlogger.setLevel(logging.DEBUG)
        self.create_derived_settings()

    def _setup_logger(self, level=logging.WARNING):
        # In Python 3.8 we could use force=True to force the root logger
        #  to override existing defaults
        # Add our formats
        with start_action(action_type="_setup_logger"):
            fstr = '[%(levelname).1s %(asctime)s.%(msecs).03d'
            fstr += ' %(module)s:%(funcName)s:%(lineno)d] %(message)s'
            dstr = '%Y-%m-%d %H:%M:%S'
            self.log_format = fstr
            self.log_datefmt = dstr
            self.log_level = level
            rootlogger = logging.getLogger()
            rootlogger.handlers = [EliotHandler()]
            to_file(sys.stderr)
            loggers = []
            lrmld = logging.root.manager.loggerDict
            loggers = loggers + [logging.getLogger(name) for name in lrmld]
            rootlogger.debug("Removing all existing log handlers.")
            for lgr in loggers:
                rootlogger.debug(
                    "Removing log handlers for '{}'".format(lgr.name))
                lgr.handlers = []
            rootlogger.debug("Setting default log level to '{}'".format(level))
            rootlogger.setLevel(level)
            # There are some things we just don't want to log at DEBUG
            chattycathies = {'kubernetes': logging.WARNING,
                             'urllib3': logging.WARNING,
                             'JupyterHub': logging.INFO}
            for k in chattycathies:
                v = chattycathies[k]
                lgr = logging.getLogger(k)
                el = lgr.getEffectiveLevel()
                if el < v:
                    rootlogger.debug(
                        "Logger {} @level {} -> {}.".format(k, el, v))
                    lgr.setLevel(v)
            self.log = make_logger(level=level)

    def load_from_environment(self):
        '''Populate attributes from environment variables.
        '''
        with start_action(action_type="load_from_environment"):
            self.debug = str_bool(os.getenv('DEBUG'))
            # Authentication parameters
            self.jwt_logout_url = os.getenv("LOGOUT_URL") or '/logout'
            self.allowed_groups = listify(
                os.getenv('ALLOWED_GROUPS'))
            self.forbidden_groups = listify(
                os.getenv('FORBIDDEN_GROUPS'))
            self.strict_ldap_groups = str_bool(os.getenv('STRICT_LDAP_GROUPS'))
            # Settings for Options Form
            self.form_selector_title = (os.getenv('LAB_SELECTOR_TITLE') or
                                        'Container Image Selector')
            self.form_template = (
                os.getenv('OPTIONS_FORM_TEMPLATE') or
                ('/opt/lsst/software/jupyterhub/templates/' +
                 'options_form.template.html'))
            self.form_sizelist = (listify(
                os.getenv('OPTIONS_FORM_SIZELIST')) or
                ['tiny', 'small', 'medium', 'large'])
            self.max_scan_delay = intify(os.getenv('MAX_SCAN_DELAY'), 300)
            self.initial_scan_interval = floatify(
                os.getenv('INITIAL_SCAN_INTERVAL'), 0.2)
            self.max_scan_interval = floatify(
                os.getenv('MAX_SCAN_INTERVAL'), 5.0)
            self.tiny_cpu_max = floatify(os.getenv('TINY_CPU_MAX'), 0.5)
            self.mb_per_cpu = intify(os.getenv('MB_PER_CPU'), 2048)
            self.size_index = intify(os.getenv('SIZE_INDEX'), 1)
            # Settings for Quota Manager
            self.resource_map = (os.getenv('RESOURCE_MAP') or
                                 '/opt/lsst/software/jupyterhub/resources/' +
                                 'resourcemap.json')
            self.max_dask_workers = int(os.getenv('MAX_DASK_WORKERS', 25))
            # Settings for Volume Manager
            self.volume_definition_file = (
                os.getenv('VOLUME_DEFINITION_FILE') or
                ('/opt/lsst/software/jupyterhub/' +
                 'mounts/mountpoints.json'))
            # Hub settings for Lab spawning
            self.lab_uid = intify(os.getenv('LAB_UID'), 769)
            self.lab_gid = intify(os.getenv('LAB_GID'), 769)
            self.lab_fs_gid = intify(os.getenv('LAB_FS_GID'), self.lab_gid)
            self.lab_default_image = (os.getenv('LAB_IMAGE') or
                                      "lsstsqre/sciplat-lab:latest")
            self.lab_size_range = os.getenv('LAB_SIZE_RANGE') or '4.0'
            self.cull_timeout = os.getenv('LAB_CULL_TIMEOUT') or '64800'
            self.cull_policy = os.getenv('LAB_CULL_POLICY') or 'idle:remote'
            self.allow_dask_spawn = str_bool(os.getenv('ALLOW_DASK_SPAWN'))
            self.restrict_dask_nodes = os.getenv('RESTRICT_DASK_NODES')
            self.restrict_lab_nodes = os.getenv('RESTRICT_LAB_NODES')
            self.lab_nodejs_max_mem = os.getenv('LAB_NODEJS_MAX_MEM') or '6144'
            self.external_hub_url = os.getenv('EXTERNAL_HUB_URL')
            self.hub_route = os.getenv('HUB_ROUTE') or '/'
            self.external_instance_url = os.getenv('EXTERNAL_INSTANCE_URL')
            self.firefly_route = os.getenv('FIREFLY_ROUTE') or '/firefly'
            self.js9_route = os.getenv('JS9_ROUTE') or '/js9'
            self.api_route = os.getenv('API_ROUTE') or '/api'
            self.tap_route = os.getenv('TAP_ROUTE') or '/api/tap'
            self.soda_route = os.getenv('SODA_ROUTE') or '/api/image/soda'
            self.workflow_route = os.getenv('WORKFLOW_ROUTE') or '/wf'
            self.external_firefly_url = os.getenv('EXTERNAL_FIREFLY_URL')
            self.external_js9_url = os.getenv('EXTERNAL_JS9_URL')
            self.external_api_url = os.getenv('EXTERNAL_API_URL')
            self.external_tap_url = os.getenv('EXTERNAL_TAP_URL')
            self.external_soda_url = os.getenv('EXTERNAL_SODA_URL')
            self.external_workflow_url = os.getenv('EXTERNAL_WORKFLOW_URL')
            self.auto_repo_urls = os.getenv('AUTO_REPO_URLS')
            # Prepuller settings
            self.lab_repo_owner = os.getenv('LAB_REPO_OWNER') or 'lsstsqre'
            self.lab_repo_name = os.getenv('LAB_REPO_NAME') or 'sciplat-lab'
            self.lab_repo_host = os.getenv('LAB_REPO_HOST') or 'hub.docker.com'
            self.prepuller_namespace = (os.getenv('PREPULLER_NAMESPACE') or
                                        get_execution_namespace())
            self.prepuller_experimentals = intify(
                os.getenv('PREPULLER_EXPERIMENTALS'), 0)
            self.prepuller_dailies = intify(os.getenv('PREPULLER_DAILIES'), 3)
            self.prepuller_weeklies = intify(
                os.getenv('PREPULLER_WEEKLIES'), 2)
            self.prepuller_releases = intify(
                os.getenv('PREPULLER_RELEASES'), 1)
            self.prepuller_cachefile = os.getenv('PREPULLER_CACHEFILE',
                                                 '/tmp/repo-cache.json')
            self.prepuller_timeout = os.getenv('PREPULLER_TIMEOUT', 3300)
            cstr = "echo \"Prepuller for $(hostname) completed at $(date).\""
            self.prepuller_command = ["/bin/sh", "-c", cstr]
            prp_cmd = os.getenv('PREPULLER_COMMAND_JSON')
            if prp_cmd:
                self.prepuller_command = json.loads(prp_cmd)
            # Reaper settings
            self.reaper_keep_experimentals = intify(
                os.getenv('REAPER_KEEP_EXPERIMENTALS'), 10)
            self.reaper_keep_dailies = intify(
                os.getenv('REAPER_KEEP_DAILIES'), 15)
            self.reaper_keep_weeklies = intify(
                os.getenv('REAPER_KEEP_WEEKLIES'), 78)
            # Fileserver settings
            self.fileserver_host = (os.getenv('EXTERNAL_FILESERVER_IP') or
                                    os.getenv('FILESERVER_SERVICE_HOST'))
            # Reaper settings
            self.reaper_user = os.getenv('IMAGE_REAPER_USER')
            self.reaper_password = os.getenv('IMAGE_REAPER_PASSWORD')
            # Hub internal settings
            my_ip = public_ips()[0]
            self.helm_tag = os.getenv('HELM_TAG')
            self.hub_host = os.getenv('HUB_SERVICE_HOST') or my_ip
            self.hub_api_port = os.getenv('HUB_SERVICE_PORT_API') or 8081
            self.proxy_host = os.getenv('PROXY_SERVICE_HOST') or my_ip
            self.proxy_api_port = os.getenv('PROXY_SERVICE_PORT_API') or 8001
            self.session_db_url = os.getenv('SESSION_DB_URL')
            # Pod multicast (used with SAL)
            self.enable_multus = str_bool(os.getenv('ENABLE_MULTUS'))
            # DDS domain (used with SAL)
            self.lab_dds_domain = os.getenv('LSST_DDS_DOMAIN')
            # These have to be set post-initialization to avoid a circular
            #  dependency.
            self.authenticator_class = None
            self.spawner_class = None

    def create_derived_settings(self):
        '''Create further settings from passed-in ones.
        '''
        with start_action(action_type="create_derived_settings"):
            self.proxy_api_url = 'http://{}:{}'.format(
                self.proxy_host, self.proxy_api_port)
            self.bind_url = 'http://0.0.0.0:8000{}'.format(self.hub_route)
            self.hub_bind_url = 'http://0.0.0.0:8081{}'.format(self.hub_route)
            self.hub_connect_url = 'http://{}:{}{}'.format(self.hub_host,
                                                           self.hub_api_port,
                                                           self.hub_route)
            mm = self.lab_nodejs_max_mem
            self.lab_node_options = None
            if mm:
                self.lab_node_options = "--max-old-space-size={}".format(mm)
            while self.hub_route.endswith('/') and self.hub_route != '/':
                self.hub_route = self.hub_route[:-1]
            if not self.external_instance_url:
                ehu = self.external_hub_url
                if ehu:
                    if ehu.endswith(self.hub_route):
                        self.external_instance_url = ehu[:-len(self.hub_route)]

    def dump(self):
        '''Return dict for pretty printing.
        '''
        myvars = vars(self)
        sanitized = sanitize_dict(myvars, ['reaper_password',
                                           'session_db_url'])
        # Stringify classrefs
        for key in ["log", "authenticator_class", "spawner_class"]:
            val = sanitized.get(key)
            if val:
                sanitized[key] = str(val)
        return sanitized

    def toJSON(self):
        return self.dump()
