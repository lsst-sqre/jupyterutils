'''Class to hold LSST-specific, but not user-specific, environment.
'''
import json
import os

from ..utils import make_logger, str_bool


class LSSTEnvironmentManager(object):
    _stashed_kwargs = None
    quota_mgr = None
    volume_mgr = None

    def __init__(self, *args, **kwargs):
        self._stashed_kwargs = kwargs
        self.debug = kwargs.pop('debug', str_bool(os.getenv('DEBUG')) or False)
        self.log = make_logger(name=__name__, debug=self.debug)
        self.log.debug("Creating LSSTEnvironmentManager")
        self.parent = kwargs.pop('parent', None)
        self._mock = kwargs.pop('_mock', False)
        quota_mgr = None
        if self.parent and hasattr(self.parent, "quota_mgr"):
            quota_mgr = self.parent.quota_mgr
        self.quota_mgr = quota_mgr
        volume_mgr = None
        if self.parent and hasattr(self.parent, "volume_mgr"):
            volume_mgr = self.parent.volume_mgr
        self.volume_mgr = volume_mgr
        self.create_pod_env()

    def create_pod_env(self):
        '''Return a dict mapping string to string for injection into the
        pod environment.
        '''
        kwargs = self._stashed_kwargs
        env = {}
        if self.debug:
            env['DEBUG'] = 'TRUE'
        env['MEM_LIMIT'] = kwargs.pop(
            'mem_limit', os.getenv('LAB_MEM_LIMIT') or '2048M')
        env['CPU_LIMIT'] = str(float(kwargs.pop(
            'cpu_limit', os.getenv('LAB_CPU_LIMIT') or '1.0')))
        env['MEM_GUARANTEE'] = str(kwargs.pop(
            'mem_guarantee', os.getenv('LAB_MEM_GUARANTEE') or '64K'))
        env['CPU_GUARANTEE'] = str(float(kwargs.pop(
            'cpu_guarantee', os.getenv('LAB_CPU_GUARANTEE') or '0.02')))
        env['LAB_SIZE_RANGE'] = str(kwargs.pop(
            'lab_size_range', os.getenv('LAB_SIZE_RANGE') or '4.0'))
        env['CULL_TIMEOUT'] = kwargs.pop(
            'cull_timeout', os.getenv('LAB_CULL_TIMEOUT'))
        if env['CULL_TIMEOUT'] is None or '':
            env['CULL_TIMEOUT'] = '43200'
        env['CULL_TIMEOUT'] = str(int(env['CULL_TIMEOUT']))
        env['CULL_POLICY'] = kwargs.pop(
            'cull_policy', os.getenv('LAB_CULL_POLICY') or 'idle:remote')
        env['RESTRICT_DASK_NODES'] = kwargs.pop(
            'restrict_dask_nodes', os.getenv('RESTRICT_DASK_NODES'))
        env['LAB_NODEJS_MAX_MEM'] = kwargs.pop(
            'lab_nodejs_max_mem', os.getenv('LAB_NODEJS_MAX_MEM'))
        env['NODE_OPTIONS'] = ''
        if env['LAB_NODEJS_MAX_MEM']:
            env['NODE_OPTIONS'] = (
                "--max-old-space-size={}".format(env['LAB_NODEJS_MAX_MEM']))
        env['EXTERNAL_HUB_URL'] = kwargs.pop(
            'external_hub_url', os.getenv('EXTERNAL_HUB_URL'))
        env['HUB_ROUTE'] = kwargs.pop(
            'hub_route', os.getenv('HUB_ROUTE') or '')
        while (env['HUB_ROUTE'].endswith('/') and env['HUB_ROUTE'] != '/'):
            env['HUB_ROUTE'] = env['HUB_ROUTE'][:-1]
        if env['EXTERNAL_HUB_URL']:
            oauth_callback = os.getenv('OAUTH_CALLBACK_URL')
            endstr = '/hub/oauth_callback'
            if oauth_callback and oauth_callback.endswith(endstr):
                env['EXTERNAL_HUB_URL'] = oauth_callback[:-len(endstr)]
        env['EXTERNAL_URL'] = env['EXTERNAL_HUB_URL']
        env['EXTERNAL_INSTANCE_URL'] = kwargs.pop(
            'external_instance_url', os.getenv('EXTERNAL_INSTANCE_URL'))
        if not env['EXTERNAL_INSTANCE_URL'] and env['HUB_ROUTE']:
            ehu = env['EXTERNAL_HUB_URL']
            if ehu:
                if ehu.endswith(env['HUB_ROUTE']):
                    env['EXTERNAL_INSTANCE_URL'] = ehu[:-len(env['HUB_ROUTE'])]
        env['FIREFLY_ROUTE'] = kwargs.pop(
            'firefly_route', os.getenv('FIREFLY_ROUTE') or '/firefly')
        env['JS9_ROUTE'] = kwargs.pop(
            'js9_route', os.getenv('JS9_ROUTE') or '/js9')
        env['API_ROUTE'] = kwargs.pop(
            'api_route', os.getenv('API_ROUTE') or '/api')
        env['TAP_ROUTE'] = kwargs.pop(
            'tap_route', os.getenv('TAP_ROUTE') or '/api/tap')
        env['SODA_ROUTE'] = kwargs.pop(
            'soda_route', os.getenv('SODA_ROUTE') or '/api/image/soda')
        env['WORKFLOW_ROUTE'] = kwargs.pop(
            'workflow_route', os.getenv('WORKFLOW_ROUTE') or '/workflow')
        env['EXTERNAL_FIREFLY_ROUTE'] = kwargs.pop(
            'external_firefly_route', os.getenv('EXTERNAL_FIREFLY_ROUTE'))
        env['EXTERNAL_JS9_ROUTE'] = kwargs.pop(
            'external_js9_route', os.getenv('EXTERNAL_JS9_ROUTE'))
        env['EXTERNAL_API_ROUTE'] = kwargs.pop(
            'external_api_route', os.getenv('EXTERNAL)API_ROUTE'))
        env['EXTERNAL_TAP_ROUTE'] = kwargs.pop(
            'external_tap_route', os.getenv('EXTERNAL_TAP_ROUTE'))
        env['EXTERNAL_SODA_ROUTE'] = kwargs.pop(
            'external_soda_route', os.getenv('EXTERNAL_SODA_ROUTE'))
        env['EXTERNAL_WORKFLOW_ROUTE'] = kwargs.pop(
            'external_workflow_route', os.getenv('EXTERNAL_WORKFLOW_ROUTE'))
        env['CLEAR_DOTLOCAL'] = kwargs.pop('clear_dotlocal', '')
        env['AUTO_REPO_URLS'] = kwargs.pop(
            'auto_repo_urls', os.getenv('AUTO_REPO_URLS'))
        if self.volume_mgr:
            env['DASK_VOLUME_B64'] = self.volume_mgr.get_dask_volume_b64()
        else:
            self.log.warning("No volume manager; can't get dask volume text!")
        if self.quota_mgr:
            if self.quota_mgr and self.quota_mgr.quota:
                if "limits.cpu" in self.quota_mgr.quota:
                    cpulimit = self.quota_mgr.quota["limits.cpu"]
                    env['NAMESPACE_CPU_LIMIT'] = cpulimit
                if "limits.memory" in self.quota_mgr.quota:
                    nmlimit = self.quota_mgr.quota["limits.memory"]
                    if nmlimit[-2:] == "Mi":
                        nmlimit = nmlimit[:-2] + "M"
                    env['NAMESPACE_MEM_LIMIT'] = nmlimit
        # Now clean up the env hash by removing any keys with empty values
        retval = {k: v for k, v in env.items() if v}
        self.log.debug("create_env yielded:\n.{}".format(retval))
        self.pod_env = retval

    def get_env(self):
        '''Return the whole stored environment to caller as a dict.
        '''
        return self.pod_env

    def get_env_key(self, key):
        '''Return value of a specific key in the stored environment to caller.
        '''
        return self.pod_env.get(key)

    def set_env(self, key, value):
        '''Set a particular key in the stored environment to the given value.
        If the value is 'None', delete the key, if it exists.

        '''
        if value is None:
            if key in self.pod_env:
                del(self.pod_env[key])
        else:
            self.pod_env[key] = value

    def update_env(self, update_dict):
        '''Update the stored environment with the supplied dict.
        '''
        self.pod_env.update(update_dict)

    def get_env_json(self):
        '''Return the stored environment as a JSON document, under the single
        key 'environment'.
        '''
        return json.dumps({"environment": self.pod_env})
