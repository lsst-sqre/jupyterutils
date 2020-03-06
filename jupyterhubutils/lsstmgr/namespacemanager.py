'''Class to provide namespace manipulation.
'''

import time
from kubernetes.client.rest import ApiException
from kubernetes import client
from .. import LoggableChild


class LSSTNamespaceManager(LoggableChild):
    '''Class to provide namespace manipulation.
    '''
    parent = None
    log = None
    namespace = None
    service_account = None  # Account for pod to run as

    def set_namespace(self, namespace):
        self.namespace = namespace

    def ensure_namespace(self):
        '''Here we make sure that the namespace exists, creating it if
        it does not.  That requires a ClusterRole that can list and create
        namespaces.

        If we create the namespace, we also create (if needed) a ServiceAccount
        within it to allow the user pod to spawn dask and workflow pods.

        '''
        namespace = self.namespace
        if not namespace or namespace == "default":
            raise ValueError("Will not use default namespace!")
        api = self.parent.api
        cfg = self.parent.config
        ns = client.V1Namespace(
            metadata=client.V1ObjectMeta(
                name=namespace,
                labels={'argocd.argoproj.io/instance': 'nublado-users'},
                annotations={
                    'argocd.argoproj.io/compare-options': 'IgnoreExtraneous',
                    'argocd.argoproj.io/sync-options': 'Prune=false',
                }))
        try:
            self.log.info("Attempting to create namespace '%s'" % namespace)
            api.create_namespace(ns)
        except ApiException as e:
            if e.status != 409:
                estr = "Create namespace '%s' failed: %s" % (ns, str(e))
                self.log.exception(estr)
                raise
            else:
                self.log.info("Namespace '%s' already exists." % namespace)
        # Wait for the namespace to actually appear before creating objects
        #  in it.
        self.wait_for_namespace()
        if cfg.allow_dask_spawn:
            self.log.debug("Ensuring namespaced service account.")
            self.ensure_namespaced_service_account()
        if self.parent.spawner.enable_namespace_quotas:
            # By the time we need this, quota will have been set, because
            #  we needed it for options form generation.
            self.log.debug("Determining resource quota.")
            qm = self.parent.quota_mgr
            qm.ensure_namespaced_resource_quota(qm.quota)
        self.log.debug("Namespace resources ensured.")

    def define_namespaced_account_objects(self):
        namespace = self.namespace
        username = self.parent.user.escaped_name
        account = "{}-svcacct".format(username)
        self.service_account = account
        md = client.V1ObjectMeta(
            name=account,
            labels={'argocd.argoproj.io/instance': 'nublado-users'},
            annotations={
                'argocd.argoproj.io/compare-options': 'IgnoreExtraneous',
                'argocd.argoproj.io/sync-options': 'Prune=false',
            }
        )
        svcacct = client.V1ServiceAccount(metadata=md)
        # These rules let us manipulate Dask pods, Argo Workflows, and
        #  Multus CNI interfaces
        rules = [
            client.V1PolicyRule(
                api_groups=["argoproj.io"],
                resources=["workflows", "workflows/finalizers"],
                verbs=["get", "list", "watch", "update", "patch", "create",
                       "delete"]
            ),
            client.V1PolicyRule(
                api_groups=["argoproj.io"],
                resources=["workflowtemplates",
                           "workflowtemplates/finalizers"],
                verbs=["get", "list", "watch"],
            ),
            client.V1PolicyRule(
                api_groups=[""],
                resources=["secrets"],
                verbs=["get"]
            ),
            client.V1PolicyRule(
                api_groups=[""],
                resources=["pods", "pods/exec", "services", "configmaps"],
                verbs=["get", "list", "watch", "create", "delete",
                       "update", "patch"]
            ),
            client.V1PolicyRule(
                api_groups=[""],
                resources=["pods/log", "serviceaccounts"],
                verbs=["get", "list", "watch"]
            ),
            #            client.V1PolicyRule(
            #                api_groups=[""],
            #                resources=["pods/status"],
            #                verbs=["get", "update"]
            #            ),
            #            client.V1PolicyRule(
            #                api_groups=["k8s.cni.cncf.io"],
            #                resources=['*'],
            #                verbs=['*']
            #            )
        ]
        role = client.V1Role(
            rules=rules,
            metadata=md)
        rolebinding = client.V1RoleBinding(
            metadata=md,
            role_ref=client.V1RoleRef(api_group="rbac.authorization.k8s.io",
                                      kind="Role",
                                      name=account),
            subjects=[client.V1Subject(
                kind="ServiceAccount",
                name=account,
                namespace=namespace)]
        )
        return svcacct, role, rolebinding

    def ensure_namespaced_service_account(self):
        # Create a service account with role and rolebinding to allow it
        #  to manipulate resources in the namespace.
        self.log.info("Ensuring namespaced service account.")
        namespace = self.namespace
        api = self.parent.api
        rbac_api = self.parent.rbac_api
        svcacct, role, rolebinding = self.define_namespaced_account_objects()
        account = self.service_account
        try:
            self.log.info("Attempting to create service account.")
            api.create_namespaced_service_account(
                namespace=namespace,
                body=svcacct)
        except ApiException as e:
            if e.status != 409:
                self.log.exception("Create service account '%s' " % account +
                                   "in namespace '%s' " % namespace +
                                   "failed: %s" % str(e))
                raise
            else:
                self.log.info("Service account '%s' " % account +
                              "in namespace '%s' already exists." % namespace)
        try:
            self.log.info("Attempting to create role in namespace.")
            rbac_api.create_namespaced_role(
                namespace,
                role)
        except ApiException as e:
            if e.status != 409:
                self.log.exception("Create role '%s' " % account +
                                   "in namespace '%s' " % namespace +
                                   "failed: %s" % str(e))
                raise
            else:
                self.log.info("Role '%s' " % account +
                              "already exists in namespace '%s'." % namespace)
        try:
            self.log.info("Attempting to create rolebinding in namespace.")
            rbac_api.create_namespaced_role_binding(
                namespace,
                rolebinding)
        except ApiException as e:
            if e.status != 409:
                self.log.exception("Create rolebinding '%s'" % account +
                                   "in namespace '%s' " % namespace +
                                   "failed: %s", str(e))
                raise
            else:
                self.log.info("Rolebinding '%s' " % account +
                              "already exists in '%s'." % namespace)

    def wait_for_namespace(self, timeout=30):
        '''Wait for namespace to be created.'''
        namespace = self.namespace
        for dl in range(timeout):
            self.log.debug("Checking for namespace " +
                           "{} [{}/{}]".format(namespace, dl, timeout))
            nl = self.parent.api.list_namespace(timeout_seconds=1)
            for ns in nl.items:
                nsname = ns.metadata.name
                if nsname == namespace:
                    self.log.debug("Namespace {} found.".format(namespace))
                    return
                self.log.debug(
                    "Namespace {} not present yet.".format(namespace))
            time.sleep(1)
        raise RuntimeError(
            "Namespace '{}' was not created in {} seconds!".format(namespace,
                                                                   timeout))

    def maybe_delete_namespace(self):
        '''Here we try to delete the namespace.  If it has no non-dask
        running pods, and it's not the default namespace, we can delete it."

        This requires a cluster role that can delete namespaces.'''
        self.log.debug("Attempting to delete namespace.")
        namespace = self.namespace
        if not namespace or namespace == "default":
            raise RuntimeError("Cannot delete default namespace!")
            return
        podlist = self.parent.api.list_namespaced_pod(namespace)
        clear_to_delete = True
        if podlist and podlist.items:
            clear_to_delete = self._check_pods(podlist.items)
        if not clear_to_delete:
            self.log.info("Not deleting namespace '%s'" % namespace)
            return False
        self.log.info("Deleting namespace '%s'" % namespace)
        self.parent.api.delete_namespace(namespace)
        return True

    def _check_pods(self, items):
        namespace = self.namespace
        for i in items:
            if i and i.status:
                phase = i.status.phase
                if (phase == "Running" or phase == "Unknown"
                        or phase == "Pending"):
                    pname = i.metadata.name
                    if pname.startswith("dask-"):
                        self.log.debug(
                            ("Abandoned dask pod '{}' can be " +
                             "reaped.").format(pname))
                        # We can murder abandoned dask pods
                        continue
                    self.log.warning(("Pod in state '{}'; cannot delete " +
                                      "namespace '{}'.").format(phase,
                                                                namespace))
                    return False
        # FIXME check on workflows as well.
        return True

    def destroy_namespaced_resource_quota(self):
        '''Remove quotas from namespace.
        You don't usually have to call this, since it will get
        cleaned up as part of namespace deletion.
        '''
        namespace = self.get_user_namespace()
        api = self.parent.api
        qname = "quota-" + namespace
        dopts = client.V1DeleteOptions()
        self.log.info("Deleting resourcequota '%s'" % qname)
        api.delete_namespaced_resource_quota(qname, namespace, dopts)

    def delete_namespaced_service_account_objects(self):
        '''Remove service accounts, roles, and rolebindings from namespace.
        You don't usually have to call this, since they will get
         cleaned up as part of namespace deletion.
        '''
        namespace = self.namespace
        account = self.service_account
        if not account:
            self.log.info("Service account not defined.")
            return
        dopts = client.V1DeleteOptions()
        self.log.info("Deleting service accounts/role/rolebinding " +
                      "for %s" % namespace)
        self.parent.rbac_api.delete_namespaced_role_binding(
            account,
            namespace,
            dopts)
        self.parent.rbac_api.delete_namespaced_role(
            account,
            namespace,
            dopts)
        self.parent.api.delete_namespaced_service_account(
            account,
            namespace,
            dopts)

    def dump(self):
        '''Return dict for pretty-printing.
        '''
        nd = {"namespace": self.namespace,
              "service_account": self.service_account,
              "parent": str(self.parent)}
        return nd
