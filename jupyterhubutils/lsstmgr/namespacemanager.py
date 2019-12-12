'''Class to provide namespace manipulation.
'''

import os

from kubernetes.client.rest import ApiException
from kubernetes import client, config

from ..utils import (get_execution_namespace,
                     get_dummy_user, make_logger, str_bool)


class LSSTNamespaceManager(object):
    '''Class to provide namespace manipulation.
    '''
    user = None
    namespace = None
    rbac_api = None
    #
    quota_mgr = None
    # These properties are set by the Spawner
    delete_namespace_on_stop = False
    enable_namespace_quotas = False

    def __init__(self, *args, **kwargs):
        self.log = make_logger()
        self.parent = kwargs.pop('parent')
        self.user = self.parent.user
        self.api = self.parent.api
        self.rbac_api = self.parent.rbac_api
        self.update_namespace_name()
        # Add an attribute for service account
        svc_acct = kwargs.pop('service_account', None)
        if not svc_acct:
            if self.parent.config.allow_dask_spawn:
                svc_acct = "dask"
        self.service_account = svc_acct
        spawner = self.parent.spawner
        self.delete_namespace_on_stop = spawner.delete_namespace_on_stop
        self.enable_namespace_quotas = spawner.enable_namespace_quotas

    def update_namespace_name(self):
        '''Build namespace name from user and execution namespace.
        '''
        execution_namespace = get_execution_namespace()
        self.log.debug("Execution namespace: '{}'".format(execution_namespace))
        user = self.user
        self.log.debug("User: '{}'".format(user))
        username = None
        if user:
            try:
                um = user.escaped_name
                if callable(um):
                    username = um()
                else:
                    username = um
            except AttributeError:
                self.log.debug("User has no escaped_name() method.")
        if execution_namespace and username:
            self.namespace = "{}-{}".format(execution_namespace,
                                            username)
        else:
            df_msg = "Using 'default' namespace."
            self.log.warning(df_msg)
            self.namespace = "default"

    def ensure_namespace(self):
        '''Here we make sure that the namespace exists, creating it if
        it does not.  That requires a ClusterRole that can list and create
        namespaces.

        If we have shadow PVs, we clone the (static) NFS PVs and then
        attach namespaced PVCs to them.  Thus the role needs to be
        able to list and create PVs and PVCs.

        If we create the namespace, we also create (if needed) a ServiceAccount
        within it to allow the user pod to spawn dask and workflow pods.

        '''
        self.update_namespace_name()
        namespace = self.namespace
        if namespace == "default":
            self.log.warning("Namespace is 'default'; no manipulation.")
            return
        ns = client.V1Namespace(
            metadata=client.V1ObjectMeta(name=namespace))
        try:
            self.log.info("Attempting to create namespace '%s'" % namespace)
            self.api.create_namespace(ns)
        except ApiException as e:
            if e.status != 409:
                estr = "Create namespace '%s' failed: %s" % (ns, str(e))
                self.log.exception(estr)
                raise
            else:
                self.log.info("Namespace '%s' already exists." % namespace)
        if self.service_account:
            self.log.debug("Ensuring namespaced service account.")
            self._ensure_namespaced_service_account()
        else:
            self.log.debug("No namespaced service account required.")
        if self.enable_namespace_quotas:
            self.log.debug("Determining resource quota.")
            qm = self.quota_mgr
            quota = qm.get_resource_quota_spec()
            if quota:
                self.log.debug("Ensuring namespace quota.")
                qm.ensure_namespaced_resource_quota(quota)
            else:
                self.log.debug("No namespace quota required.")
        self.log.debug("Namespace resources ensured.")

    def _define_namespaced_account_objects(self):
        # We may want these when and if we move Argo workflows into the
        #  deployment.
        #
        #    client.V1PolicyRule(
        #        api_groups=["argoproj.io"],
        #        resources=["workflows", "workflows/finalizers"],
        #        verbs=["get", "list", "watch", "update", "patch", "delete"]
        #    ),
        #    client.V1PolicyRule(
        #        api_groups=["argoproj.io"],
        #        resources=["workflowtemplates",
        #                   "workflowtemplates/finalizers"],
        #        verbs=["get", "list", "watch"],
        #    ),
        #
        #    client.V1PolicyRule(
        #        api_groups=[""],
        #        resources=["secrets"],
        #        verbs=["get"]
        #    ),
        #    client.V1PolicyRule(
        #        api_groups=[""],
        #        resources=["configmaps"],
        #        verbs=["list"]
        #    ),
        namespace = self.namespace
        account = self.service_account
        if not account:
            self.log.info("No service account defined.")
            return (None, None, None)
        md = client.V1ObjectMeta(name=account)
        svcacct = client.V1ServiceAccount(metadata=md)
        rules = [
            client.V1PolicyRule(
                api_groups=[""],
                resources=["pods", "services"],
                verbs=["get", "list", "watch", "create", "delete"]
            ),
            client.V1PolicyRule(
                api_groups=[""],
                resources=["pods/log", "serviceaccounts"],
                verbs=["get", "list"]
            ),
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

    def _ensure_namespaced_service_account(self):
        # Create a service account with role and rolebinding to allow it
        #  to manipulate pods in the namespace.
        self.log.info("Ensuring namespaced service account.")
        namespace = self.namespace
        account = self.service_account
        svcacct, role, rolebinding = self._define_namespaced_account_objects()
        if not svcacct:
            self.log.info("Service account not defined.")
            return
        try:
            self.log.info("Attempting to create service account.")
            self.api.create_namespaced_service_account(
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
            self.rbac_api.create_namespaced_role(
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
            self.rbac_api.create_namespaced_role_binding(
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

    def maybe_delete_namespace(self):
        '''Here we try to delete the namespace.  If it has no non-dask
        running pods, and it's not the default namespace, we can delete it."

        This requires a cluster role that can delete namespaces.'''
        self.log.debug("Attempting to delete namespace.")
        namespace = self.namespace
        if namespace == "default":
            self.log.warning("Cannot delete 'default' namespace")
            return
        podlist = self.api.list_namespaced_pod(namespace)
        clear_to_delete = True
        if podlist and podlist.items and len(podlist.items) > 0:
            clear_to_delete = self._check_pods(podlist.items)
        if not clear_to_delete:
            self.log.info("Not deleting namespace '%s'" % namespace)
            return False
        self.log.info("Clear to delete namespace '%s'" % namespace)
        self.log.info("Deleting namespace '%s'" % namespace)
        self.api.delete_namespace(namespace)
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
                        # We can murder abandoned dask pods
                        continue
                    self.log.info("Pod in state '%s'; " % phase +
                                  "cannot delete namespace '%s'." % namespace)
                    return False
        return True

    def destroy_namespaced_resource_quota(self):
        '''Remove quotas from namespace.
        You don't usually have to call this, since it will get
        cleaned up as part of namespace deletion.
        '''
        namespace = self.get_user_namespace()
        qname = "quota-" + namespace
        dopts = client.V1DeleteOptions()
        self.log.info("Deleting resourcequota '%s'" % qname)
        self.api.delete_namespaced_resource_quota(qname, namespace, dopts)

    def delete_namespaced_service_account_objects(self):
        '''Remove service accounts, roles, and rolebindings from namespace.
        You don't usually have to call this, since they will get
         cleaned up as part of namespace deletion.
        '''
        namespace = self.get_user_namespace()
        account = self.service_account
        if not account:
            self.log.info("Service account not defined.")
            return
        dopts = client.V1DeleteOptions()
        self.log.info("Deleting service accounts/role/rolebinding " +
                      "for %s" % namespace)
        self.rbac_api.delete_namespaced_role_binding(
            account,
            namespace,
            dopts)
        self.rbac_api.delete_namespaced_role(
            account,
            namespace,
            dopts)
        self.api.delete_namespaced_service_account(
            account,
            namespace,
            dopts)
