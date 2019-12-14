'''LSST JupyterHub utilities and helpers.

These implement the LSST-specific tooling for the LSST Science
Platform Notebook Aspect.  The repo scanner looks for Docker images in
a repository with a particular tag format; the prepuller pulls a
subset of those images to each node.  The reaper removes images past a
certain age, based on the tag format.  The LSST Manager class provides
a hierarchy of objects that hold LSST-specific configuration and logic
for spawning JupyterLab pods, and the spawner and authenticators
provide the pod spawner and the LSST-supported authentication methods
and logic.  Convenience functions are in 'utils' and JupyterHub
configuration convenience functions are in 'config_helpers'.
'''
from .prepuller import Prepuller
from .reaper import Reaper
from .scanrepo import ScanRepo
from .scanrepo import SingletonScanner
from .singleton import Singleton
from .lsstmgr import LSSTMiddleManager, check_membership
from .spawner import LSSTSpawner
from .authenticator.lsstcilogonauth import LSSTCILogonOAuthenticator
from .authenticator.lsstgithubauth import LSSTGitHubOAuthenticator
from .authenticator.lsstjwtauth import LSSTJWTAuthenticator
from .utils import (rreplace, sanitize_dict, get_execution_namespace,
                    make_logger, str_bool, str_true, listify, list_duplicates)
from .config import LSSTConfig
from .setup_auth import configure_auth_and_spawner
from ._version import __version__

__all__ = [LSSTMiddleManager, check_membership, Prepuller, Reaper,
           ScanRepo, Singleton, SingletonScanner, LSSTSpawner,
           LSSTCILogonOAuthenticator, LSSTGitHubOAuthenticator,
           LSSTJWTAuthenticator, rreplace, sanitize_dict,
           get_execution_namespace, make_logger, str_bool, str_true,
           listify, list_duplicates, LSSTConfig,
           configure_auth_and_spawner, __version__]
