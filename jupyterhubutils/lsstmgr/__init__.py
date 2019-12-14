'''The LSSTMiddleManager is the only exported class: all the managers that
actually manage things report to it.  However, we expose some of the functions
from the group manager.
'''
from .middlemanager import LSSTMiddleManager
from .authmanager import check_membership, group_merge, deduplicate_groups

__all__ = [LSSTMiddleManager, check_membership,
           group_merge, deduplicate_groups]
