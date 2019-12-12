'''Class to hold LSST-specific authentication/authorization details
and methods.

Most of this was formerly held in the JupyterHub config as classes defined
in '10-authenticator.py'.
'''
import random

from ..utils import make_logger, list_duplicates


class LSSTAuthManager(object):
    authenticator = None
    group_map = {}  # key is group name, value is group id

    def __init__(self, *args, **kwargs):
        self.log = make_logger()
        self.log.debug("Creating LSSTAuthManager.")
        self.parent = kwargs.pop('parent')
        self.authenticator = self.parent.authenticator

    def _group_merge(self, groups_1, groups_2):
        # First just merge them
        grps = []
        grps.extend(groups_1)
        grps.extend(groups_2)
        # Naive deduplication
        grps = list(set(grps))
        grpnames = [x.split(':', 1)[0] for x in grps]
        gnset = set(grpnames)
        # Check for need to do less-naive dedupe
        if len(gnset) != len(grpnames):
            # We have a collision
            grps = self._deduplicate_groups(grps, grpnames)
        return grps

    def _deduplicate_groups(self, grps):
        grpsplits = [x.split(':', 1) for x in grps]
        grpnames = [x[0] for x in grpsplits]
        grpnums = [x[1] for x in grpsplits]
        flist = list_duplicates(grpnames)
        for name, positions in flist:
            i = 1
            for p in positions[1:]:
                # start with group_2 (leave the first one alone)
                i += 1
                grps[p] = grpnames[p] + "_" + str(i) + ":" + grpnums[p]
        return grps

    def check_membership(self):
        allowed_groups = self.authenticator.allowed_groups.split(",")
        forbidden_groups = self.authenticator.forbidden_groups.split(",")
        user_groups = self.authenticator.groups
        deny = list(set(forbidden_groups) & set(user_groups))
        if deny:
            self.log.warning("User in forbidden group: %s" % str(deny))
            return False
        self.log.debug(
            "User not in forbidden groups: {}".format(forbidden_groups))
        intersection = list(set(allowed_groups) &
                            set(user_groups))
        if intersection:
            self.log.debug("User in allowed groups: {}".format(intersection))
            return True
        self.log.warning("User not in any groups %s" % str(allowed_groups))
        return False

    def get_fake_gid(self):
        '''Use if we have strict_ldap_groups off, to assign GIDs to names
        with no matching Unix GID.  Since these will not appear as filesystem
        groups, being consistent with them isn't important.  We just need
        to make their GIDs something likely to not match anything real.

        There is a chance of collision, but it doesn't really matter.

        We do need to keep the no-GID groups around, though, because we might
        be using them to make options form or quota decisions (if we know we
        don't, we should turn on strict_ldap_groups).
        '''
        grpbase = 3E7
        grprange = 1E7
        igrp = random.randint(grpbase, (grpbase + grprange))
        return igrp

    def get_group_map(self):
        '''Convenience function for group retrieval.
        '''
        return self.group_map

    def get_group_string(self):
        '''Convenience function for retrieving the group name-to-uid mapping
        list as a string suitable for passing to the spawned pod.
        '''
        return ','.join(["{}:{}".format(x, self.group_map[x])
                         for x in list(self.group_map.keys())])
