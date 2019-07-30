import copy
import datetime
import functools
import hashlib
import json
import logging
import re
import requests
import semver
import urllib.parse
import urllib.request


class ScanRepo(object):
    """Class to scan repository and create results.

       Based on:
       https://github.com/shangteus/py-dockerhub/blob/master/dockerhub.py"""

    host = 'hub.docker.com'
    path = ''
    owner = ''
    name = ''
    port = None
    data = {}
    debug = False
    json = False
    insecure = False
    sort_field = "name"
    experimentals = 0
    dailies = 3
    weeklies = 2
    releases = 1
    recommended = True
    _results = None
    _name_to_manifest = {}
    _all_tags = []
    logger = None
    cachefile = None

    def __init__(self, host='', path='', owner='', name='',
                 experimentals=0, dailies=3, weeklies=2, releases=1,
                 recommended=True,
                 json=False, port=None,
                 cachefile=None,
                 insecure=False, sort_field="", debug=False):
        logging.basicConfig()
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        if host:
            self.host = host
        if path:
            self.path = path
        if owner:
            self.owner = owner
        if name:
            self.name = name
        if experimentals:
            self.experimentals = experimentals
        if dailies:
            self.dailies = dailies
        if weeklies:
            self.weeklies = weeklies
        if releases:
            self.releases = releases
        if json:
            self.json = json
        protocol = "https"
        if insecure:
            self.insecure = insecure
            protocol = "http"
        if sort_field:
            self.sort_field = sort_field
        if debug:
            self.debug = debug
            self.logger.setLevel(logging.DEBUG)
            self.logger.debug("Debug logging on.")
        exthost = self.host
        reghost = exthost
        if reghost == "hub.docker.com":
            reghost = "registry.hub.docker.com"
        if port:
            exthost += ":" + str(port)
            reghost += ":" + str(port)
        if cachefile:
            self.cachefile = cachefile
            self._read_cache()
        if not self.path:
            self.path = ("/v2/repositories/" + self.owner + "/" +
                         self.name + "/tags/")
        self.url = protocol + "://" + exthost + self.path
        self.registry_url = (protocol + "://" + reghost + "/v2/" +
                             self.owner + "/" + self.name + "/")
        self.logger.debug("URL: {}".format(self.url))
        self.logger.debug("Registry URL: {}".format(self.registry_url))

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        """Close the session"""
        if self._session:
            self._session.close()

    def extract_image_info(self):
        """Build image name list and image description list"""
        cs = []
        if (self.recommended and "recommended" in self.data):
            cs.extend(self.data["recommended"])
        for k in ["experimental", "daily", "weekly", "release"]:
            if k in self.data:
                cs.extend(self.data[k])
        ldescs = []
        for c in cs:
            tag = c["name"]
            ld = c.get("description")
            if not ld:
                ld = self._describe_tag(tag)
            ldescs.append(ld)
        ls = [self.owner + "/" + self.name + ":" + x["name"] for x in cs]
        return ls, ldescs

    def _read_cache(self):
        try:
            with open(self.cachefile) as f:
                data = json.load(f)
        except Exception as exc:
            self.logger.error(
                "Failed to load cachefile '{}'".format(self.cachefile))
            self.logger.error("Error: {}".format(exc))
            return
        for kind in data.keys():
            for item in data[kind]:
                tstr = item["updated"]
                item["updated"] = self._convert_time(tstr)
        self.data = data

    def _describe_tag(self, tag):
        # New-style tags have underscores separating components.
        ld = tag  # Default description is just the tag name
        components = None
        if tag.find('_') != -1:
            components = tag.split('_')
            btype = components[0]
            # Handle the r17_0_1 case.
            ctm = re.search(r'\d+$', btype)
            if ctm is not None:
                mj = int(ctm.group())
                components.insert(1, mj)
                btype = btype[0]
            if (tag.startswith("recommended") or tag.startswith("latest")):
                ld = tag[0].upper() + tag[1:]
                restag = self.resolve_tag(tag)
                if restag:
                    ld += " ({})".format(self._describe_tag(restag))
            elif btype == "r":
                rmaj = components[1]
                rmin = components[2]
                rpatch = None
                rrest = None
                if len(components) > 3:
                    rpatch = components[3]
                if len(components) > 4:
                    rrest = "_".join(components[4:])
                ld = "Release %s.%s" % (rmaj, rmin)
                if rpatch:
                    ld = ld + "." + rpatch
                if rrest:
                    ld = ld + "-" + rrest
            elif btype == "w":
                year = components[1]
                week = components[2]
                ld = "Weekly %s_%s" % (year, week)
            elif btype == "d":
                year = components[1]
                month = components[2]
                day = components[3]
                ld = "Daily %s_%s_%s" % (year, month, day)
            elif btype == "exp":
                rest = "_".join(components[1:])
                ld = "Experimental %s" % rest
        else:
            if (tag.startswith("recommended") or tag.startswith("latest")):
                ld = tag[0].upper() + tag[1:]
                restag = self.resolve_tag(tag)
                if restag:
                    ld += " ({})".format(self._describe_tag(restag))
            elif tag[0] == "r":
                rmaj = tag[1:3]
                rmin = tag[3:]
                ld = "Release %s.%s" % (rmaj, rmin)
            elif tag[0] == "w":
                year = tag[1:5]
                week = tag[5:]
                ld = "Weekly %s_%s" % (year, week)
            elif tag[0] == "d":
                year = tag[1:5]
                month = tag[5:7]
                day = tag[7:]
                ld = "Daily %s_%s_%s" % (year, month, day)
            elif tag[0] == "e":
                rest = tag[1:]
                ld = "Experimental %s" % rest
        return ld

    def resolve_tag(self, tag):
        """Resolve a tag (used for "recommended" or "latest*" """
        mfest = self._name_to_manifest.get(tag)
        if not mfest:
            return None
        hash = mfest.get("hash")
        if not hash:
            return None
        for k in self._name_to_manifest:
            if (k.startswith("recommended") or k.startswith("latest")):
                continue
            if self._name_to_manifest[k].get("hash") == hash:
                return k

    def _data_to_json(self):
        rdata = copy.deepcopy(self.data)
        for kind in rdata:
            for entry in rdata[kind]:
                dt = entry["updated"]
                entry["updated"] = dt.isoformat()
        return json.dumps(rdata, sort_keys=True, indent=4)

    def report(self):
        """Print the tag data"""
        if self.json:
            print(self._data_to_json())
        else:
            ls, ldescs = self.extract_image_info()
            ldstr = ",".join(ldescs)
            lstr = ",".join(ls)
            print("# Environment variables for Jupyter Lab containers")
            print("LAB_CONTAINER_NAMES=\'%s\'" % lstr)
            print("LAB_CONTAINER_DESCS=\'%s\'" % ldstr)
            print("export LAB_CONTAINER_NAMES LAB_CONTAINER_DESCS")

    def get_data(self):
        """Return the tag data"""
        return self.data

    def get_all_tags(self):
        """Return all tags in the repository."""
        return self._all_tags

    def _get_url(self, **kwargs):
        params = None
        resp = None
        url = self.url
        if kwargs:
            params = urllib.parse.urlencode(kwargs)
            url += "?%s" % params
        headers = {"Accept": "application/json"}
        req = urllib.request.Request(url, None, headers)
        resp = urllib.request.urlopen(req)
        page = resp.read()
        return page

    def scan(self):
        url = self.url
        self.logger.debug("Beginning repo scan of '{}'.".format(url))
        results = []
        page = 1
        resp_bytes = None
        while True:
            try:
                resp_bytes = self._get_url(page=page)
            except Exception as e:
                message = "Failure retrieving %s: %s" % (url, str(e))
                if resp_bytes:
                    message += " [ data: %s ]" % (
                        str(resp_bytes.decode("utf-8")))
                raise ValueError(message)
            resp_text = resp_bytes.decode("utf-8")
            try:
                j = json.loads(resp_text)
            except ValueError:
                raise ValueError("Could not decode '%s' -> '%s' as JSON" %
                                 (url, str(resp_text)))
            results.extend(j["results"])
            if "next" not in j or not j["next"]:
                break
            page = page + 1
        self._results = results
        self._map_names_to_manifests()
        self._reduce_results()

    def _map_names_to_manifests(self):
        results = self._results
        namemap = self._name_to_manifest
        check_names = []
        for res in results:
            name = res["name"]
            tstamp = self._convert_time(res["last_updated"])
            if not namemap.get(name):
                namemap[name] = {
                    "layers": None,
                    "updated": tstamp,
                    "hash": None
                }
            if tstamp <= namemap[name]["updated"] and namemap[name]["hash"]:
                # We have a manifest
                continue
            check_names.append(name)
        if not check_names:
            self.logger.debug("All images have current hash.")
            # We're up to date.
            return
        baseurl = self.registry_url
        url = baseurl + "manifests/recommended"
        i_resp = requests.get(url)
        authtok = None
        sc = i_resp.status_code
        if sc == 401:
            self.logger.debug("Getting token to retrieve layer lists.")
            magicheader = i_resp.headers['Www-Authenticate']
            if magicheader[:7] == "Bearer ":
                hd = {}
                hl = magicheader[7:].split(",")
                for hn in hl:
                    il = hn.split("=")
                    kk = il[0]
                    vv = il[1].replace('"', "")
                    hd[kk] = vv
                if (not hd or "realm" not in hd or "service" not in hd
                        or "scope" not in hd):
                    return None
                endpoint = hd["realm"]
                del hd["realm"]
                tresp = requests.get(endpoint, params=hd, json=True)
                jresp = tresp.json()
                authtok = jresp.get("token")
        elif sc != 200:
            self.logger.warning("GET %s -> %d" % (url, sc))
        headers = {"Accept": "application/json"}
        if authtok:
            headers.update({"Authorization": "Bearer {}".format(authtok)})
        for name in check_names:
            self.logger.debug("Calculating hash for '{}' tag.".format(name))
            resp = requests.get(baseurl + "manifests/{}".format(name),
                                headers=headers,
                                json=True)
            recjson = {}
            if resp:
                recjson = resp.json()
            layers = self._get_layers(recjson)
            namemap[name]["layers"] = layers
            hash = self._get_layer_hash(layers)
            namemap[name]["hash"] = hash
            self.logger.debug("{} hash: {}".format(name, hash))
        self._name_to_manifest = namemap

    def _get_layers(self, recjson):
        fsl = recjson.get("fsLayers")
        if not fsl:
            return None
        return [x["blobSum"] for x in fsl]

    def _get_layer_hash(self, layers):
        lstr = "+".join(layers).encode('utf-8')
        lhash = hashlib.sha256(lstr)
        return lhash.hexdigest()

    def _reduce_results(self):
        results = self._results
        sort_field = self.sort_field
        # Recommended
        # Release/Weekly/Daily
        # Experimental/Latest/Other
        c_candidates = []
        r_candidates = []
        w_candidates = []
        d_candidates = []
        e_candidates = []
        l_candidates = []
        o_candidates = []
        # This is the order for tags to appear in menu:
        displayorder = []
        if self.recommended:
            displayorder.extend([c_candidates])
        displayorder.extend([e_candidates, d_candidates, w_candidates,
                             r_candidates])
        # This is the order for tags to appear in drop-down:
        imgorder = [l_candidates]
        imgorder.extend(displayorder)
        imgorder.extend(o_candidates)
        reduced_results = {}
        for res in results:
            vname = res["name"]
            reduced_results[vname] = {
                "name": vname,
                "id": res["id"],
                "size": res["full_size"],
                "description": self._describe_tag(vname)
            }
            entry = reduced_results[vname]
            manifest = self._name_to_manifest.get(vname)
            if manifest:
                entry["updated"] = manifest.get("updated")
                entry["hash"] = manifest.get("hash")
            else:
                entry["updated"] = self._convert_time(res["last_updated"])
                entry["hash"] = None
        for res in reduced_results:
            if res.startswith("r") and not res.startswith("recommended"):
                r_candidates.append(reduced_results[res])
            elif res.startswith("w"):
                w_candidates.append(reduced_results[res])
            elif res.startswith("d"):
                d_candidates.append(reduced_results[res])
            elif res.startswith("exp"):
                e_candidates.append(reduced_results[res])
            elif res.startswith("latest"):
                l_candidates.append(reduced_results[res])
            elif res.startswith("recommended"):
                c_candidates.append(reduced_results[res])
            else:
                o_candidates.append(res)
        for clist in imgorder:
            if sort_field != 'name':
                clist.sort(key=lambda x: x[sort_field], reverse=True)
            else:
                clist = self._sort_images_by_name(clist)
        r = {}
        # Index corresponds to order in displayorder
        idxbase = 0
        imap = {}
        if self.recommended:
            imap.update({"recommended": {"index": idxbase,
                                         "count": 1}
                         })
            idxbase = 1
        imap.update({"experimental": {"index": idxbase,
                                      "count": self.experimentals},
                     "daily": {"index": idxbase + 1,
                               "count": self.dailies},
                     "weekly": {"index": idxbase + 2,
                                "count": self.weeklies},
                     "release": {"index": idxbase + 3,
                                 "count": self.releases}
                     })
        for ikey in list(imap.keys()):
            idx = imap[ikey]["index"]
            ict = imap[ikey]["count"]
            if ict:
                r[ikey] = displayorder[idx][:ict]
        all_tags = []
        for clist in imgorder:
            all_tags.extend(x["name"] for x in clist)
        self.data = r
        if self.cachefile:
            try:
                with open(self.cachefile, 'w') as f:
                    f.write(self._data_to_json())
            except Exception as exc:
                self.logger.error(
                    "Could not write to {}: {}".format(self.cachefile, exc))
        self._all_tags = all_tags

    def _sort_images_by_name(self, clist):
        # We have a flag day where we start putting underscores into
        #  image tags.  Those always go at the top.
        # We begin by splitting the list of candidate images into new
        #  and old style images.
        oldstyle = []
        newstyle = []
        for cimg in clist:
            name = cimg["name"]
            if name.find("_") == -1:
                oldstyle.append(cimg)
            else:
                # "latest_X" is not a semantic version tag.
                if name.startswith("latest_"):
                    oldstyle.append(cimg)
                else:
                    newstyle.append(cimg)
        # Old-style sort is simple string comparison.
        oldstyle.sort(key=lambda x: x["name"], reverse=True)
        # New style, we refer to semver module for comparison.
        #  (also works fine for date sorts)
        seml = []
        for cimg in newstyle:
            name = cimg["name"]
            components = name.split("_")
            # Get this.  It's not represented as r_17, no, it's r17.
            # So if we find that the end of the first group is digits,
            #  we split those off with a regular expression, and insert
            #  them into the list where the major number should be.
            ctype = components[0]
            ctm = re.search(r'\d+$', ctype)
            if ctm is not None:
                mj = int(ctm.group())
                components.insert(1, mj)
            # First character is image type, not semantically significant
            #  for versioning.
            if components[0] == "exp":
                _ = components.pop(0)
            major = 0
            if len(components) > 1:
                major = int(components[1])
            minor = 0
            if len(components) > 2:
                minor = int(components[2])
            patch = 0
            prerelease = None
            if len(components) > 3:
                try:
                    patch = int(components[3])
                except ValueError:
                    # Not an integer, so this is probably an experimental/
                    #  not-for-release version, so leave it at patch level 0
                    #  and treat the string as a prerelease version
                    prerelease = components[3]
            if len(components) > 4:
                prerelease = components[4]
            build = None
            if len(components) > 5:
                build = '_'.join(components[5:])
            cimg["semver"] = semver.format_version(
                major, minor, patch, prerelease, build)
            seml.append(cimg["semver"])
        seml.sort(key=functools.cmp_to_key(semver.compare), reverse=True)
        sorted_newstyle = []
        for skey in seml:
            for ni in newstyle:
                if ni["semver"] == skey:
                    sorted_newstyle.append(ni)
                    break
        # Return all new style names first.
        return sorted_newstyle.extend(oldstyle)

    def _sort_releases_by_name(self, r_candidates):
        # rXYZrc2 should *precede* rXYZ
        # We're going to decorate short (that is, no rc tag) release names
        #  with "zzz", re-sort, and then undecorate.
        nm = {}
        for c in r_candidates:
            tag = c["name"]
            if len(tag) == 4:
                xtag = tag+"zzz"
                nm[xtag] = tag
                c["name"] = xtag
        r_candidates.sort(key=lambda x: x["name"], reverse=True)
        for c in r_candidates:
            xtag = c["name"]
            c["name"] = nm[xtag]
        return r_candidates

    def _convert_time(self, ts):
        f = '%Y-%m-%dT%H:%M:%S.%f%Z'
        if ts[-1] == "Z":
            ts = ts[:-1] + "UTC"
        return datetime.datetime.strptime(ts, f)
