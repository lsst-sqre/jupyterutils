import copy
import datetime
import json
import logging
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
    dailies = 3
    weeklies = 2
    releases = 1
    _all_tags = []
    logger = None

    def __init__(self, host='', path='', owner='', name='',
                 dailies=3, weeklies=2, releases=1,
                 json=False, port=None,
                 insecure=False, sort_field="", debug=False):
        logging.basicConfig()
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.logger.setLevel(logging.INFO)
        if host:
            self.host = host
        if path:
            self.path = path
        if owner:
            self.owner = owner
        if name:
            self.name = name
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
        if port:
            exthost += ":" + str(port)
        if not self.path:
            self.path = ("/v2/repositories/" + self.owner + "/" +
                         self.name + "/tags/")
        self.url = protocol + "://" + exthost + self.path
        self.logger.debug("URL %s" % self.url)

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
        for k in ["daily", "weekly", "release"]:
            cs.extend(self.data[k])
        ldescs = []
        for c in cs:
            tag = c["name"]
            if tag[0] == "r":
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
            ldescs.append(ld)
        ls = [self.owner + "/" + self.name + ":" + x["name"] for x in cs]
        return ls, ldescs

    def report(self):
        """Print the tag data"""
        if self.json:
            rdata = copy.deepcopy(self.data)
            for kind in rdata:
                for entry in rdata[kind]:
                    dt = entry["updated"]
                    entry["updated"] = dt.isoformat()
            print(json.dumps(rdata, sort_keys=True, indent=4))
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
        self._reduce_results(results)

    def _reduce_results(self, results):
        sort_field = self.sort_field
        # Release/Weekly/Daily
        # Experimental/Latest/Other
        r_candidates = []
        w_candidates = []
        d_candidates = []
        e_candidates = []
        l_candidates = []
        o_candidates = []
        # This is the order for tags to appear in menu:
        displayorder = [d_candidates, w_candidates, r_candidates]
        # This is the order for tags to appear in drop-down:
        imgorder = [l_candidates, e_candidates]
        imgorder.extend(displayorder)
        imgorder.extend(o_candidates)
        reduced_results = {}
        for res in results:
            vname = res["name"]
            reduced_results[vname] = {
                "name": vname,
                "id": res["id"],
                "size": res["full_size"],
                "updated": self._convert_time(res["last_updated"])
            }
        for res in reduced_results:
            fc = res[0]
            if fc == "r":
                r_candidates.append(reduced_results[res])
            elif fc == "w":
                w_candidates.append(reduced_results[res])
            elif fc == "d":
                d_candidates.append(reduced_results[res])
            elif res[:3] == "exp":
                e_candidates.append(reduced_results[res])
            elif res[:6] == "latest":
                l_candidates.append(reduced_results[res])
            else:
                o_candidates.append(res)
        for clist in imgorder:
            clist.sort(key=lambda x: x[sort_field], reverse=True)
        if sort_field == 'name':
            r_candidates=self._sort_releases_by_name(r_candidates)
        r = {}
        # Index corresponds to order in displayorder
        imap = {"daily": {"index": 0,
                          "count": self.dailies},
                "weekly": {"index": 1,
                           "count": self.weeklies},
                "release": {"index": 2,
                            "count": self.releases}
                }
        for ikey in list(imap.keys()):
            idx = imap[ikey]["index"]
            ict = imap[ikey]["count"]
            r[ikey] = displayorder[idx][:ict]
        all_tags = []
        for clist in imgorder:
            all_tags.extend(x["name"] for x in clist)
        self.data = r
        self._all_tags = all_tags

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
