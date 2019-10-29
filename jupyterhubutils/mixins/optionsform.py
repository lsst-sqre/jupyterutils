from .. import SingletonScanner

import datetime
import json
import logging
import os

from .logobject import LSSTLogObject


class LSSTOptionsForm(LSSTLogObject):
    """Mixin class to add a scanner-based options form and read values
       from it."""

    sizelist = ["tiny", "small", "medium", "large"]
    _sizemap = {}
    _scanner = None

    def __init__(self, args, **kwargs):
        self.super().__init__(args, kwargs)

    def lsst_options_form(self):
        # Make options form by scanning container repository
        title = os.getenv("LAB_SELECTOR_TITLE") or "Container Image Selector"
        owner = os.getenv("LAB_REPO_OWNER") or "lsstsqre"
        name = os.getenv("LAB_REPO_NAME") or "sciplat-lab"
        host = os.getenv("LAB_REPO_HOST") or "hub.docker.com"
        experimentals = int(os.getenv("PREPULLER_EXPERIMENTALS", 0))
        dailies = int(os.getenv("PREPULLER_DAILIES", 3))
        weeklies = int(os.getenv("PREPULLER_WEEKLIES", 2))
        releases = int(os.getenv("PREPULLER_RELEASES", 1))
        cachefile = os.getenv("HOME") + "/repo-cache.json"
        debug = False
        if os.getenv("DEBUG"):
            debug = True
        scanner = SingletonScanner(host=host,
                                   owner=owner,
                                   name=name,
                                   experimentals=experimentals,
                                   dailies=dailies,
                                   weeklies=weeklies,
                                   releases=releases,
                                   cachefile=cachefile,
                                   debug=debug)
        self._scanner = scanner
        self._sync_scan()
        lnames, ldescs = scanner.extract_image_info()
        if not lnames or len(lnames) < 2:
            return ""
        resmap = scanner.get_all_scan_results()
        all_tags = list(resmap.keys())
        optform = "<label for=\"%s\">%s</label><br />\n" % (title, title)
        now = datetime.datetime.now()
        nowstr = now.ctime()
        if not now.tzinfo:
            # If we don't have tzinfo, assume it's in UTC"
            nowstr += " UTC"
        optform = "<style>\n"
        optform += "    td#clear_dotlocal {\n"
        optform += "        border: 1px solid black;\n"
        optform += "        padding: 2%;\n"
        optform += "    }\n"
        optform += "    td#images {\n"
        optform += "        padding-right: 5%;\n"
        optform += "    }\n"
        optform += "</style>\n"
        optform += "<table>\n        <tr>"
        optform += "<th>Image</th></th><th>Size<br /></th></tr>\n"
        optform += "        <tr><td rowspan=2 id=\"images\">\n"
        self._make_sizemap()
        checked = False
        saveimg = ""
        for idx, img in enumerate(lnames):
            optform += "          "
            optform += " <input type=\"radio\" name=\"kernel_image\""
            optform += " value=\"%s\"" % img
            if not checked:
                checked = True
                saveimg = img
                optform += " checked=\"checked\""
            optform += "> %s<br />\n" % ldescs[idx]
        optform += "          "
        optform += " <input type=\"radio\" name=\"kernel_image\""
        colon = saveimg.find(':')
        custtag = saveimg[:colon] + ":__custom"
        optform += " value=\"%s\"> or select image tag " % custtag
        optform += "          "
        optform += "<select name=\"image_tag\""
        optform += "onchange=\"document.forms['spawn_form']."
        optform += "kernel_image.value='%s'\">\n" % custtag
        optform += "          "
        optform += "<option value=\"latest\"><br /></option>\n"
        for tag in all_tags:
            optform += "            "
            optform += "<option value=\"%s\">%s<br /></option>\n" % (tag, tag)
        optform += "          </select><br />\n"
        optform += "          </td>\n          <td valign=\"top\">\n"
        checked = False
        sizemap = self._sizemap
        sizes = list(sizemap.keys())
        size_index = self._get_size_index()
        defaultsize = sizes[size_index]
        for size in sizemap:
            optform += "            "
            optform += " <input type=\"radio\" name=\"size\""
            if size == defaultsize:
                checked = True
                optform += " checked=\"checked\""
            optform += " value=\"%s\"> %s<br />\n" % (size,
                                                      sizemap[size]["desc"])
        optform += "          </td></tr>\n"
        optform += "          <tr><td id=\"clear_dotlocal\">"
        optform += "<input type=\"checkbox\" name=\"clear_dotlocal\""
        optform += " value=\"true\">"
        optform += " Clear <tt>.local</tt> directory (caution!)<br />"
        optform += "</td></tr>\n"
        optform += "      </table>\n"
        optform += "<hr />\n"
        optform += "Menu updated at %s<br />\n" % nowstr
        return optform

    def _make_sizemap(self):
        sizes = self.sizelist
        tiny_cpu = os.environ.get('TINY_MAX_CPU') or 0.5
        if type(tiny_cpu) is str:
            tiny_cpu = float(tiny_cpu)
        mem_per_cpu = os.environ.get('MB_PER_CPU') or 2048
        if type(mem_per_cpu) is str:
            mem_per_cpu = int(mem_per_cpu)
        cpu = tiny_cpu
        for sz in sizes:
            mem = mem_per_cpu * cpu
            self._sizemap[sz] = {"cpu": cpu,
                                 "mem": mem}
            desc = sz.title() + " (%.2f CPU, %dM RAM)" % (cpu, mem)
            self._sizemap[sz]["desc"] = desc
            cpu = cpu * 2
        # Clean up if list of sizes changed.
        for esz in self._sizemap:
            if esz not in sizes:
                del self._sizemap[esz]

    def _get_size_index(self):
        sizes = list(self._sizemap.keys())
        si = self._custom_resources.get(
            "size_index") or os.environ.get('SIZE_INDEX') or 1
        size_index = int(si)
        if size_index >= len(sizes):
            size_index = len(sizes) - 1
        return size_index

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
