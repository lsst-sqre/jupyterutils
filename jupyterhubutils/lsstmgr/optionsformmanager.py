'''Class to manage an LSST-specific options form.
'''
import datetime
import jinja2
import json
import os

from time import sleep

from .. import SingletonScanner
from ..utils import make_logger


class LSSTOptionsFormManager(object):
    '''Class to create and read a spawner form.
    '''

    quota_mgr = None
    sizelist = ["tiny", "small", "medium", "large"]
    _sizemap = {}
    _scanner = None

    def __init__(self, *args, **kwargs):
        self.log = make_logger()
        self.log.debug("Creating LSSTOptionsFormManager")
        self.parent = kwargs.pop('parent')

    def lsst_options_form(self):
        '''Create an LSST Options Form from parent's config object.
        '''
        # Make options form by scanning container repository
        cfg = self.parent.config
        scanner = SingletonScanner(host=cfg.lab_repo_host,
                                   owner=cfg.lab_repo_owner,
                                   name=cfg.lab_repo_name,
                                   experimentals=cfg.prepuller_experimentals,
                                   dailies=cfg.prepuller_dailies,
                                   weeklies=cfg.prepuller_weeklies,
                                   releases=cfg.prepuller_releases,
                                   cachefile=cfg.prepuller_cachefile,
                                   debug=cfg.debug)
        self._scanner = scanner
        self._sync_scan()
        lnames, ldescs = scanner.extract_image_info()
        desclist = []
        # If there's only one image, we don't need a form.
        if not lnames or len(lnames) < 2:
            return ""
        # Setting this up to pass into the Jinja template more easily
        for idx, img in enumerate(lnames):
            desclist.append({"name": img,
                             "desc": ldescs[idx]})
        colon = lnames[0].find(':')
        custtag = lnames[0][:colon] + ":__custom"
        resmap = scanner.get_all_scan_results()
        all_tags = list(resmap.keys())
        now = datetime.datetime.now()
        nowstr = now.ctime()
        if not now.tzinfo:
            # If we don't have tzinfo, assume it's in UTC
            nowstr += " UTC"
        self._make_sizemap()
        template_loader = jinja2.FileSystemLoader()
        template_environment = jinja2.Environment(loader=template_loader)
        template_file = self.parent.config.form_template
        template = template_environment.get_template(template_file)
        optform = template.render(
            defaultsize=cfg.size_index,
            desclist=desclist,
            all_tags=all_tags,
            custtag=custtag,
            sizemap=self._sizemap,
            nowstr=nowstr)
        return optform

    def resolve_tag(self, tag):
        '''Delegate to scanner to resolve convenience tags.
        '''
        return self._scanner.resolve_tag(tag)

    def _sync_scan(self):
        scanner = self._scanner
        delay_interval = 5
        max_delay = 300
        delay = 0
        while scanner.last_updated == datetime.datetime(1970, 1, 1):
            self.log.info("Scan results not available yet; sleeping " +
                          "{}s ({}s so far).".format(delay_interval,
                                                     delay))
            sleep(delay_interval)
            delay = delay + delay_interval
            if delay >= max_delay:
                errstr = ("Scan results did not become available in " +
                          "{}s.".format(max_delay))
                raise RuntimeError(errstr)

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
        sls = list(self._sizemap.keys())
        for esz in sls:
            if esz not in sizes:
                del self._sizemap[esz]

    def _get_size_index(self):
        sizes = list(self._sizemap.keys())
        cfg = self.parent.config
        cr = None
        if self.quota_mgr:
            cr = self.quota_mgr._custom_resources
        if cr is None:
            cr = {}
        si = cr.get("size_index") or cfg.size_index
        size_index = int(si)
        if size_index >= len(sizes):
            size_index = len(sizes) - 1
        return size_index

    def options_from_form(self, formdata=None):
        '''Get user selections.
        '''
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
