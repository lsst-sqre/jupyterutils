import datetime
import threading
import time
from .scanrepo import ScanRepo
from ..singleton import Singleton


class SingletonScanner(ScanRepo, metaclass=Singleton):
    '''Singleton Object to hold rate-limited scanner.
    '''
    min_refresh_time = 60
    max_cache_age = 600
    last_updated = datetime.datetime(1970, 1, 1)  # The Epoch
    scanning = False
    lock = threading.RLock()

    def __init__(self, **kwargs):
        min_refresh_time = kwargs.get('min_refresh_time')
        if min_refresh_time is None:
            min_refresh_time = 60
        max_cache_age = kwargs.get('max_cache_age')
        if max_cache_age is None:
            max_cache_age = 600
        # Now remove them from kwargs before superclass init
        for karg in ['min_refresh_time', 'max_cache_age']:
            if karg in kwargs:
                kwargs.pop(karg)
        super().__init__(**kwargs)
        self.min_refresh_time = min_refresh_time
        self.max_cache_age = max_cache_age
        if max_cache_age < min_refresh_time:
            max_cache_age = 2 * min_refresh_time
            self.max_cache_age = max_cache_age
            self.logger.error("Nonsensical cache age/refresh time ratio.")
            self.logger.warning("Setting max_age to %ds." % max_cache_age)
        thd = threading.Thread(target=self.scan)
        self.logger.info("Starting background scan.")
        thd.start()

    def scan(self):
        '''Execute repo scan.
        '''
        _maxdelay = 300
        _initialdelay = 5
        _sofar = 0
        delay = _initialdelay
        if self.scanning:
            while not self._results:
                # If there have been no results, wait up to _maxdelay seconds
                #  for them.
                self.logger.info(
                    "Scan in progress; waiting {}s for results.".format(delay))
                time.sleep(delay)
                _sofar += delay
                if _sofar >= _maxdelay:
                    self.logger.warning("Scan in progress never finished.")
                    break
                delay *= 2
                if _sofar + delay > _maxdelay:
                    delay = _maxdelay - _sofar
            return  # Use cached (possibly stale) version if results exist.
        self.scanning = True
        now = datetime.datetime.utcnow()
        mt = self.min_refresh_time
        min_delay = datetime.timedelta(seconds=mt)
        if (now - self.last_updated < min_delay):
            self.logger.warning("%ds not elapsed; not rescanning." % mt)
            self.scanning = False
            return
        self.logger.info("Rescanning.")
        with self.lock:
            super().scan()
            self.last_updated = now
            self.scanning = False

    def _scan_if_needed(self):
        now = datetime.datetime.utcnow()
        max_age = datetime.timedelta(seconds=self.max_cache_age)
        last_updated = self.last_updated
        if ((now - last_updated) > max_age):
            self.logger.info("Scan data has expired.")
            self.scan()

    def get_data(self):
        '''Return repo data.
        '''
        self._scan_if_needed()
        return self.data

    def get_all_tags(self):
        '''Return all tags in repo.
        '''
        self._scan_if_needed()
        return self._all_tags

    def get_all_scan_results(self):
        '''Return results from repository scan as dict.
        '''
        self._scan_if_needed()
        return self._results_map

    def extract_image_info(self):
        '''Get info for all images.
        '''
        self._scan_if_needed()
        return super().extract_image_info()

    def report(self):
        '''Report results of scan.
        '''
        self._scan_if_needed()
        return super().report()
