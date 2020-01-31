from ..utils import make_logger


class Loggable(object):
    log = None

    def __init__(self, *args, **kwargs):
        self.log = make_logger()
        self.log.debug("Creating {}".format(self.__class__.__name__))
