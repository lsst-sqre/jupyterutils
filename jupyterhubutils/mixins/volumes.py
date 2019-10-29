import logging


class LSSTVolumes(object):
    """Mixin class to provide support for document-driven Volume assignment
    """

    def __init__(self, args, **kwargs):
        # Add a logger if we don't already have one.
        if hasattr(self, 'log') and self.log:
            return
        self.log = logging.getLogger(__name__)
