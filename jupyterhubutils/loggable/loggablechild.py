from .loggable import Loggable


class LoggableChild(Loggable):
    parent = None

    def __init__(self, *args, **kwargs):
        parent = kwargs.pop('parent', self.parent)
        if not parent:
            es = "Child object must be passed parent at __init__()"
            raise ValueError(es)
        self.parent = parent
        super().__init__(*args, **kwargs)
