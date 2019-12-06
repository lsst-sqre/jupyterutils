'''Classes and tool for scanning Docker repositories.
'''
from .scanrepo import ScanRepo
from .standalone import standalone
from .singletonscanner import SingletonScanner
__all__ = [ScanRepo, SingletonScanner, standalone]
