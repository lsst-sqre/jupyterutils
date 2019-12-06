'''The LSSTMiddleManager is the only exported class: all the managers that
actually manage things report to it.
'''
from .middlemanager import LSSTMiddleManager

__all__ = [LSSTMiddleManager]
