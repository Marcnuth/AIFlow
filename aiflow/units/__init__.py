import pkgutil
import logging
from abc import ABC, abstractmethod
import requests
import itertools
from multiprocessing import cpu_count
from concurrent.futures import ProcessPoolExecutor, as_completed


logger = logging.getLogger(__name__)


class Unit(ABC):

    def __init__(self, *args, **kwargs):
        pass

    @abstractmethod
    def execute(self, **kwargs):
        pass


__all__ = []
for loader, module_name, is_pkg in pkgutil.walk_packages(__path__):
    __all__.append(module_name)
    _module = loader.find_module(module_name).load_module(module_name)
    globals()[module_name] = _module
