"""
In this script, we define the API for the producers
"""

from abc import ABC, abstractmethod

class Producer(ABC):
    @abstractmethod
    def connect_source(self):
        pass
    @abstractmethod
    def start_stream(self):
        pass
    @abstractmethod
    def get_next(self):
        pass