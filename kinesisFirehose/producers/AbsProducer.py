"""
In this script, we define the API for the producers
"""

from abc import ABC, abstractmethod

class Producer(ABC):
    """
    A class defining the API for producers in future
    """
    @abstractmethod
    def connect_source(self):
        pass
    @abstractmethod
    def start_stream(self):
        pass
    @abstractmethod
    def read1(self):
        pass