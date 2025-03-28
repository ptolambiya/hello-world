# etl_application/connections/base_connection.py
from abc import ABC, abstractmethod

class BaseConnection(ABC):
    @abstractmethod
    def connect(self, config: dict):
        pass
    
    @abstractmethod
    def extract_data(self, query: str = None):
        pass
    
    @abstractmethod
    def close(self):
        pass
