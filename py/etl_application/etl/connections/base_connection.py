# etl_application/connections/base_connection.py
from abc import ABC, abstractmethod
from typing import Dict, Type, List

class BaseConnection(ABC):
    _registry: Dict[str, Type['BaseConnection']] = {}

    def __init_subclass__(cls, system_types: List[str] = None, **kwargs):
        super().__init_subclass__(**kwargs)
        if system_types:
            for st in system_types:
                cls._registry[st] = cls
            cls.system_types = system_types  # Store supported types

    @classmethod
    def get_connection_class(cls, system_type: str) -> Type['BaseConnection']:
        conn_cls = cls._registry.get(system_type)
        if not conn_cls:
            raise ValueError(f"No connection registered for type: {system_type}")
        return conn_cls

    @abstractmethod
    def connect(self, config: dict):
        pass
    
    @abstractmethod
    def extract_data(self, query: str = None):
        pass
    
    @abstractmethod
    def close(self):
        pass
