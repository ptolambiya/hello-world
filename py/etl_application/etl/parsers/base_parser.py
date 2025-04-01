from abc import ABC, abstractmethod
from typing import Dict, Type

class BaseParser(ABC):
    _registry: Dict[str, Type['BaseParser']] = {}

    def __init_subclass__(cls, data_type: str = None, **kwargs):
        super().__init_subclass__(**kwargs)
        if data_type:
            cls._registry[data_type] = cls

    @classmethod
    def get_parser_class(cls, data_type: str) -> Type['BaseParser']:
        parser_cls = cls._registry.get(data_type)
        if not parser_cls:
            raise ValueError(f"No parser registered for type: {data_type}")
        return parser_cls
    
    def __init__(self, mappings: list):
        self.mappings = mappings
        
    @abstractmethod
    def parse(self, raw_data):
        pass
