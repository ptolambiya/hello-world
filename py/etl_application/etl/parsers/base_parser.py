from abc import ABC, abstractmethod

class BaseParser(ABC):
    def __init__(self, mappings: list):
        self.mappings = mappings
        
    @abstractmethod
    def parse(self, raw_data):
        pass
