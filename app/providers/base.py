from abc import ABC, abstractmethod

class ProviderBase(ABC):

    def __init__(self, settings: dict):
        self.settings = settings

    @abstractmethod
    def fetch_data(self) -> UnifiedReading:
        pass
