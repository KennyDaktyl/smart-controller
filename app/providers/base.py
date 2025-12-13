from abc import ABC, abstractmethod
from app.schemas.base_reading import UnifiedReading

class ProviderBase(ABC):

    def __init__(self, settings: dict):
        """settings = konfiguracja dla usera np. username/password, IP, token, itp."""
        self.settings = settings

    @abstractmethod
    def fetch_data(self) -> UnifiedReading:
        """Zwraca dane w ujednoliconym formacie."""
        pass
