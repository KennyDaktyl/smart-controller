from abc import ABC, abstractmethod
from app.domain.provider_state import ProviderState


class ProviderAdapter(ABC):

    def __init__(self, config: dict):
        self.config = config

    @abstractmethod
    def fetch_state(self) -> ProviderState:
        pass
