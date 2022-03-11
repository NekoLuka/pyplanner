from abc import ABC, abstractmethod


class BaseDistributor(ABC):
    @abstractmethod
    def save_schedule(self):
        pass

    @abstractmethod
    def get_schedule(self):
        pass
