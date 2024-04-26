from dataclasses import dataclass
from multiprocessing import Event
from multiprocessing.managers import BaseManager
from multiprocessing.synchronize import Event as EventType
from typing import Optional


class DashboardStartedEvent:
    
    def __init__(self) -> None:
        self.event: Optional[EventType] = None
        
    def init(self) -> None:
        self.event = Event()
    
    def reset(self) -> None:
        self.event = None
        
    def set(self) -> None:
        if self.event is None:
            self.init()
        self.event.set()
    
    def is_set(self) -> bool:
        return self.event.is_set() if self.event is not None else False
    
    def wait(self, timeout: Optional[float] = None) -> bool:
        return self.event.wait(timeout) if self.event is not None else False


class DashboardManager(BaseManager):
    pass


@dataclass
class DashboardManagerConnectionDetails:
    host: Optional[str] = None
    port: Optional[int] = None
    
    def clear(self) -> None:
        self.host = None
        self.port = None
