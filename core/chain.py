from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Optional
import pandas as pd


class PipelineNode(ABC):
    def __init__(self) -> None:
        self._next: Optional["PipelineNode"] = None

    def set_next(self, node: "PipelineNode") -> "PipelineNode":
        self._next = node
        return node

    def handle(self, frame: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
        processed = self.process(frame)
        if self._next:
            return self._next.handle(processed)
        return processed

    @abstractmethod
    def process(self, frame: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
        ...
