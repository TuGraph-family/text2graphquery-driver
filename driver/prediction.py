from abc import ABC, abstractmethod
from typing import List, Dict

class Text2GraphSystem(ABC):
    """Generation System Interface"""
    @abstractmethod
    def predict_batch(self, data: List[Dict]) -> List[Dict]:
        """Batch Prediction"""
        pass