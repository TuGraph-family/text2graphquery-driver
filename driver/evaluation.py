from abc import ABC, abstractmethod
from typing import List, Dict, Any, Union

class DatabaseDriver(ABC):
    """Database Driver Interface"""
    @abstractmethod
    def connect(self):
        """Establish connection"""
        pass

    @abstractmethod
    def query(self, cypher: str, db_name: str) -> Union[List[Dict], None]:
        """Execute the query and return the result list; return None if an error occurs."""
        pass

    @abstractmethod
    def close(self):
        """Close connection"""
        pass

class BaseMetric(ABC):
    """Evaluation Metrics Interface"""
    @abstractmethod
    def compute(self, predictions: List[str], golds: List[str], **kwargs) -> Any:
        """Metric Calculation"""
        pass