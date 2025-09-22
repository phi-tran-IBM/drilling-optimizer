from abc import ABC, abstractmethod
from typing import Dict, Any, List

class ValidationInterface(ABC):
    """Abstract interface for validation strategies"""
    
    @abstractmethod
    def validate(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """Validate a plan and return results"""
        pass
