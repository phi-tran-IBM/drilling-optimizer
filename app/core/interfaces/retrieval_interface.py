from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional

class RetrievalInterface(ABC):
    """
    Abstract interface for all retrieval strategies.
    Allows swapping between different retrieval implementations.
    """
    
    @abstractmethod
    def retrieve(self, 
                query: str, 
                context: Optional[Dict[str, Any]] = None,
                limit: int = 10) -> Dict[str, Any]:
        """
        Retrieve relevant information for a query.
        
        Returns:
            Dict containing retrieved context with standardized structure
        """
        pass
    
    @abstractmethod
    def get_strategy_name(self) -> str:
        """Return the name of this retrieval strategy"""
        pass
