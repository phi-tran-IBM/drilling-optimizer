from abc import abstractmethod
from typing import Dict, Any, Optional
from app.core.interfaces import RetrievalInterface, KnowledgeGraphInterface
from app.core.config.settings import settings

class BaseRetrievalStrategy(RetrievalInterface):
    """
    Base class for all retrieval strategies.
    Provides common functionality and enforces interface.
    """
    
    def __init__(self, 
                 kg_client: Optional[KnowledgeGraphInterface] = None,
                 vector_client: Optional[Any] = None):
        self.kg_client = kg_client
        self.vector_client = vector_client
        self.graph_weight = settings.graph_weight
        self.astra_weight = settings.astra_weight
    
    @abstractmethod
    def retrieve(self, 
                query: str,
                context: Optional[Dict[str, Any]] = None,
                limit: int = 10) -> Dict[str, Any]:
        """Must be implemented by each strategy"""
        pass
    
    def _merge_contexts(self, 
                       graph_context: Dict,
                       vector_context: Dict) -> Dict[str, Any]:
        """
        Merge graph and vector contexts with weighting.
        """
        return {
            "formations": graph_context.get("formations", []),
            "constraints": graph_context.get("constraints", []),
            "documents": vector_context.get("docs", []),
            "metadata": {
                "graph_weight": self.graph_weight,
                "vector_weight": self.astra_weight,
                "strategy": self.get_strategy_name()
            }
        }
