# app/core/interfaces/knowledge_graph_interface.py
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional

class KnowledgeGraphInterface(ABC):
    """
    Abstract interface for knowledge graph operations.
    Enables switching between Neo4j, NetworkX, or other graph backends.
    """
    
    @abstractmethod
    def query(self, cypher: str, parameters: Optional[Dict] = None) -> List[Dict]:
        """Execute a graph query"""
        pass
    
    @abstractmethod
    def get_subgraph(self, 
                     node_id: str, 
                     max_depth: int = 2,
                     relationship_types: Optional[List[str]] = None) -> Dict:
        """Retrieve a subgraph centered on a node"""
        pass
    
    @abstractmethod
    def upsert_node(self, 
                   label: str,
                   properties: Dict[str, Any],
                   merge_key: str) -> str:
        """Create or update a node"""
        pass
    
    @abstractmethod
    def create_relationship(self,
                          from_node: Dict,
                          to_node: Dict,
                          rel_type: str,
                          properties: Optional[Dict] = None) -> bool:
        """Create a relationship between nodes"""
        pass