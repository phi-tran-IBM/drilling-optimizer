# app/retrieval/strategies/constraint_first_strategy.py
from typing import Dict, Any, Optional
from app.retrieval.strategies.base_strategy import BaseRetrievalStrategy

class ConstraintFirstStrategy(BaseRetrievalStrategy):
    """
    Retrieval strategy that prioritizes engineering constraints.
    Optimal for planning and validation queries.
    """
    
    def get_strategy_name(self) -> str:
        return "constraint_first"
    
    def retrieve(self,
                query: str,
                context: Optional[Dict[str, Any]] = None,
                limit: int = 10) -> Dict[str, Any]:
        """
        1. First retrieve all applicable constraints
        2. Then get formation context
        3. Finally get relevant documents
        """
        well_id = context.get("well_id") if context else None
        
        # Step 1: Get all constraints for the well/formation
        constraints = self._retrieve_constraints(well_id)
        
        # Step 2: Get formation-specific context
        formations = self._retrieve_formations(well_id)
        
        # Step 3: Get constraint-filtered historical examples
        examples = self._retrieve_constrained_examples(constraints, formations)
        
        # Step 4: Get relevant documents
        docs = self._retrieve_documents(query, constraints)
        
        return {
            "constraints": constraints,
            "formations": formations,
            "historical_examples": examples,
            "documents": docs,
            "metadata": {
                "strategy": self.get_strategy_name(),
                "constraint_count": len(constraints),
                "priority": "constraints_first"
            }
        }
    
    def _retrieve_constraints(self, well_id: Optional[str]) -> list:
        """Retrieve all applicable constraints from knowledge graph"""
        if not self.kg_client or not well_id:
            return []
        
        query = """
        MATCH (w:Well {well_id: $well_id})-[:HAS_FORMATION]->(f:Formation)
        MATCH (f)-[:HAS_CONSTRAINT]->(c:EngineeringConstraint)
        RETURN c
        ORDER BY c.priority DESC
        """
        
        return self.kg_client.query(query, {"well_id": well_id})