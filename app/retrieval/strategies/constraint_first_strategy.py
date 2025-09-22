from typing import Dict, Any, Optional
from app.retrieval.strategies.base_strategy import BaseRetrievalStrategy

class ConstraintFirstStrategy(BaseRetrievalStrategy):
    """
    Retrieval strategy that prioritizes engineering constraints.
    """
    
    def get_strategy_name(self) -> str:
        return "constraint_first"
    
    def retrieve(self,
                query: str,
                context: Optional[Dict[str, Any]] = None,
                limit: int = 10) -> Dict[str, Any]:
        """
        Retrieve with constraints as priority
        """
        well_id = context.get("well_id") if context else None
        
        # Placeholder implementation
        return {
            "constraints": [],
            "formations": [],
            "documents": [],
            "metadata": {
                "strategy": self.get_strategy_name()
            }
        }
