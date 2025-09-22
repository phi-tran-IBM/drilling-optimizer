"""
Define the core ontology for the drilling domain knowledge graph
Based on the Knowledge-Driven architecture recommendations
"""

from typing import Dict, List, Any
from enum import Enum

class NodeLabels(str, Enum):
    """Standard node labels in the drilling knowledge graph"""
    WELL = "Well"
    FORMATION = "Formation"
    BHA_TOOL = "BHATool"
    DRILLING_PARAMETER = "DrillingParameter"
    ENGINEERING_CONSTRAINT = "EngineeringConstraint"
    HISTORICAL_PLAN = "HistoricalPlan"
    KPI = "KPI"
    PLAN_ITERATION = "PlanIteration"

class RelationshipTypes(str, Enum):
    """Standard relationship types in the drilling knowledge graph"""
    HAS_FORMATION = "HAS_FORMATION"
    USES_TOOL = "USES_TOOL"
    HAS_CONSTRAINT = "HAS_CONSTRAINT"
    PREDICTED_TO_VIOLATE = "PREDICTED_TO_VIOLATE"
    CAUSES_IMPROVEMENT = "CAUSES_IMPROVEMENT"

class DrillingOntology:
    """
    Central ontology definition for the drilling domain.
    """
    
    @staticmethod
    def get_node_schema(label: NodeLabels) -> Dict[str, Any]:
        """Get the property schema for a node type"""
        schemas = {
            NodeLabels.WELL: {
                "required": ["well_id"],
                "properties": {
                    "well_id": "string",
                    "api_number": "string",
                    "location": "geospatial",
                    "well_type": "string"
                }
            },
            NodeLabels.FORMATION: {
                "required": ["name"],
                "properties": {
                    "name": "string",
                    "depth_start": "float",
                    "depth_end": "float",
                    "rock_strength": "float"
                }
            }
        }
        return schemas.get(label, {})
