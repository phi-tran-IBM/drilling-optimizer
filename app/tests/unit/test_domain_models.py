"""Unit tests for domain models"""
import pytest
from app.domain.models.drilling_plan import DrillingPlan, Formation

def test_formation_model():
    """Test Formation model validation"""
    formation = Formation(
        name="Bone Spring",
        depth_start=8000.0,
        depth_end=10000.0
    )
    assert formation.name == "Bone Spring"
    assert formation.depth_end > formation.depth_start
