"""Unit tests for core interfaces"""
import pytest
from app.core.interfaces import RetrievalInterface

def test_retrieval_interface():
    """Test that interface cannot be instantiated directly"""
    with pytest.raises(TypeError):
        RetrievalInterface()
