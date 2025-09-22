from abc import ABC, abstractmethod
from typing import Dict, Any, Optional

class LLMInterface(ABC):
    """Abstract interface for LLM interactions"""
    
    @abstractmethod
    def generate(self, prompt: str, **kwargs) -> str:
        """Generate text from prompt"""
        pass
