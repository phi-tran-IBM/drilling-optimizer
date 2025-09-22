import os
from pathlib import Path
from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Optional

class Settings(BaseSettings):
    """
    Centralized configuration management using Pydantic.
    Validates environment variables and provides defaults.
    """
    
    # Core settings
    app_name: str = "Well Planning Knowledge System"
    debug: bool = Field(default=False)
    environment: str = Field(default="development")
    
    # Neo4j settings
    neo4j_uri: Optional[str] = Field(default=None, alias="NEO4J_URI")
    neo4j_user: str = Field(default="neo4j", alias="NEO4J_USERNAME")
    neo4j_password: Optional[str] = Field(default=None, alias="NEO4J_PASSWORD")
    neo4j_database: str = Field(default="neo4j", alias="NEO4J_DATABASE")
    
    # AstraDB settings
    astra_endpoint: Optional[str] = Field(default=None, alias="ASTRA_DB_API_ENDPOINT")
    astra_token: Optional[str] = Field(default=None, alias="ASTRA_DB_APPLICATION_TOKEN")
    astra_collection: str = Field(default="drilling_docs", alias="ASTRA_DB_VECTOR_COLLECTION")
    
    # Add the missing fields that were causing validation errors
    astra_db_vector_dim: Optional[str] = Field(default="1536", alias="ASTRA_DB_VECTOR_DIM")
    astra_use_server_vectorize: Optional[str] = Field(default="true", alias="ASTRA_USE_SERVER_VECTORIZE")
    log_level: Optional[str] = Field(default="INFO", alias="LOG_LEVEL")
    openai_api_key: Optional[str] = Field(default=None, alias="OPENAI_API_KEY")
    
    # WatsonX settings
    wx_api_key: Optional[str] = Field(default=None, alias="WX_API_KEY")
    wx_url: Optional[str] = Field(default=None, alias="WX_URL")
    wx_project_id: Optional[str] = Field(default=None, alias="WX_PROJECT_ID")
    wx_model_id: str = Field(default="meta-llama/llama-3-3-70b-instruct", alias="WX_MODEL_ID")
    
    # Retrieval weights
    graph_weight: float = Field(default=0.7, alias="GRAPH_WEIGHT")
    astra_weight: float = Field(default=0.3, alias="ASTRA_WEIGHT")
    
    # Workflow settings
    max_loops: int = Field(default=5, alias="MAX_LOOPS")
    enable_monitoring: bool = Field(default=True, alias="ENABLE_MONITORING")
    enable_multi_agent: bool = Field(default=False, alias="ENABLE_MULTI_AGENT")
    
    class Config:
        env_file = ".env"
        case_sensitive = False
        populate_by_name = True
        # Add this to allow extra environment variables
        extra = "ignore"  # This will ignore extra env vars instead of rejecting them

# Global settings instance
settings = Settings()