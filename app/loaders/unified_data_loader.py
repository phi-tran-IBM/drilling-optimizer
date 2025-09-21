#!/usr/bin/env python3
"""
Unified Well Data Loader for Knowledge-Driven Well Planning System

This module consolidates all well data sources (BOEM, NPD, NSTA, RRC, USGS) into a single,
robust data loading system with comprehensive error handling, validation, and Neo4j integration.

The loader supports the Enterprise Knowledge Graph architecture described in the project documentation,
enabling structured data ingestion from multiple government and regulatory well databases.

Usage:
    python unified_data_loader.py --sources all --create-indexes --summary
    python unified_data_loader.py --sources boem npd --limit 100
    
Author: Well Planning System
Version: 1.0.0
Dependencies: neo4j>=5.16.0, pandas, requests, python-dotenv
"""

import os
import sys
import argparse
import logging
import traceback
from typing import Dict, List, Optional, Union, Any
from pathlib import Path
from dataclasses import dataclass
from contextlib import contextmanager

import pandas as pd
import requests
from dotenv import load_dotenv
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, AuthError, ConfigurationError

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('well_data_loader.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class DataSourceConfig:
    """Configuration for a well data source."""
    name: str
    source_type: str
    country: str
    state: Optional[str] = None
    api_url: Optional[str] = None
    sample_file: Optional[str] = None
    required_columns: Optional[List[str]] = None
    id_column: str = "well_id"


class Neo4jConnectionManager:
    """
    Manages Neo4j database connections with robust error handling and validation.
    
    This class handles connection lifecycle, authentication, and provides helper methods
    for common database operations used in the well planning knowledge graph.
    """
    
    def __init__(self):
        """Initialize connection manager with environment variables."""
        self.uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self.username = os.getenv("NEO4J_USERNAME", "neo4j")
        self.password = os.getenv("NEO4J_PASSWORD")
        self.database = os.getenv("NEO4J_DATABASE", "neo4j")
        self.driver = None
        
        if not self.password:
            raise ValueError("NEO4J_PASSWORD environment variable is required")
    
    @contextmanager
    def get_driver(self):
        """Context manager for Neo4j driver lifecycle."""
        try:
            self.driver = GraphDatabase.driver(
                self.uri, 
                auth=(self.username, self.password),
                max_connection_lifetime=30 * 60,  # 30 minutes
                max_connection_pool_size=50,
                connection_acquisition_timeout=60  # 60 seconds
            )
            # Verify connectivity
            self.driver.verify_connectivity()
            logger.info(f"Successfully connected to Neo4j at {self.uri}")
            yield self.driver
        except ServiceUnavailable as e:
            logger.error(f"Neo4j service unavailable: {e}")
            raise
        except AuthError as e:
            logger.error(f"Neo4j authentication failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error connecting to Neo4j: {e}")
            raise
        finally:
            if self.driver:
                self.driver.close()
                logger.info("Neo4j connection closed")
    
    def execute_query(self, query: str, parameters: Dict[str, Any] = None) -> List[Dict]:
        """Execute a Cypher query and return results."""
        with self.get_driver() as driver:
            with driver.session(database=self.database) as session:
                result = session.run(query, parameters or {})
                return [record.data() for record in result]
    
    def execute_write_transaction(self, query: str, parameters: Dict[str, Any] = None) -> Dict:
        """Execute a write transaction and return summary."""
        with self.get_driver() as driver:
            with driver.session(database=self.database) as session:
                result = session.run(query, parameters or {})
                summary = result.consume()
                return {
                    "nodes_created": summary.counters.nodes_created,
                    "relationships_created": summary.counters.relationships_created,
                    "properties_set": summary.counters.properties_set
                }


class WellDataValidator:
    """
    Validates well data for consistency and completeness before Neo4j ingestion.
    
    This class implements data quality checks aligned with the Enterprise Knowledge Graph
    ontology defined in the project documentation.
    """
    
    @staticmethod
    def validate_dataframe(df: pd.DataFrame, source_config: DataSourceConfig) -> pd.DataFrame:
        """
        Validate and clean a DataFrame containing well data.
        
        Args:
            df: Raw DataFrame from data source
            source_config: Configuration for the data source
            
        Returns:
            Cleaned and validated DataFrame
            
        Raises:
            ValueError: If data validation fails
        """
        if df.empty:
            logger.warning(f"Empty DataFrame for source {source_config.name} - this may be due to API issues or missing sample files")
            # Return empty DataFrame instead of raising error for better fault tolerance
            return df
        
        # Log original data shape
        logger.info(f"Validating {len(df)} records from {source_config.name}")
        
        # Remove completely empty rows
        initial_count = len(df)
        df = df.dropna(how='all')
        if len(df) < initial_count:
            logger.warning(f"Removed {initial_count - len(df)} completely empty rows")
        
        # Validate required columns if specified
        if source_config.required_columns:
            missing_cols = set(source_config.required_columns) - set(df.columns)
            if missing_cols:
                logger.warning(f"Missing columns in {source_config.name}: {missing_cols}")
        
        # Clean and standardize data
        df = WellDataValidator._clean_dataframe(df, source_config)
        
        logger.info(f"Validation complete: {len(df)} valid records for {source_config.name}")
        return df
    
    @staticmethod
    def _clean_dataframe(df: pd.DataFrame, source_config: DataSourceConfig) -> pd.DataFrame:
        """Apply data cleaning transformations."""
        # Strip whitespace from string columns
        string_cols = df.select_dtypes(include=['object']).columns
        df[string_cols] = df[string_cols].apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        
        # Handle numeric columns
        numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
        for col in numeric_cols:
            # Replace infinite values with NaN
            df[col] = df[col].replace([float('inf'), float('-inf')], pd.NA)
        
        # Ensure well_id exists and is valid
        if source_config.id_column not in df.columns:
            # Try to create well_id from available columns
            df = WellDataValidator._create_well_id(df, source_config)
        
        # Remove rows without valid well_id
        initial_count = len(df)
        df = df.dropna(subset=[source_config.id_column])
        if len(df) < initial_count:
            logger.warning(f"Removed {initial_count - len(df)} rows without valid {source_config.id_column}")
        
        return df
    
    @staticmethod
    def _create_well_id(df: pd.DataFrame, source_config: DataSourceConfig) -> pd.DataFrame:
        """Create well_id from available identifier columns."""
        possible_id_cols = [
            'API', 'API_NUMBER', 'WELL_API', 'WELLBORE', 'WELL_ID', 
            'WELL_WONS', 'OBJECTID', 'WELL_NAME', 'NAME'
        ]
        
        for col in possible_id_cols:
            if col in df.columns:
                df['well_id'] = df[col].astype(str)
                logger.info(f"Created well_id from {col} for {source_config.name}")
                return df
        
        # If no suitable column found, create from index
        df['well_id'] = f"{source_config.name}_" + df.index.astype(str)
        logger.warning(f"Created well_id from index for {source_config.name}")
        return df


class UnifiedWellDataLoader:
    """
    Unified data loader for multiple well data sources supporting the Knowledge-Driven
    Well Planning System architecture.
    
    This loader consolidates BOEM, NPD, NSTA, RRC, and USGS data sources into a single
    Enterprise Knowledge Graph stored in Neo4j, enabling advanced GraphRAG capabilities.
    """
    
    def __init__(self):
        """Initialize the unified data loader."""
        self.neo4j_manager = Neo4jConnectionManager()
        self.validator = WellDataValidator()
        self.data_sources = self._configure_data_sources()
        
    def _configure_data_sources(self) -> Dict[str, DataSourceConfig]:
        """Configure all supported data sources."""
        return {
            'boem': DataSourceConfig(
                name='BOEM',
                source_type='offshore_wells',
                country='US',
                sample_file='data/external_samples/boem_offshore_wells_sample.csv',
                required_columns=['API', 'STATUS', 'FIELD_NAME', 'LAT', 'LON'],
                id_column='well_id'
            ),
            'npd': DataSourceConfig(
                name='NPD',
                source_type='wellbores',
                country='NO',
                sample_file='data/external_samples/npd_wellbores_sample.csv',
                required_columns=['WELLBORE', 'STATUS', 'FIELD', 'LAT', 'LON'],
                id_column='well_id'
            ),
            'nsta': DataSourceConfig(
                name='NSTA',
                source_type='offshore_wells',
                country='UK',
                api_url=os.getenv("NSTA_WELLS_URL", 
                    "https://services9.arcgis.com/8pcKnVYHe23zA6C4/arcgis/rest/services/Offshore_Wells_WGS84/FeatureServer/0/query"),
                required_columns=['WELL_ID', 'STATUS', 'YEAR', 'LATITUDE', 'LONGITUDE'],
                id_column='well_id'
            ),
            'rrc': DataSourceConfig(
                name='RRC',
                source_type='texas_wells',
                country='US',
                state='TX',
                sample_file='data/external_samples/rrc_texas_wells_sample.csv',  # Add sample file path
                required_columns=['API_NUMBER', 'OPERATOR', 'COUNTY'],
                id_column='well_id'
            ),
            'usgs': DataSourceConfig(
                name='USGS',
                source_type='aggregated_grid',
                country='US',
                sample_file='data/external_samples/usgs_drilling_history_sample.csv',
                required_columns=['grid_id', 'total_wells'],
                id_column='grid_id'
            )
        }
    
    def load_all_sources(self, limit_per_source: Optional[int] = None) -> Dict[str, int]:
        """
        Load data from all configured sources into Neo4j.
        
        Args:
            limit_per_source: Optional limit on records per source
            
        Returns:
            Dictionary with source names and record counts loaded
        """
        results = {}
        
        for source_name, config in self.data_sources.items():
            try:
                logger.info(f"Starting load for {source_name}")
                count = self.load_source(source_name, limit=limit_per_source)
                results[source_name] = count
                logger.info(f"Successfully loaded {count} records from {source_name}")
            except Exception as e:
                logger.error(f"Failed to load {source_name}: {e}")
                logger.error(traceback.format_exc())
                results[source_name] = 0
        
        # Log summary
        total_records = sum(results.values())
        logger.info(f"Data loading complete. Total records loaded: {total_records}")
        for source, count in results.items():
            logger.info(f"  {source}: {count} records")
        
        return results
    
    def load_source(self, source_name: str, limit: Optional[int] = None) -> int:
        """
        Load data from a specific source.
        
        Args:
            source_name: Name of the source to load
            limit: Optional limit on number of records
            
        Returns:
            Number of records loaded
        """
        if source_name not in self.data_sources:
            raise ValueError(f"Unknown source: {source_name}")
        
        config = self.data_sources[source_name]
        
        # Fetch data based on source type
        if source_name == 'nsta':
            df = self._fetch_nsta_data(config, limit)
        else:
            df = self._load_csv_data(config, limit)
        
        # Validate and clean data
        df = self.validator.validate_dataframe(df, config)
        
        # Load into Neo4j
        return self._load_to_neo4j(df, config)
    
    def _fetch_nsta_data(self, config: DataSourceConfig, limit: Optional[int] = None) -> pd.DataFrame:
        """Fetch NSTA data from API with enhanced error handling."""
        try:
            params = {
                "where": "1=1",
                "outFields": "*",
                "f": "json",
                "resultRecordCount": limit or 50
            }
            
            logger.info(f"Fetching NSTA data from: {config.api_url}")
            response = requests.get(config.api_url, params=params, timeout=60)
            response.raise_for_status()
            
            data = response.json()
            features = data.get("features", [])
            
            if not features:
                logger.warning("No features returned from NSTA API - this may be due to API changes or restrictions")
                # Return empty DataFrame instead of raising error
                return pd.DataFrame()
            
            rows = [feature["attributes"] for feature in features]
            df = pd.DataFrame(rows)
            
            logger.info(f"Fetched {len(df)} records from NSTA API")
            return df
            
        except requests.RequestException as e:
            logger.error(f"Failed to fetch NSTA data: {e}")
            logger.warning("Returning empty DataFrame for NSTA - API may be unavailable")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error processing NSTA data: {e}")
            logger.warning("Returning empty DataFrame for NSTA")
            return pd.DataFrame()
    
    def _load_csv_data(self, config: DataSourceConfig, limit: Optional[int] = None) -> pd.DataFrame:
        """Load data from CSV file."""
        if not config.sample_file:
            raise ValueError(f"No sample file configured for {config.name}")
        
        file_path = Path(config.sample_file)
        if not file_path.exists():
            raise FileNotFoundError(f"Sample file not found: {file_path}")
        
        try:
            df = pd.read_csv(file_path)
            
            if limit:
                df = df.head(limit)
            
            logger.info(f"Loaded {len(df)} records from {file_path}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to load CSV {file_path}: {e}")
            raise
    
    def _load_to_neo4j(self, df: pd.DataFrame, config: DataSourceConfig) -> int:
        """Load DataFrame into Neo4j using appropriate Cypher query."""
        if df.empty:
            logger.warning(f"No data to load for {config.name}")
            return 0
        
        # Convert DataFrame to records for Neo4j
        records = df.to_dict("records")
        
        # Choose appropriate Cypher query based on source type
        if config.name == 'USGS':
            query = self._get_usgs_cypher_query()
        else:
            query = self._get_well_cypher_query(config)
        
        try:
            result = self.neo4j_manager.execute_write_transaction(query, {"rows": records})
            
            nodes_created = result.get("nodes_created", 0)
            properties_set = result.get("properties_set", 0)
            
            logger.info(f"Neo4j load result for {config.name}: "
                       f"{nodes_created} nodes created, {properties_set} properties set")
            
            return nodes_created
            
        except Exception as e:
            logger.error(f"Failed to load {config.name} data to Neo4j: {e}")
            raise
    
    def _get_well_cypher_query(self, config: DataSourceConfig) -> str:
        """Generate Cypher query for well data based on source configuration."""
        base_query = """
        UNWIND $rows AS r
        MERGE (w:Well {well_id: coalesce(
            r.API, r.API_NUMBER, r.WELL_API, r.WELLBORE, r.WELL_ID, 
            r.WELL_WONS, toString(r.OBJECTID), r.WELL_NAME, r.NAME, r.well_id
        )})
        SET w.country = $country,
            w.source = $source,
            w.last_updated = datetime()
        """
        
        # Add source-specific properties using standard Cypher (no APOC dependency)
        property_mappings = {
            'BOEM': """
                SET w.status = r.STATUS,
                    w.field = r.FIELD_NAME,
                    w.latitude = toFloat(r.LAT),
                    w.longitude = toFloat(r.LON),
                    w.location_text = toString(r.LAT) + ',' + toString(r.LON)
            """,
            'NPD': """
                SET w.status = r.STATUS,
                    w.field = r.FIELD,
                    w.latitude = toFloat(r.LAT),
                    w.longitude = toFloat(r.LON),
                    w.location_text = toString(r.LAT) + ',' + toString(r.LON)
            """,
            'NSTA': """
                SET w.status = r.STATUS,
                    w.year = toInteger(r.YEAR),
                    w.kb_elevation = toFloat(r.KB_ELEVATION),
                    w.latitude = toFloat(r.LATITUDE),
                    w.longitude = toFloat(r.LONGITUDE),
                    w.location_text = toString(r.LATITUDE) + ',' + toString(r.LONGITUDE)
            """,
            'RRC': """
                SET w.state = 'TX',
                    w.operator = coalesce(r.OPERATOR, r.Operator, r.OPERATOR_NAME),
                    w.county = coalesce(r.COUNTY, r.COUNTY_NAME),
                    w.field = coalesce(r.FIELD, r.Field),
                    w.location_text = coalesce(r.SURVEY, r.SURF_LOCATION, r.LOCATION)
            """
        }
        
        query = base_query.replace("$country", f"'{config.country}'")
        query = query.replace("$source", f"'{config.name}'")
        query += property_mappings.get(config.name, "")
        
        return query
    
    def _get_usgs_cypher_query(self) -> str:
        """Generate Cypher query for USGS aggregated grid data."""
        return """
        UNWIND $rows AS r
        MERGE (g:RegionGrid {grid_id: r.grid_id})
        SET g.source = 'USGS',
            g.total_wells = toInteger(r.total_wells),
            g.oil_wells = toInteger(r.oil),
            g.gas_wells = toInteger(r.gas),
            g.horizontal_wells = toInteger(r.horizontal),
            g.fractured_wells = toInteger(r.fractured),
            g.last_updated = datetime()
        """
    
    def create_knowledge_graph_indexes(self):
        """Create indexes and constraints for the Enterprise Knowledge Graph."""
        index_queries = [
            # Well node indexes
            "CREATE CONSTRAINT well_id_unique IF NOT EXISTS FOR (w:Well) REQUIRE w.well_id IS UNIQUE",
            "CREATE INDEX well_country IF NOT EXISTS FOR (w:Well) ON (w.country)",
            "CREATE INDEX well_source IF NOT EXISTS FOR (w:Well) ON (w.source)",
            "CREATE INDEX well_status IF NOT EXISTS FOR (w:Well) ON (w.status)",
            "CREATE INDEX well_latitude IF NOT EXISTS FOR (w:Well) ON (w.latitude)",
            "CREATE INDEX well_longitude IF NOT EXISTS FOR (w:Well) ON (w.longitude)",
            
            # RegionGrid indexes
            "CREATE CONSTRAINT grid_id_unique IF NOT EXISTS FOR (g:RegionGrid) REQUIRE g.grid_id IS UNIQUE",
            "CREATE INDEX grid_source IF NOT EXISTS FOR (g:RegionGrid) ON (g.source)",
            
            # Prepare for future knowledge graph expansion (these will be created when those nodes exist)
            "CREATE INDEX formation_name IF NOT EXISTS FOR (f:Formation) ON (f.name)",
            "CREATE INDEX bha_tool_type IF NOT EXISTS FOR (t:BHATool) ON (t.tool_type)",
            "CREATE INDEX constraint_type IF NOT EXISTS FOR (c:EngineeringConstraint) ON (c.constraint_type)"
        ]
        
        for query in index_queries:
            try:
                self.neo4j_manager.execute_write_transaction(query)
                logger.info(f"Created index/constraint successfully")
            except Exception as e:
                # Some indexes may fail if the node types don't exist yet - this is expected
                if "No such label" in str(e) or "Label" in str(e):
                    logger.debug(f"Skipped index creation (label doesn't exist yet): {e}")
                else:
                    logger.warning(f"Failed to create index/constraint: {e}")
    
    def get_data_summary(self) -> Dict[str, Any]:
        """Get summary of loaded data from Neo4j."""
        summary_query = """
        MATCH (w:Well)
        RETURN w.source as source, w.country as country, count(*) as well_count
        ORDER BY source
        """
        
        grid_query = """
        MATCH (g:RegionGrid)
        RETURN count(*) as grid_count, sum(g.total_wells) as total_wells_in_grids
        """
        
        try:
            well_summary = self.neo4j_manager.execute_query(summary_query)
            grid_summary = self.neo4j_manager.execute_query(grid_query)
            
            return {
                "well_data_by_source": well_summary,
                "grid_data": grid_summary[0] if grid_summary else {},
                "timestamp": pd.Timestamp.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Failed to get data summary: {e}")
            return {"error": str(e)}


def main():
    """Main function for command-line usage."""
    parser = argparse.ArgumentParser(
        description="Unified Well Data Loader for Knowledge-Driven Well Planning System"
    )
    parser.add_argument(
        "--sources", 
        nargs="+", 
        choices=["boem", "npd", "nsta", "rrc", "usgs", "all"],
        default=["all"],
        help="Data sources to load (default: all)"
    )
    parser.add_argument(
        "--limit", 
        type=int, 
        help="Limit number of records per source"
    )
    parser.add_argument(
        "--create-indexes", 
        action="store_true",
        help="Create Neo4j indexes and constraints"
    )
    parser.add_argument(
        "--summary", 
        action="store_true",
        help="Show data summary after loading"
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Set logging level"
    )
    
    args = parser.parse_args()
    
    # Set logging level
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    try:
        # Initialize loader
        loader = UnifiedWellDataLoader()
        
        # Create indexes if requested
        if args.create_indexes:
            logger.info("Creating Neo4j indexes and constraints...")
            loader.create_knowledge_graph_indexes()
        
        # Load data
        if "all" in args.sources:
            results = loader.load_all_sources(limit_per_source=args.limit)
        else:
            results = {}
            for source in args.sources:
                count = loader.load_source(source, limit=args.limit)
                results[source] = count
        
        # Show summary if requested
        if args.summary:
            logger.info("Generating data summary...")
            summary = loader.get_data_summary()
            print(f"\nData Summary:\n{pd.Series(summary).to_string()}")
        
        # Final results
        total_loaded = sum(results.values())
        logger.info(f"Data loading completed successfully. Total records: {total_loaded}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Data loading failed: {e}")
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())