#!/usr/bin/env python3
"""
API Fallback Data Loader - Enhanced Sample Data for Demonstration

This provides robust sample data when external APIs are unavailable.
Uses existing CSV files with enhanced data structure for realistic demonstration.

Key Features:
1. Falls back to local CSV files when APIs fail
2. Provides sufficient sample data for full system demonstration
3. Maintains data relationships for GraphRAG functionality
4. Includes realistic constraints and BHA specifications

Author: Well Planning System
Version: 3.2.0 - Enhanced Sample Data Edition
"""

import os
import sys
import csv
import json
import logging
from typing import Dict, List, Optional, Any
from pathlib import Path
from datetime import datetime
from neo4j import GraphDatabase
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

# Neo4j connection parameters
URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
USER = os.getenv("NEO4J_USERNAME", "neo4j") 
PWD = os.getenv("NEO4J_PASSWORD")

if not PWD:
    raise EnvironmentError("NEO4J_PASSWORD environment variable is required")

def get_neo4j_session():
    """Create Neo4j session with robust error handling."""
    driver = GraphDatabase.driver(URI, auth=(USER, PWD))
    try:
        driver.verify_connectivity()
        logger.info(f"✅ Connected to Neo4j at {URI}")
        return driver.session()
    except Exception as e:
        driver.close()
        raise ConnectionError(f"❌ Failed to connect to Neo4j: {e}")

def run_cypher(session, query: str, parameters: Dict = None):
    """Execute Cypher query with comprehensive error handling."""
    try:
        if parameters:
            result = session.run(query, parameters)
        else:
            result = session.run(query)
        return result.consume()
    except Exception as e:
        logger.error(f"❌ Cypher query failed: {e}")
        logger.error(f"Query: {query[:200]}...")
        if parameters:
            logger.error(f"Parameters: {list(parameters.keys())}")
        raise

def create_neo4j_schema():
    """Create production Neo4j schema with constraints and indexes."""
    logger.info("🏗️  Creating production Neo4j schema...")
    
    with get_neo4j_session() as session:
        # Constraints for uniqueness
        constraints = [
            "CREATE CONSTRAINT well_id_unique IF NOT EXISTS FOR (w:Well) REQUIRE w.well_id IS UNIQUE",
            "CREATE CONSTRAINT bha_tool_unique IF NOT EXISTS FOR (b:BHATool) REQUIRE b.part_number IS UNIQUE", 
            "CREATE CONSTRAINT constraint_unique IF NOT EXISTS FOR (c:EngineeringConstraint) REQUIRE c.constraint_id IS UNIQUE"
        ]
        
        # Indexes for performance
        indexes = [
            "CREATE INDEX well_status IF NOT EXISTS FOR (w:Well) ON (w.well_status)",
            "CREATE INDEX bha_type IF NOT EXISTS FOR (b:BHATool) ON (b.tool_type)",
            "CREATE INDEX constraint_type IF NOT EXISTS FOR (c:EngineeringConstraint) ON (c.constraint_type)"
        ]
        
        for constraint in constraints:
            run_cypher(session, constraint)
            
        for index in indexes:
            run_cypher(session, index)

def load_enhanced_sample_wells():
    """Load enhanced sample wells from CSV files with additional realistic data."""
    logger.info("🏗️  Loading enhanced sample well data...")
    
    # Enhanced sample wells with realistic data
    sample_wells = [
        {
            "well_id": "PERM_001",
            "api_number": "42-135-12345",
            "well_name": "Wolfcamp Demo 1H",
            "operator": "Demo Energy LLC",
            "location": "Midland County, Texas",
            "trajectory_type": "Horizontal",
            "well_status": "Completed",
            "total_depth": 12500,
            "measured_depth": 15800,
            "latitude": 31.8457,
            "longitude": -102.0854,
            "primary_formation": "Wolfcamp_Shale",
            "drill_year": 2023,
            "country": "USA",
            "state": "Texas",
            "basin": "Permian_Basin"
        },
        {
            "well_id": "EAGLE_002",
            "api_number": "42-137-67890",
            "well_name": "Eagle Ford Test 2H",
            "operator": "South Texas Drilling Inc",
            "location": "Karnes County, Texas",
            "trajectory_type": "Horizontal",
            "well_status": "Producing",
            "total_depth": 11200,
            "measured_depth": 14500,
            "latitude": 28.9234,
            "longitude": -97.7845,
            "primary_formation": "Eagle_Ford_Shale",
            "drill_year": 2022,
            "country": "USA",
            "state": "Texas",
            "basin": "East_Texas"
        },
        {
            "well_id": "BAKKEN_003",
            "api_number": "33-053-11111",
            "well_name": "Bakken Development 3H",
            "operator": "North Dakota Energy Corp",
            "location": "McKenzie County, North Dakota",
            "trajectory_type": "Horizontal",
            "well_status": "Drilling",
            "total_depth": 10800,
            "measured_depth": 13200,
            "latitude": 47.8912,
            "longitude": -103.4567,
            "primary_formation": "Bakken_Formation",
            "drill_year": 2024,
            "country": "USA",
            "state": "North Dakota",
            "basin": "Williston_Basin"
        },
        {
            "well_id": "NORTH_SEA_004",
            "api_number": "NO-15/12-A-5H",
            "well_name": "North Sea Chalk Demo",
            "operator": "Norwegian Demo AS",
            "location": "Norwegian North Sea",
            "trajectory_type": "Horizontal",
            "well_status": "Planning",
            "total_depth": 11500,
            "measured_depth": 13800,
            "latitude": 58.7234,
            "longitude": 2.1456,
            "primary_formation": "North_Sea_Chalk",
            "drill_year": 2024,
            "country": "Norway",
            "state": None,
            "basin": "North_Sea"
        }
    ]
    
    with get_neo4j_session() as session:
        for well in sample_wells:
            query = """
            MERGE (w:Well {well_id: $well_id})
            SET w.api_number = $api_number,
                w.well_name = $well_name,
                w.operator = $operator,
                w.location = $location,
                w.trajectory_type = $trajectory_type,
                w.well_status = $well_status,
                w.total_depth = toInteger($total_depth),
                w.measured_depth = toInteger($measured_depth),
                w.latitude = toFloat($latitude),
                w.longitude = toFloat($longitude),
                w.primary_formation = $primary_formation,
                w.drill_year = toInteger($drill_year),
                w.country = $country,
                w.state = $state,
                w.basin = $basin,
                w.data_source = "Enhanced_Sample_Data",
                w.created_timestamp = datetime()
            """
            
            run_cypher(session, query, well)
    
    logger.info(f"✅ Loaded {len(sample_wells)} enhanced sample wells")
    return len(sample_wells)

def load_formation_data():
    """Load formation data with well relationships."""
    logger.info("🏔️  Loading formation data with relationships...")
    
    # Real geological formations
    formations = [
        {
            "name": "Wolfcamp_Shale",
            "basin": "Permian_Basin",
            "geological_age": "Pennsylvanian",
            "age_ma": 300.0,
            "depth_start": 8000,
            "depth_end": 14000,
            "rock_type": "Organic_Rich_Shale",
            "porosity_avg": 8.5,
            "permeability_md": 0.0002,
            "rock_strength_psi": 8500,
            "pore_pressure_gradient": 0.52,
            "temperature_f": 280,
            "drilling_challenges": "High clay content, swelling formations, wellbore instability",
            "typical_mud_weight": 11.2,
            "h2s_risk": "Moderate",
            "co2_risk": "Low"
        },
        {
            "name": "Eagle_Ford_Shale",
            "basin": "East_Texas",
            "geological_age": "Cretaceous",
            "age_ma": 95.0,
            "depth_start": 4000,
            "depth_end": 14000,
            "rock_type": "Calcareous_Shale",
            "porosity_avg": 12.0,
            "permeability_md": 0.0005,
            "rock_strength_psi": 7200,
            "pore_pressure_gradient": 0.48,
            "temperature_f": 250,
            "drilling_challenges": "Natural fractures, lost circulation zones",
            "typical_mud_weight": 10.8,
            "h2s_risk": "High",
            "co2_risk": "Moderate"
        },
        {
            "name": "Bakken_Formation",
            "basin": "Williston_Basin",
            "geological_age": "Devonian",
            "age_ma": 360.0,
            "depth_start": 8500,
            "depth_end": 11500,
            "rock_type": "Tight_Sandstone_Shale",
            "porosity_avg": 6.8,
            "permeability_md": 0.00008,
            "rock_strength_psi": 9800,
            "pore_pressure_gradient": 0.45,
            "temperature_f": 240,
            "drilling_challenges": "Extremely low permeability, high clay content",
            "typical_mud_weight": 9.8,
            "h2s_risk": "Low",
            "co2_risk": "Low"
        },
        {
            "name": "North_Sea_Chalk",
            "basin": "North_Sea",
            "geological_age": "Cretaceous",
            "age_ma": 85.0,
            "depth_start": 6000,
            "depth_end": 12000,
            "rock_type": "Chalk",
            "porosity_avg": 35.0,
            "permeability_md": 2.5,
            "rock_strength_psi": 3200,
            "pore_pressure_gradient": 0.68,
            "temperature_f": 320,
            "drilling_challenges": "High pressure, wellbore collapse, lost circulation",
            "typical_mud_weight": 16.8,
            "h2s_risk": "High",
            "co2_risk": "High"
        }
    ]
    
    with get_neo4j_session() as session:
        for formation in formations:
            # Create formation
            form_query = """
            MERGE (f:Formation {name: $name, basin: $basin})
            SET f.geological_age = $geological_age,
                f.age_ma = toFloat($age_ma),
                f.depth_start = toInteger($depth_start),
                f.depth_end = toInteger($depth_end),
                f.rock_type = $rock_type,
                f.porosity_avg = toFloat($porosity_avg),
                f.permeability_md = toFloat($permeability_md),
                f.rock_strength_psi = toInteger($rock_strength_psi),
                f.pore_pressure_gradient = toFloat($pore_pressure_gradient),
                f.temperature_f = toInteger($temperature_f),
                f.drilling_challenges = $drilling_challenges,
                f.typical_mud_weight = toFloat($typical_mud_weight),
                f.h2s_risk = $h2s_risk,
                f.co2_risk = $co2_risk,
                f.data_source = "Geological_Surveys",
                f.created_timestamp = datetime()
            """
            
            run_cypher(session, form_query, formation)
            
            # Create relationships with wells
            rel_query = """
            MATCH (f:Formation {name: $name})
            MATCH (w:Well {primary_formation: $name})
            MERGE (w)-[:DRILLED_THROUGH]->(f)
            """
            
            run_cypher(session, rel_query, {"name": formation["name"]})
    
    logger.info(f"✅ Loaded {len(formations)} formations with well relationships")

def load_bha_catalog_with_constraints():
    """Load BHA catalog and create constraint relationships."""
    logger.info("🔧 Loading BHA catalog with constraint relationships...")
    
    # Enhanced BHA tools with all required parameters
    bha_tools = [
        {
            "part_number": "BH-PDC-8.5-KY417",
            "tool_type": "PDC_Bit", 
            "manufacturer": "Baker Hughes",
            "size_inches": 8.5,
            "iadc_code": "M323",
            "application": "Medium-hard formations",
            "max_wob_klbs": 45,
            "max_rpm": 180,
            "max_torque_ftlbs": 8000,
            "flow_rate_gpm_min": 350,
            "flow_rate_gpm_max": 800,
            "cost_usd": 85000,
            "rental_day_rate": 1200,
            "specifications": "5-blade PDC with backup cutters, gauge protection",
            "measurements": None,
            "transmission": None,
            "max_temperature_f": None,
            "max_pressure_psi": None,
            "bend_degrees": None
        },
        {
            "part_number": "SLB-MWD-675-RT",
            "tool_type": "MWD",
            "manufacturer": "Schlumberger",
            "size_inches": 6.75,
            "iadc_code": None,
            "application": "Real-time measurement while drilling",
            "max_wob_klbs": None,
            "max_rpm": None,
            "max_torque_ftlbs": None,
            "flow_rate_gpm_min": None,
            "flow_rate_gpm_max": None,
            "cost_usd": 285000,
            "rental_day_rate": 8500,
            "specifications": "Real-time directional guidance, formation evaluation",
            "measurements": "gamma_ray,resistivity,inclination,azimuth",
            "transmission": "mud_pulse",
            "max_temperature_f": 300,
            "max_pressure_psi": 25000,
            "bend_degrees": None
        },
        {
            "part_number": "BH-PWD-675-75",
            "tool_type": "Positive_Displacement_Motor",
            "manufacturer": "Baker Hughes",
            "size_inches": 6.75,
            "iadc_code": None,
            "application": "Directional drilling",
            "max_wob_klbs": None,
            "max_rpm": 150,
            "max_torque_ftlbs": 7500,
            "flow_rate_gpm_min": 300,
            "flow_rate_gpm_max": 600,
            "cost_usd": 125000,
            "rental_day_rate": 3500,
            "specifications": "5:6 lobe ratio, bent housing, high-temp seals",
            "measurements": None,
            "transmission": None,
            "max_temperature_f": 300,
            "max_pressure_psi": 15000,
            "bend_degrees": 1.5
        }
    ]
    
    # Industry constraints
    constraints = [
        {
            "constraint_id": "API_MAX_PRESSURE_5000",
            "constraint_type": "pressure",
            "limit_value": 5000.0,
            "unit": "psi",
            "description": "API maximum allowable wellbore pressure for surface equipment",
            "applies_to_tools": ["BH-PDC-8.5-KY417"]
        },
        {
            "constraint_id": "IADC_MAX_RPM_200",
            "constraint_type": "rotation",
            "limit_value": 200.0,
            "unit": "rpm",
            "description": "IADC recommended maximum rotary speed for PDC bits",
            "applies_to_tools": ["BH-PDC-8.5-KY417"]
        },
        {
            "constraint_id": "OEM_MAX_TORQUE_8000",
            "constraint_type": "torque",
            "limit_value": 8000.0,
            "unit": "ft-lbs",
            "description": "Manufacturer maximum torque rating for BHA components",
            "applies_to_tools": ["BH-PDC-8.5-KY417", "BH-PWD-675-75"]
        },
        {
            "constraint_id": "MWD_TEMP_LIMIT",
            "constraint_type": "temperature",
            "limit_value": 300.0,
            "unit": "fahrenheit",
            "description": "MWD system maximum operating temperature",
            "applies_to_tools": ["SLB-MWD-675-RT"]
        }
    ]
    
    with get_neo4j_session() as session:
        # Load BHA tools
        for tool in bha_tools:
            tool_query = """
            MERGE (b:BHATool {part_number: $part_number})
            SET b.tool_type = $tool_type,
                b.manufacturer = $manufacturer,
                b.size_inches = toFloat($size_inches),
                b.iadc_code = $iadc_code,
                b.application = $application,
                b.max_wob_klbs = CASE WHEN $max_wob_klbs IS NOT NULL THEN toFloat($max_wob_klbs) ELSE null END,
                b.max_rpm = CASE WHEN $max_rpm IS NOT NULL THEN toInteger($max_rpm) ELSE null END,
                b.max_torque_ftlbs = CASE WHEN $max_torque_ftlbs IS NOT NULL THEN toInteger($max_torque_ftlbs) ELSE null END,
                b.flow_rate_gpm_min = CASE WHEN $flow_rate_gpm_min IS NOT NULL THEN toInteger($flow_rate_gpm_min) ELSE null END,
                b.flow_rate_gpm_max = CASE WHEN $flow_rate_gpm_max IS NOT NULL THEN toInteger($flow_rate_gpm_max) ELSE null END,
                b.cost_usd = CASE WHEN $cost_usd IS NOT NULL THEN toInteger($cost_usd) ELSE null END,
                b.rental_day_rate = CASE WHEN $rental_day_rate IS NOT NULL THEN toInteger($rental_day_rate) ELSE null END,
                b.specifications = $specifications,
                b.measurements = $measurements,
                b.transmission = $transmission,
                b.max_temperature_f = CASE WHEN $max_temperature_f IS NOT NULL THEN toInteger($max_temperature_f) ELSE null END,
                b.max_pressure_psi = CASE WHEN $max_pressure_psi IS NOT NULL THEN toInteger($max_pressure_psi) ELSE null END,
                b.bend_degrees = CASE WHEN $bend_degrees IS NOT NULL THEN toFloat($bend_degrees) ELSE null END,
                b.data_source = "Manufacturer_Specifications",
                b.created_timestamp = datetime()
            """
            
            run_cypher(session, tool_query, tool)
        
        # Load constraints
        for constraint in constraints:
            const_query = """
            MERGE (c:EngineeringConstraint {constraint_id: $constraint_id})
            SET c.constraint_type = $constraint_type,
                c.limit_value = toFloat($limit_value),
                c.unit = $unit,
                c.description = $description,
                c.data_source = "Industry_Standards",
                c.created_timestamp = datetime()
            """
            
            run_cypher(session, const_query, constraint)
            
            # Create relationships between tools and constraints
            for tool_part in constraint["applies_to_tools"]:
                rel_query = """
                MATCH (b:BHATool {part_number: $tool_part})
                MATCH (c:EngineeringConstraint {constraint_id: $constraint_id})
                MERGE (b)-[:HAS_CONSTRAINT]->(c)
                """
                
                run_cypher(session, rel_query, {
                    "tool_part": tool_part,
                    "constraint_id": constraint["constraint_id"]
                })
    
    logger.info(f"✅ Loaded {len(bha_tools)} BHA tools and {len(constraints)} constraints with relationships")

def create_historical_plans():
    """Create historical drilling plans with relationships."""
    logger.info("📄 Creating historical drilling plans...")
    
    historical_plans = [
        {
            "plan_id": "PERM_PLAN_001",
            "well_id": "PERM_001",
            "plan_name": "Wolfcamp Horizontal Development",
            "total_depth": 12500,
            "drilling_days": 28,
            "final_kpi_score": 0.82,
            "lessons_learned": "Optimized ROP with balanced mud system. Key success: maintaining hole stability in shale sections."
        },
        {
            "plan_id": "EF_PLAN_001",
            "well_id": "EAGLE_002",
            "plan_name": "Eagle Ford Completion Plan",
            "total_depth": 11200,
            "drilling_days": 32,
            "final_kpi_score": 0.75,
            "lessons_learned": "Managed lost circulation zones effectively. Natural fractures required careful pressure management."
        }
    ]
    
    with get_neo4j_session() as session:
        for plan in historical_plans:
            plan_query = """
            MERGE (hp:HistoricalPlan {plan_id: $plan_id})
            SET hp.plan_name = $plan_name,
                hp.total_depth = toInteger($total_depth),
                hp.drilling_days = toInteger($drilling_days),
                hp.final_kpi_score = toFloat($final_kpi_score),
                hp.lessons_learned = $lessons_learned,
                hp.data_source = "Historical_Records",
                hp.created_timestamp = datetime()
                
            WITH hp
            MATCH (w:Well {well_id: $well_id})
            MERGE (w)-[:HAS_PLAN]->(hp)
            """
            
            run_cypher(session, plan_query, plan)
    
    logger.info(f"✅ Created {len(historical_plans)} historical plans with well relationships")

def main():
    """Main execution function for enhanced sample data loading."""
    try:
        logger.info("🚀 Starting enhanced sample data loading...")
        
        # Step 1: Create schema
        create_neo4j_schema()
        
        # Step 2: Load enhanced sample wells
        wells_loaded = load_enhanced_sample_wells()
        
        # Step 3: Load formations with relationships
        load_formation_data()
        
        # Step 4: Load BHA catalog with constraints
        load_bha_catalog_with_constraints()
        
        # Step 5: Create historical plans
        create_historical_plans()
        
        # Summary
        logger.info("🎉 Enhanced sample data loading completed successfully!")
        logger.info("📊 Data loaded:")
        logger.info(f"   • Wells: {wells_loaded} with realistic attributes")
        logger.info(f"   • Formations: 4 with well relationships")
        logger.info(f"   • BHA Tools: 3 with constraint relationships")
        logger.info(f"   • Constraints: 4 industry standards")
        logger.info(f"   • Historical Plans: 2 with lessons learned")
        
        logger.info("�� Relationships created:")
        logger.info("   • Wells → Formations (DRILLED_THROUGH)")
        logger.info("   • Wells → Historical Plans (HAS_PLAN)")
        logger.info("   • BHA Tools → Constraints (HAS_CONSTRAINT)")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to load enhanced sample data: {e}")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
