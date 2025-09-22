import os
import json
import re
from typing import List, Dict, Any
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

# Environment validation - fail fast if missing required variables
def validate_environment():
    """Validate all required environment variables are present."""
    required_vars = [
        "NEO4J_URI", "NEO4J_USERNAME", "NEO4J_PASSWORD",
        "ASTRA_DB_API_ENDPOINT", "ASTRA_DB_APPLICATION_TOKEN", 
        "ASTRA_DB_VECTOR_COLLECTION"
    ]
    
    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        raise EnvironmentError(f"Missing required environment variables: {missing}")

# CRITICAL: Call validation immediately on module import - THIS LINE WAS MISSING
validate_environment()

# Now safe to get environment variables since validation passed
URI = os.getenv("NEO4J_URI")
USER = os.getenv("NEO4J_USERNAME") 
PWD = os.getenv("NEO4J_PASSWORD")

# Global driver instance
_driver = None

def _get_driver():
    """Get Neo4j driver instance."""
    global _driver
    if _driver is None:
        _driver = GraphDatabase.driver(URI, auth=(USER, PWD))
    return _driver

def _session():
    """Get Neo4j session."""
    return _get_driver().session()

def close_driver():
    """Call this when shutting down the application"""
    global _driver
    if _driver:
        _driver.close()
        _driver = None

def _astra_snippets(keywords: str, k: int = 3) -> List[Dict[str, str]]:
    """Retrieve k short snippets from AstraDB using vectorize - STRICT MODE."""
    from astrapy import DataAPIClient
    
    # Get validated environment variables
    endpoint = os.getenv("ASTRA_DB_API_ENDPOINT")
    token = os.getenv("ASTRA_DB_APPLICATION_TOKEN")
    collection_name = os.getenv("ASTRA_DB_VECTOR_COLLECTION")
    
    # Validate server-side vectorize is enabled
    vectorize_enabled = os.getenv("ASTRA_USE_SERVER_VECTORIZE", "false").lower()
    if vectorize_enabled not in ["true", "1", "yes"]:
        raise EnvironmentError(
            "Server-side vectorize is required but not enabled. "
            "Set ASTRA_USE_SERVER_VECTORIZE=true and enable vectorize on your collection."
        )
    
    try:
        client = DataAPIClient()
        db = client.get_database(
            endpoint, 
            token=token,
            keyspace="well_planning"
        )
        coll = db.get_collection(collection_name)
        
        # Use vectorize for semantic search - this will fail if vectorize is not properly configured
        results = coll.find({}, 
                          sort={"$vectorize": keywords}, 
                          limit=k,
                          include_similarity=True)
        
        snippets = []
        for d in results:
            body = d.get("body", "")
            src = d.get("path") or d.get("filename") or d.get("url")
            similarity = d.get("$similarity", 0.0)
            
            if not body:
                raise ValueError(f"Document has no text content: {src}")
            
            snippet = body[:500] + ("..." if len(body) > 500 else "")
            snippets.append({
                "source": src or "unknown",
                "snippet": snippet,
                "similarity": similarity
            })
        
        if not snippets:
            raise ValueError(f"No documents found for keywords: {keywords}")
            
        return snippets
        
    except Exception as e:
        raise ConnectionError(f"AstraDB vectorize search failed: {e}")

def retrieve_subgraph_context(well_id: str, objectives: str) -> Dict[str, Any]:
    """Retrieve context for a well using GraphRAG - fails fast if no data exists."""
    
    # Enhanced query to get comprehensive well context
    q = """
    MATCH (w:Well {well_id: $wid})
    OPTIONAL MATCH (w)-[:HAS_FORMATION]->(f:Formation)
    WITH w, collect({
        name: f.name, 
        depth: [f.depth_start, f.depth_end], 
        rock_strength: f.rock_strength, 
        pore_pressure: f.pore_pressure,
        properties: properties(f)
    }) AS formations
    
    OPTIONAL MATCH (w)-[:HAS_PLAN]->(hp:HistoricalPlan)
    WITH w, formations, collect({
        plan_id: hp.plan_id, 
        kpi: hp.final_kpi_score,
        drilling_days: hp.drilling_days,
        lessons_learned: hp.lessons_learned
    })[0..3] AS examples
    
    OPTIONAL MATCH (b:BHATool)-[:HAS_CONSTRAINT]->(c:EngineeringConstraint)
    WITH w, formations, examples, collect({
        part: b.part_number, 
        type: b.tool_type, 
        manufacturer: b.manufacturer,
        limits: properties(c)
    }) AS bha
    
    RETURN {
        well_id: w.well_id,
        location: w.location,
        depth_target: w.depth_target,
        formations: formations, 
        examples: examples, 
        bha: bha,
        well_properties: properties(w)
    } AS ctx
    """
    
    try:
        with _session() as s:
            rec = s.run(q, wid=well_id).single()
            if not rec or not rec["ctx"]:
                raise ValueError(f"No data found for well_id: {well_id}")
            
            ctx = rec["ctx"]
    except Exception as e:
        if "No data found" in str(e):
            raise
        else:
            raise ConnectionError(f"Neo4j query failed: {e}")
    
    # Strict validation - no graceful degradation
    if not ctx.get("well_id"):
        raise ValueError(f"Invalid well data retrieved for: {well_id}")
    
    if not ctx.get("formations"):
        raise ValueError(f"No formation data found for well: {well_id}")
    
    # Add objectives to context
    ctx["objectives"] = objectives
    
    # Create enhanced keywords for vector search
    form_names = ", ".join([f.get("name", "") for f in ctx.get("formations", [])])
    well_location = ctx.get("location", "")
    keywords = f"{form_names} {well_location} {objectives}".strip()
    
    if not keywords:
        keywords = well_id
    
    # Get document snippets using GraphRAG approach - STRICT MODE
    ctx["docs"] = _astra_snippets(keywords, k=5)
    
    # Add metadata for transparency
    ctx["retrieval_metadata"] = {
        "search_keywords": keywords,
        "formations_count": len(ctx.get("formations", [])),
        "historical_examples_count": len(ctx.get("examples", [])),
        "bha_tools_count": len(ctx.get("bha", [])),
        "documents_retrieved": len(ctx.get("docs", []))
    }
    
    return ctx

def validate_against_constraints(well_id: str, plan_text: str) -> Dict[str, Any]:
    """Validate plan against real engineering constraints from knowledge graph."""
    
    # Extract parameters from plan text using structured parsing
    from app.llm.watsonx_client import parse_markdown_plan
    
    parsed_plan = parse_markdown_plan(plan_text)
    if parsed_plan.get("parsing_error"):
        raise ValueError(f"Cannot parse plan for validation: {parsed_plan['parsing_error']}")
    
    violations = []
    checked_constraints = []
    violation_details = []
    
    # Get constraints from Neo4j with enhanced query
    q = """
    MATCH (w:Well {well_id: $well_id})-[:HAS_FORMATION]->(f:Formation)
    MATCH (b:BHATool)-[:HAS_CONSTRAINT]->(c:EngineeringConstraint)
    RETURN DISTINCT
        f.name as formation, 
        f.pore_pressure as pp, 
        f.rock_strength as rs,
        b.part_number as tool_part,
        b.tool_type as tool_type,
        c.constraint_id as constraint_id, 
        c.constraint_type as constraint_type, 
        c.limit_value as limit_value, 
        c.unit as unit,
        c.description as description
    """
    
    try:
        with _session() as s:
            constraints = list(s.run(q, well_id=well_id))
    except Exception as e:
        raise ConnectionError(f"Failed to retrieve constraints from Neo4j: {e}")
    
    if not constraints:
        raise ValueError(f"No constraints found for well: {well_id}")
    
    # Validate each constraint against plan parameters
    for constraint in constraints:
        constraint_id = constraint["constraint_id"]
        constraint_type = constraint["constraint_type"]
        limit_value = float(constraint["limit_value"])
        unit = constraint["unit"]
        
        checked_constraints.append(constraint_id)
        
        # Pressure constraints
        if constraint_type == "pressure":
            mud_weights = extract_mud_weights(parsed_plan)
            for mw in mud_weights:
                if mw.get("value", 0) > limit_value:
                    violation_msg = f"Mud weight {mw['value']} {mw.get('unit', 'ppg')} exceeds limit {limit_value} {unit}"
                    violations.append(violation_msg)
                    violation_details.append({
                        "type": "pressure",
                        "constraint_id": constraint_id,
                        "actual_value": mw['value'],
                        "limit_value": limit_value,
                        "unit": unit,
                        "severity": "high" if mw['value'] > limit_value * 1.2 else "medium"
                    })
        
        # Torque constraints
        elif constraint_type == "torque":
            torque_values = extract_torque_values(parsed_plan)
            for tv in torque_values:
                if tv.get("value", 0) > limit_value:
                    violation_msg = f"Torque {tv['value']} {tv.get('unit', 'ft-lbs')} exceeds limit {limit_value} {unit}"
                    violations.append(violation_msg)
                    violation_details.append({
                        "type": "torque",
                        "constraint_id": constraint_id,
                        "actual_value": tv['value'],
                        "limit_value": limit_value,
                        "unit": unit,
                        "severity": "high" if tv['value'] > limit_value * 1.1 else "medium"
                    })
        
        # Rotation/RPM constraints
        elif constraint_type in ["rotation", "speed"]:
            rpm_values = extract_rpm_values(parsed_plan)
            for rpm in rpm_values:
                if rpm.get("value", 0) > limit_value:
                    violation_msg = f"RPM {rpm['value']} {rpm.get('unit', 'rpm')} exceeds limit {limit_value} {unit}"
                    violations.append(violation_msg)
                    violation_details.append({
                        "type": "rotation",
                        "constraint_id": constraint_id,
                        "actual_value": rpm['value'],
                        "limit_value": limit_value,
                        "unit": unit,
                        "severity": "medium"
                    })
        
        # Force/WOB constraints
        elif constraint_type == "force":
            wob_values = extract_wob_values(parsed_plan)
            for wob in wob_values:
                if wob.get("value", 0) > limit_value:
                    violation_msg = f"WOB {wob['value']} {wob.get('unit', 'lbs')} exceeds limit {limit_value} {unit}"
                    violations.append(violation_msg)
                    violation_details.append({
                        "type": "force",
                        "constraint_id": constraint_id,
                        "actual_value": wob['value'],
                        "limit_value": limit_value,
                        "unit": unit,
                        "severity": "high" if wob['value'] > limit_value * 1.15 else "medium"
                    })
    
    # Calculate confidence based on constraint coverage and violation severity
    high_severity_count = sum(1 for v in violation_details if v.get("severity") == "high")
    confidence = 0.9 if len(violations) == 0 else max(0.1, 0.9 - (len(violations) * 0.1) - (high_severity_count * 0.1))
    
    return {
        "passes": len(violations) == 0,
        "violations": violations,
        "violation_details": violation_details,
        "details": f"Checked {len(checked_constraints)} constraints across {len(set(c['constraint_type'] for c in constraints))} types",
        "checked_constraints": checked_constraints,
        "constraint_types_checked": list(set(c["constraint_type"] for c in constraints)),
        "confidence": round(confidence, 2),
        "total_constraints": len(constraints),
        "high_severity_violations": high_severity_count
    }

def extract_mud_weights(parsed_plan: Dict) -> List[Dict[str, Any]]:
    """Extract mud weight values from parsed plan."""
    mud_weights = []
    
    # Check parameters section
    for param in parsed_plan.get("parameters", []):
        if "mud_weight" in param:
            mw_str = param["mud_weight"]
            try:
                # Parse value and unit from string like "12.5 ppg"
                parts = mw_str.split()
                value = float(parts[0])
                unit = parts[1] if len(parts) > 1 else "ppg"
                mud_weights.append({"value": value, "unit": unit, "section": param.get("section", "unknown")})
            except (ValueError, IndexError):
                continue
    
    # Also search in plan text
    plan_text = parsed_plan.get("plan_text", "")
    mud_weight_patterns = [
        r"mud weight[:\s]*(\d+\.?\d*)\s*([a-zA-Z]*)",
        r"(\d+\.?\d*)\s*ppg",
        r"density[:\s]*(\d+\.?\d*)\s*([a-zA-Z]*)"
    ]
    
    for pattern in mud_weight_patterns:
        matches = re.findall(pattern, plan_text, re.IGNORECASE)
        for match in matches:
            try:
                if isinstance(match, tuple):
                    value = float(match[0])
                    unit = match[1] if len(match) > 1 and match[1] else "ppg"
                else:
                    value = float(match)
                    unit = "ppg"
                
                # Filter reasonable mud weight values (8-20 ppg typical)
                if 6 <= value <= 25:
                    mud_weights.append({"value": value, "unit": unit, "section": "text_extraction"})
            except (ValueError, IndexError):
                continue
    
    return mud_weights

def extract_torque_values(parsed_plan: Dict) -> List[Dict[str, Any]]:
    """Extract torque values from parsed plan."""
    torque_values = []
    
    # Check parameters section
    for param in parsed_plan.get("parameters", []):
        for key, value in param.items():
            if "torque" in key.lower() and isinstance(value, str):
                try:
                    # Parse value and unit from string like "7500 ft-lbs"
                    parts = value.split()
                    if len(parts) >= 1:
                        torque_val = float(parts[0])
                        unit = " ".join(parts[1:]) if len(parts) > 1 else "ft-lbs"
                        torque_values.append({"value": torque_val, "unit": unit, "section": param.get("section", "unknown")})
                except (ValueError, IndexError):
                    continue
    
    # Search in plan text for torque mentions
    plan_text = parsed_plan.get("plan_text", "")
    torque_patterns = [
        r"torque[:\s]*(\d+\.?\d*)\s*([a-zA-Z\-\s]*)",
        r"(\d+\.?\d*)\s*ft-?lbs?",
        r"max\s*torque[:\s]*(\d+\.?\d*)\s*([a-zA-Z\-\s]*)"
    ]
    
    for pattern in torque_patterns:
        matches = re.findall(pattern, plan_text, re.IGNORECASE)
        for match in matches:
            try:
                if isinstance(match, tuple):
                    value = float(match[0])
                    unit = match[1].strip() if len(match) > 1 and match[1] else "ft-lbs"
                else:
                    value = float(match)
                    unit = "ft-lbs"
                
                # Filter reasonable torque values (100-15000 ft-lbs typical)
                if 50 <= value <= 20000:
                    torque_values.append({"value": value, "unit": unit, "section": "text_extraction"})
            except (ValueError, IndexError):
                continue
    
    return torque_values

def extract_rpm_values(parsed_plan: Dict) -> List[Dict[str, Any]]:
    """Extract RPM values from parsed plan."""
    rpm_values = []
    
    # Check parameters section
    for param in parsed_plan.get("parameters", []):
        if "rpm" in param:
            rpm_str = param["rpm"]
            try:
                parts = rpm_str.split()
                value = float(parts[0])
                unit = parts[1] if len(parts) > 1 else "rpm"
                rpm_values.append({"value": value, "unit": unit, "section": param.get("section", "unknown")})
            except (ValueError, IndexError):
                continue
    
    # Search in plan text
    plan_text = parsed_plan.get("plan_text", "")
    rpm_patterns = [
        r"rpm[:\s]*(\d+\.?\d*)",
        r"(\d+\.?\d*)\s*rpm",
        r"rotation[:\s]*(\d+\.?\d*)\s*rpm"
    ]
    
    for pattern in rpm_patterns:
        matches = re.findall(pattern, plan_text, re.IGNORECASE)
        for match in matches:
            try:
                value = float(match)
                # Filter reasonable RPM values (20-300 typical)
                if 10 <= value <= 500:
                    rpm_values.append({"value": value, "unit": "rpm", "section": "text_extraction"})
            except (ValueError, IndexError):
                continue
    
    return rpm_values

def extract_wob_values(parsed_plan: Dict) -> List[Dict[str, Any]]:
    """Extract WOB (Weight on Bit) values from parsed plan."""
    wob_values = []
    
    # Check parameters section
    for param in parsed_plan.get("parameters", []):
        if "wob" in param:
            wob_str = param["wob"]
            try:
                # Handle values like "35 klbs" or "35000 lbs"
                parts = wob_str.split()
                value = float(parts[0])
                unit = parts[1] if len(parts) > 1 else "lbs"
                
                # Convert klbs to lbs for comparison
                if unit.lower() == "klbs":
                    value *= 1000
                    unit = "lbs"
                
                wob_values.append({"value": value, "unit": unit, "section": param.get("section", "unknown")})
            except (ValueError, IndexError):
                continue
    
    # Search in plan text
    plan_text = parsed_plan.get("plan_text", "")
    wob_patterns = [
        r"wob[:\s]*(\d+\.?\d*)\s*([a-zA-Z]*)",
        r"weight on bit[:\s]*(\d+\.?\d*)\s*([a-zA-Z]*)",
        r"(\d+\.?\d*)\s*k?lbs"
    ]
    
    for pattern in wob_patterns:
        matches = re.findall(pattern, plan_text, re.IGNORECASE)
        for match in matches:
            try:
                if isinstance(match, tuple):
                    value = float(match[0])
                    unit = match[1] if len(match) > 1 and match[1] else "lbs"
                else:
                    value = float(match)
                    unit = "lbs"
                
                # Convert klbs to lbs
                if unit.lower() == "klbs":
                    value *= 1000
                    unit = "lbs"
                
                # Filter reasonable WOB values (5000-60000 lbs typical)
                if 1000 <= value <= 80000:
                    wob_values.append({"value": value, "unit": unit, "section": "text_extraction"})
            except (ValueError, IndexError):
                continue
    
    return wob_values

def record_iteration(plan_id: str, iteration: int, draft: str, validation: Dict, kpis: Dict) -> None:
    """Record planning iteration in Neo4j with proper data types"""
    
    # Convert complex objects to JSON strings for Neo4j storage
    validation_json = json.dumps(validation) if validation else "{}"
    kpis_json = json.dumps(kpis) if kpis else "{}"
    
    q = """
    MERGE (p:PlanIteration {plan_id: $pid, iter: $iter}) 
    SET p.draft = $draft, 
        p.validation_json = $validation_json, 
        p.kpis_json = $kpis_json, 
        p.ts = timestamp(),
        p.passes = $passes,
        p.violations_count = $violations_count,
        p.total_score = $total_score,
        p.confidence = $confidence
    """
    
    try:
        with _session() as s:
            s.run(q, 
                pid=plan_id, 
                iter=iteration, 
                draft=draft[:2000],  # Truncate very long drafts
                validation_json=validation_json,
                kpis_json=kpis_json,
                passes=validation.get("passes", False),
                violations_count=len(validation.get("violations", [])),
                total_score=kpis.get("kpi_overall", 0.0) if kpis else 0.0,
                confidence=validation.get("confidence", 0.0)
            )
    except Exception as e:
        raise ConnectionError(f"Failed to record iteration in Neo4j: {e}")

def initialize_schema():
    """Initialize the Neo4j schema - run this once after setting up local Neo4j"""
    schema_queries = [
        # Create constraints
        "CREATE CONSTRAINT well_id_unique IF NOT EXISTS FOR (w:Well) REQUIRE w.well_id IS UNIQUE",
        "CREATE CONSTRAINT plan_iteration_unique IF NOT EXISTS FOR (p:PlanIteration) REQUIRE (p.plan_id, p.iter) IS UNIQUE",
        "CREATE CONSTRAINT bha_tool_unique IF NOT EXISTS FOR (b:BHATool) REQUIRE b.part_number IS UNIQUE",
        "CREATE CONSTRAINT constraint_unique IF NOT EXISTS FOR (c:EngineeringConstraint) REQUIRE c.constraint_id IS UNIQUE",
        
        # Create indexes
        "CREATE INDEX formation_name IF NOT EXISTS FOR (f:Formation) ON (f.name)",
        "CREATE INDEX bha_tool_type IF NOT EXISTS FOR (b:BHATool) ON (b.tool_type)",
        "CREATE INDEX plan_iteration_ts IF NOT EXISTS FOR (p:PlanIteration) ON (p.ts)",
        "CREATE INDEX constraint_type IF NOT EXISTS FOR (c:EngineeringConstraint) ON (c.constraint_type)",
        "CREATE INDEX well_location IF NOT EXISTS FOR (w:Well) ON (w.location)",
    ]
    
    try:
        with _session() as s:
            for query in schema_queries:
                s.run(query)
        print("✅ Neo4j schema initialized successfully")
    except Exception as e:
        raise ConnectionError(f"Schema initialization failed: {e}")

def load_sample_data():
    """Load sample data into Neo4j"""
    sample_data_queries = [
        # Clear existing data
        "MATCH (n) DETACH DELETE n",
        
        # Create sample well
        """CREATE (w:Well {well_id: 'WELL_001', location: 'Permian Basin, Texas', depth_target: 10000, well_type: 'Horizontal'})""",
        
        # Create sample formations
        """CREATE (f1:Formation {name: 'Sandstone_A', depth_start: 0, depth_end: 5000, rock_strength: 25000, pore_pressure: 0.45, formation_type: 'Sandstone'})""",
        """CREATE (f2:Formation {name: 'Shale_B', depth_start: 5000, depth_end: 10000, rock_strength: 35000, pore_pressure: 0.52, formation_type: 'Shale'})""",
        
        # Create sample BHA tools
        """CREATE (b1:BHATool {part_number: 'PDC-001', tool_type: 'PDC_Bit', manufacturer: 'Baker Hughes', diameter: 8.5, specifications: 'High performance PDC bit for hard formations'})""",
        """CREATE (b2:BHATool {part_number: 'MWD-001', tool_type: 'MWD', manufacturer: 'Halliburton', specifications: 'Measurement while drilling tool'})""",
        """CREATE (b3:BHATool {part_number: 'MOTOR-001', tool_type: 'Motor', manufacturer: 'Schlumberger', specifications: 'Positive displacement motor'})""",
        
        # Create sample constraints with proper types
        """CREATE (c1:EngineeringConstraint {constraint_id: 'MAX_PRESSURE', constraint_type: 'pressure', limit_value: 5000, unit: 'psi', description: 'Maximum allowable wellbore pressure'})""",
        """CREATE (c2:EngineeringConstraint {constraint_id: 'MAX_RPM', constraint_type: 'rotation', limit_value: 150, unit: 'rpm', description: 'Maximum rotary speed'})""",
        """CREATE (c3:EngineeringConstraint {constraint_id: 'MAX_TORQUE', constraint_type: 'torque', limit_value: 8000, unit: 'ft-lbs', description: 'Maximum allowable torque'})""",
        """CREATE (c4:EngineeringConstraint {constraint_id: 'MAX_WOB', constraint_type: 'force', limit_value: 40000, unit: 'lbs', description: 'Maximum weight on bit'})""",
        
        # Create historical plans
        """CREATE (hp1:HistoricalPlan {plan_id: 'HIST_001', final_kpi_score: 85.2, drilling_days: 12.5, lessons_learned: 'Optimal RPM for this formation is 120-140'})""",
        """CREATE (hp2:HistoricalPlan {plan_id: 'HIST_002', final_kpi_score: 78.9, drilling_days: 15.2, lessons_learned: 'Increase mud weight gradually to prevent circulation losses'})""",
        
        # Create relationships
        """MATCH (w:Well {well_id: 'WELL_001'}), (f1:Formation {name: 'Sandstone_A'}) CREATE (w)-[:HAS_FORMATION]->(f1)""",
        """MATCH (w:Well {well_id: 'WELL_001'}), (f2:Formation {name: 'Shale_B'}) CREATE (w)-[:HAS_FORMATION]->(f2)""",
        """MATCH (b:BHATool {part_number: 'PDC-001'}), (c:EngineeringConstraint {constraint_id: 'MAX_WOB'}) CREATE (b)-[:HAS_CONSTRAINT]->(c)""",
        """MATCH (b:BHATool {part_number: 'PDC-001'}), (c:EngineeringConstraint {constraint_id: 'MAX_TORQUE'}) CREATE (b)-[:HAS_CONSTRAINT]->(c)""",
        """MATCH (b:BHATool {part_number: 'MOTOR-001'}), (c:EngineeringConstraint {constraint_id: 'MAX_PRESSURE'}) CREATE (b)-[:HAS_CONSTRAINT]->(c)""",
        """MATCH (w:Well {well_id: 'WELL_001'}), (hp:HistoricalPlan) CREATE (w)-[:HAS_PLAN]->(hp)""",
    ]
    
    try:
        with _session() as s:
            for query in sample_data_queries:
                s.run(query)
        print("✅ Sample data loaded successfully")
    except Exception as e:
        raise ConnectionError(f"Sample data loading failed: {e}")

def test_neo4j_connection():
    """Test Neo4j connection - fails fast if unavailable"""
    try:
        with _session() as s:
            result = s.run("RETURN 'Neo4j Connected' as message")
            record = result.single()
            print(f"✅ Neo4j connection successful: {record['message']}")
            return True
    except Exception as e:
        raise ConnectionError(f"Neo4j connection failed: {e}")

def get_well_statistics():
    """Get statistics about wells in the knowledge graph."""
    q = """
    MATCH (w:Well)
    OPTIONAL MATCH (w)-[:HAS_FORMATION]->(f:Formation)
    OPTIONAL MATCH (w)-[:HAS_PLAN]->(hp:HistoricalPlan)
    RETURN 
        count(DISTINCT w) as total_wells,
        count(DISTINCT f) as total_formations,
        count(DISTINCT hp) as total_historical_plans,
        collect(DISTINCT w.well_id)[0..5] as sample_well_ids
    """
    
    try:
        with _session() as s:
            result = s.run(q).single()
            return dict(result) if result else {}
    except Exception as e:
        raise ConnectionError(f"Failed to get well statistics: {e}")

if __name__ == "__main__":
    # Test connection and optionally load data
    try:
        test_neo4j_connection()
        stats = get_well_statistics()
        print(f"📊 Knowledge Graph Stats: {stats}")
        
        print("\nDo you want to initialize schema and load sample data? (y/n): ", end="")
        response = input().lower().strip()
        if response == 'y':
            initialize_schema()
            load_sample_data()
            print("🎉 Setup complete!")
    except Exception as e:
        print(f"❌ Setup failed: {e}")
        exit(1)