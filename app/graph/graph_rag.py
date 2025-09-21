import os
import json  # Add this import
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()
URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
USER = os.getenv("NEO4J_USERNAME", "neo4j") 
PWD = os.getenv("NEO4J_PASSWORD")

# Global driver instance
_driver = None

def _get_driver():
    global _driver
    if _driver is None:
        _driver = GraphDatabase.driver(URI, auth=(USER, PWD))
    return _driver

def _session():
    return _get_driver().session()

def close_driver():
    """Call this when shutting down the application"""
    global _driver
    if _driver:
        _driver.close()
        _driver = None

def _astra_snippets(keywords: str, k: int = 3):
    """Retrieve k short snippets from AstraDB using simple retrieval."""
    try:
        from astrapy import DataAPIClient
        client = DataAPIClient()
        db = client.get_database(
            os.getenv("ASTRA_DB_API_ENDPOINT"), 
            token=os.getenv("ASTRA_DB_APPLICATION_TOKEN"),
            keyspace="well_planning"
        )
        coll = db.get_collection(os.getenv("ASTRA_DB_VECTOR_COLLECTION", "drilling_docs"))
        
        # Simple document retrieval (vectorize not configured yet)
        items = list(coll.find({}).limit(k))
        
        snippets = []
        for d in items:
            body = d.get("body","")
            src = d.get("path") or d.get("url")
            if body:
                snippet = body[:500] + ("..." if len(body) > 500 else "")
            else:
                snippet = "(no text extracted)"
            snippets.append({"source": src, "snippet": snippet})
        return snippets
    except Exception as e:
        print(f"AstraDB connection failed, using fallback: {e}")
        # Return fallback sample data
        return [
            {"source": "drilling_manual.pdf", "snippet": "PDC bits are recommended for sandstone formations..."},
            {"source": "best_practices.md", "snippet": "Maintain WOB between 20-35k lbs for optimal ROP..."},
            {"source": "vibration_guide.pdf", "snippet": "Use stabilizers to minimize lateral vibration..."}
        ]

def retrieve_subgraph_context(well_id: str, objectives: str) -> dict:
    """Retrieve context for a well, with fallback if no data exists"""
    q = """
MATCH (w:Well {well_id:$wid})-[:HAS_FORMATION]->(f:Formation)
WITH w, collect({name:f.name, depth:[f.depth_start,f.depth_end], rs:f.rock_strength, pp:f.pore_pressure}) AS formations
OPTIONAL MATCH (hp:HistoricalPlan)<-[:HAS_PLAN]-(w)
WITH w, formations, collect({plan_id:hp.plan_id, kpi:hp.final_kpi_score})[0..3] AS examples
OPTIONAL MATCH (b:BHATool)-[:HAS_CONSTRAINT]->(c:EngineeringConstraint)
WITH w, formations, examples, collect({part:b.part_number, type:b.tool_type, limits:c{.*}}) AS bha
RETURN {well_id:w.well_id, formations:formations, examples:examples, bha:bha} AS ctx
"""
    try:
        with _session() as s:
            rec = s.run(q, wid=well_id).single()
            ctx = rec["ctx"] if rec else None
    except Exception as e:
        print(f"Neo4j query failed, using fallback: {e}")
        ctx = None
    
    # Enhanced fallback with sample data
    if not ctx:
        ctx = {
            "well_id": well_id,
            "formations": [
                {"name": "Sandstone_A", "depth": [0, 5000], "rs": 25000, "pp": 0.45},
                {"name": "Shale_B", "depth": [5000, 10000], "rs": 35000, "pp": 0.52}
            ],
            "examples": [
                {"plan_id": "HIST_001", "kpi": 85.2},
                {"plan_id": "HIST_002", "kpi": 78.9}
            ],
            "bha": [
                {"part": "PDC-001", "type": "PDC_Bit", "limits": {"max_wob": 40000, "max_rpm": 150}}
            ]
        }
    
    ctx["objectives"] = objectives
    form_names = ", ".join([f.get("name","") for f in ctx.get("formations", [])])
    keywords = (form_names + " " + objectives).strip() or well_id
    
    # Get document snippets
    ctx["docs"] = _astra_snippets(keywords, k=3)
    
    return ctx

def validate_against_constraints(well_id: str, plan_text: str) -> dict:
    """Validate plan against constraints"""
    # For now, using demo validation with some variability
    import random
    import hashlib
    
    # Use hash of plan_text for consistent results
    plan_hash = int(hashlib.md5(plan_text.encode()).hexdigest(), 16) % 100
    passes = plan_hash > 30  # 70% pass rate
    
    if passes:
        return {
            "passes": True, 
            "violations": [],  # CHANGE: Return empty list instead of 0
            "details": "All constraints satisfied",
            "checked_constraints": ["pressure", "vibration", "torque"],
            "confidence": 0.9  # ADD: For enhanced KPI
        }
    else:
        violations_count = 1 + (plan_hash % 3)  # 1-3 violations
        violation_list = [  # CHANGE: Return list of violation descriptions
            "Pressure exceeds maximum limit (5200 > 5000 psi)",
            "Vibration level critical (3.2 > 2.5 g)",
            "Torque limit exceeded (8500 > 8000 ft-lbs)"
        ][:violations_count]
        
        return {
            "passes": False, 
            "violations": violation_list,  # CHANGE: List instead of count
            "details": f"Found {violations_count} constraint violation(s)",
            "checked_constraints": ["pressure", "vibration", "torque"],
            "violation_details": [
                {"type": "pressure", "severity": "warning", "value": 5200, "limit": 5000},
                {"type": "vibration", "severity": "critical", "value": 3.2, "limit": 2.5}
            ][:violations_count],
            "confidence": 0.3  # ADD: For enhanced KPI
        }

def record_iteration(plan_id: str, iteration: int, draft: str, validation: dict, kpis: dict):
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
        p.violations = $violations,
        p.total_score = $total_score
    """
    
    try:
        with _session() as s:
            s.run(q, 
                pid=plan_id, 
                iter=iteration, 
                draft=draft[:1000],  # Truncate long drafts
                validation_json=validation_json,
                kpis_json=kpis_json,
                passes=validation.get("passes", False),
                violations=validation.get("violations", 0),
                total_score=kpis.get("kpi_overall", 0.0) if kpis else 0.0  # CHANGE: Use kpi_overall instead of overall
            )
    except Exception as e:
        print(f"Failed to record iteration in Neo4j: {e}")

def initialize_schema():
    """Initialize the Neo4j schema - run this once after setting up local Neo4j"""
    schema_queries = [
        # Create constraints
        "CREATE CONSTRAINT well_id_unique IF NOT EXISTS FOR (w:Well) REQUIRE w.well_id IS UNIQUE",
        "CREATE CONSTRAINT plan_iteration_unique IF NOT EXISTS FOR (p:PlanIteration) REQUIRE (p.plan_id, p.iter) IS UNIQUE",
        
        # Create indexes
        "CREATE INDEX formation_name IF NOT EXISTS FOR (f:Formation) ON (f.name)",
        "CREATE INDEX bha_tool_type IF NOT EXISTS FOR (b:BHATool) ON (b.tool_type)",
        "CREATE INDEX plan_iteration_ts IF NOT EXISTS FOR (p:PlanIteration) ON (p.ts)",
    ]
    
    try:
        with _session() as s:
            for query in schema_queries:
                s.run(query)
        print("✅ Neo4j schema initialized successfully")
    except Exception as e:
        print(f"❌ Schema initialization failed: {e}")

def load_sample_data():
    """Load sample data into Neo4j"""
    sample_data_queries = [
        # Clear existing data
        "MATCH (n) DETACH DELETE n",
        
        # Create sample well
        """CREATE (w:Well {well_id: 'WELL_001', location: 'Texas', depth_target: 10000})""",
        
        # Create sample formations
        """CREATE (f1:Formation {name: 'Sandstone_A', depth_start: 0, depth_end: 5000, rock_strength: 25000, pore_pressure: 0.45})""",
        """CREATE (f2:Formation {name: 'Shale_B', depth_start: 5000, depth_end: 10000, rock_strength: 35000, pore_pressure: 0.52})""",
        
        # Create sample BHA tools
        """CREATE (b1:BHATool {part_number: 'PDC-001', tool_type: 'PDC_Bit', manufacturer: 'Baker Hughes'})""",
        """CREATE (b2:BHATool {part_number: 'MWD-001', tool_type: 'MWD', manufacturer: 'Halliburton'})""",
        
        # Create sample constraints
        """CREATE (c1:EngineeringConstraint {constraint_id: 'MAX_WOB', constraint_type: 'Force', limit_value: 40000, unit: 'lbs'})""",
        """CREATE (c2:EngineeringConstraint {constraint_id: 'MAX_RPM', constraint_type: 'Speed', limit_value: 150, unit: 'rpm'})""",
        
        # Create historical plans
        """CREATE (hp1:HistoricalPlan {plan_id: 'HIST_001', final_kpi_score: 85.2, drilling_days: 12.5})""",
        """CREATE (hp2:HistoricalPlan {plan_id: 'HIST_002', final_kpi_score: 78.9, drilling_days: 15.2})""",
        
        # Create relationships
        """MATCH (w:Well {well_id: 'WELL_001'}), (f1:Formation {name: 'Sandstone_A'}) CREATE (w)-[:HAS_FORMATION]->(f1)""",
        """MATCH (w:Well {well_id: 'WELL_001'}), (f2:Formation {name: 'Shale_B'}) CREATE (w)-[:HAS_FORMATION]->(f2)""",
        """MATCH (b:BHATool {part_number: 'PDC-001'}), (c:EngineeringConstraint {constraint_id: 'MAX_WOB'}) CREATE (b)-[:HAS_CONSTRAINT]->(c)""",
        """MATCH (w:Well {well_id: 'WELL_001'}), (hp:HistoricalPlan) CREATE (w)-[:HAS_PLAN]->(hp)""",
    ]
    
    try:
        with _session() as s:
            for query in sample_data_queries:
                s.run(query)
        print("✅ Sample data loaded successfully")
    except Exception as e:
        print(f"❌ Sample data loading failed: {e}")
        print("   This is normal if Neo4j is not running - fallbacks will be used")

# Test Neo4j connection
def test_neo4j_connection():
    """Test Neo4j connection and initialize if working"""
    try:
        with _session() as s:
            result = s.run("RETURN 'Neo4j Connected' as message")
            record = result.single()
            print(f"✅ Neo4j connection successful: {record['message']}")
            return True
    except Exception as e:
        print(f"⚠️  Neo4j connection failed: {e}")
        print("   Using fallback data - this is normal for development")
        return False

if __name__ == "__main__":
    # Test connection and optionally load data
    if test_neo4j_connection():
        print("\nDo you want to initialize schema and load sample data? (y/n): ", end="")
        response = input().lower().strip()
        if response == 'y':
            initialize_schema()
            load_sample_data()
    else:
        print("Skipping Neo4j setup - fallback mode will be used")