#!/usr/bin/env python3
"""
Quick script to check what data is loaded in Neo4j for the well planning system.
"""

import os
import sys
from dotenv import load_dotenv
from neo4j import GraphDatabase
import pandas as pd

load_dotenv()

def connect_to_neo4j():
    """Connect to Neo4j database."""
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    username = os.getenv("NEO4J_USERNAME", "neo4j")
    password = os.getenv("NEO4J_PASSWORD")
    
    if not password:
        print("Error: NEO4J_PASSWORD environment variable not set")
        return None
    
    try:
        driver = GraphDatabase.driver(uri, auth=(username, password))
        driver.verify_connectivity()
        print(f"✅ Connected to Neo4j at {uri}")
        return driver
    except Exception as e:
        print(f"❌ Failed to connect to Neo4j: {e}")
        return None

def run_query(driver, query, description):
    """Run a query and display results."""
    print(f"\n🔍 {description}")
    print("-" * 50)
    
    try:
        with driver.session() as session:
            result = session.run(query)
            records = [record.data() for record in result]
            
            if records:
                df = pd.DataFrame(records)
                print(df.to_string(index=False))
            else:
                print("No data found")
                
    except Exception as e:
        print(f"Error running query: {e}")

def main():
    """Main function to check Neo4j data."""
    print("🚀 Checking Neo4j Data for Well Planning System")
    print("=" * 60)
    
    driver = connect_to_neo4j()
    if not driver:
        return 1
    
    try:
        # Query 1: Count all node types
        query1 = """
        MATCH (n) 
        RETURN labels(n) as node_type, count(*) as count
        ORDER BY count DESC
        """
        run_query(driver, query1, "All Node Types and Counts")
        
        # Query 2: All wells summary
        query2 = """
        MATCH (w:Well) 
        RETURN w.well_id, w.source, w.country, w.status, w.field, 
               w.latitude, w.longitude, w.operator
        ORDER BY w.source
        """
        run_query(driver, query2, "All Wells Summary")
        
        # Query 3: Wells by source
        query3 = """
        MATCH (w:Well) 
        RETURN w.source, w.country, count(*) as well_count
        ORDER BY well_count DESC
        """
        run_query(driver, query3, "Wells Count by Source")
        
        # Query 4: Texas RRC wells specifically
        query4 = """
        MATCH (w:Well {country: 'US', source: 'RRC'})
        RETURN w.well_id, w.operator, w.county, w.field, w.status
        ORDER BY w.operator
        """
        run_query(driver, query4, "Texas RRC Wells Details")
        
        # Query 5: USGS grid data
        query5 = """
        MATCH (g:RegionGrid)
        RETURN g.grid_id, g.total_wells, g.oil_wells, g.gas_wells, 
               g.horizontal_wells, g.fractured_wells
        ORDER BY g.total_wells DESC
        """
        run_query(driver, query5, "USGS Regional Grid Data")
        
        # Query 6: Database statistics
        query6 = """
        CALL db.stats.retrieve('GRAPH COUNTS') 
        YIELD section, data 
        RETURN section, data
        """
        run_query(driver, query6, "Database Statistics")
        
    finally:
        driver.close()
        print("\n✅ Database connection closed")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())