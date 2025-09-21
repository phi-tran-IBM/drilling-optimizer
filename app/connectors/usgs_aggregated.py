import os, argparse, pandas as pd
from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()

def to_neo4j(df: pd.DataFrame):
    from app.graph.graph_rag import URI, USER, PWD
    driver = GraphDatabase.driver(URI, auth=(USER, PWD))
    q = """
UNWIND $rows AS r
MERGE (g:RegionGrid {grid_id:r.grid_id})
SET g.source='USGS', g.total_wells=toInteger(r.total_wells), g.oil=toInteger(r.oil), g.gas=toInteger(r.gas),
    g.horiz=toInteger(r.horizontal), g.frac=toInteger(r.fractured)
"""
    with driver.session() as s:
        s.run(q, rows=df.to_dict("records"))
    driver.close()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", type=str, default="data/external_samples/usgs_drilling_history_sample.csv")
    args = ap.parse_args()
    df = pd.read_csv(args.csv)
    if df.empty:
        raise SystemExit("Empty USGS sample.")
    to_neo4j(df)
    print(f"Loaded {len(df)} USGS grid records into Neo4j.")

if __name__ == "__main__":
    main()
