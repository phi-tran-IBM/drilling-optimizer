import os, argparse, pandas as pd
from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()

def to_neo4j(df: pd.DataFrame):
    from app.graph.graph_rag import URI, USER, PWD
    driver = GraphDatabase.driver(URI, auth=(USER, PWD))
    q = """
UNWIND $rows AS r
MERGE (w:Well {well_id: coalesce(r.API, toString(r.OBJECTID), r.WELL_NAME)})
SET w.country='US', w.source='BOEM', w.status = r.STATUS, w.field = r.FIELD_NAME,
    w.location = apoc.convert.toJson({lat:r.LAT, lon:r.LON})
"""
    with driver.session() as s:
        s.run(q, rows=df.to_dict("records"))
    driver.close()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=str, default="data/external_samples/boem_offshore_wells_sample.csv")
    args = ap.parse_args()
    df = pd.read_csv(args.sample)
    if df.empty:
        raise SystemExit("No BOEM rows in sample.")
    to_neo4j(df)
    print(f"Loaded {len(df)} BOEM wells into Neo4j.")

if __name__ == "__main__":
    main()
