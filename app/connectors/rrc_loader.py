import os, argparse, pandas as pd
from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()

def to_neo4j(df: pd.DataFrame):
    from app.graph.graph_rag import URI, USER, PWD
    driver = GraphDatabase.driver(URI, auth=(USER, PWD))
    q = """
UNWIND $rows AS r
MERGE (w:Well {well_id: coalesce(r.API_NUMBER, r.API, r.WELL_API, r.WELL_NO)})
SET w.country='US', w.state='TX', w.source='RRC',
    w.operator = coalesce(r.OPERATOR, r.Operator, r.OPERATOR_NAME),
    w.county = coalesce(r.COUNTY, r.COUNTY_NAME),
    w.field = coalesce(r.FIELD, r.Field),
    w.location = coalesce(r.SURVEY, r.SURF_LOCATION, r.LOCATION)
"""
    with driver.session() as s:
        s.run(q, rows=df.to_dict("records"))
    driver.close()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    ap.add_argument("--limit", type=int, default=200)
    args = ap.parse_args()
    df = pd.read_csv(args.csv)
    if args.limit:
        df = df.head(args.limit)
    to_neo4j(df)
    print(f"Loaded {len(df)} RRC wells into Neo4j.")

if __name__ == "__main__":
    main()
