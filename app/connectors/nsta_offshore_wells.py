import os, argparse, requests, pandas as pd
from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()
NSTA_FEATURE_URL = os.getenv("NSTA_WELLS_URL",
    "https://services9.arcgis.com/8pcKnVYHe23zA6C4/arcgis/rest/services/Offshore_Wells_WGS84/FeatureServer/0/query")

def fetch_nsta(limit=50):
    params = {"where":"1=1","outFields":"*","f":"json","resultRecordCount":limit}
    r = requests.get(NSTA_FEATURE_URL, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()
    rows = [f["attributes"] for f in data.get("features", [])]
    return pd.DataFrame(rows)

def to_neo4j(df: pd.DataFrame):
    from app.graph.graph_rag import URI, USER, PWD
    driver = GraphDatabase.driver(URI, auth=(USER, PWD))
    q = """
UNWIND $rows AS r
MERGE (w:Well {well_id: coalesce(r.WELL_ID, r.WELL_WONS, toString(r.OBJECTID))})
SET w.country='UK', w.source='NSTA', w.status = r.STATUS, w.year = r.YEAR, w.kb = r.KB_ELEVATION,
    w.location = apoc.convert.toJson({lat:r.LATITUDE, lon:r.LONGITUDE})
"""
    with driver.session() as s:
        s.run(q, rows=df.to_dict("records"))
    driver.close()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=50)
    args = ap.parse_args()
    df = fetch_nsta(args.limit)
    if df.empty:
        raise SystemExit("No NSTA records fetched.")
    to_neo4j(df)
    print(f"Loaded {len(df)} NSTA wells into Neo4j.")

if __name__ == "__main__":
    main()
