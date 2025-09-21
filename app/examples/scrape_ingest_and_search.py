import os, argparse, requests, tempfile
from dotenv import load_dotenv
from bs4 import BeautifulSoup as BS
from app.vector.astra_doc_ingest import main as ingest_main
from app.graph.graph_rag import retrieve_subgraph_context

load_dotenv()

def scrape_to_tempfile(url: str) -> str:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    html = r.text
    text = BS(html, "html.parser").get_text(" ")
    fd, path = tempfile.mkstemp(suffix=".txt", prefix="scrape_")
    with os.fdopen(fd, "w") as f:
        f.write(text)
    return path

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="https://opendata-nstauthority.hub.arcgis.com/pages/well-data")
    ap.add_argument("--well_id", default="W-1001")
    ap.add_argument("--objectives", default="Minimize cost and vibration")
    args = ap.parse_args()

    local_txt = scrape_to_tempfile(args.url)
    os.system(f"python -m app.vector.astra_doc_ingest --files {local_txt} --source_tag scraped" )

    ctx = retrieve_subgraph_context(args.well_id, args.objectives)
    print("=== FORMATIONS ===")
    for f in ctx.get("formations", []):
        print(f"- {f.get('name')} depth={f.get('depth')} rs={f.get('rs')} pp={f.get('pp')}")
    print("\n=== DOC SNIPPETS (Astra) ===")
    for d in ctx.get("docs", []):
        print(f"- src={d.get('source')}\n  snippet={d.get('snippet')[:200]}")

if __name__ == "__main__":
    main()
