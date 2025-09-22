import os, argparse, glob, subprocess, shlex
from typing import List, Dict, Optional
from dotenv import load_dotenv
from neo4j import GraphDatabase
from pdfminer.high_level import extract_text
from app.papers.topic_suggester import suggest_topics

load_dotenv()

def _neo_session():
    from app.graph.graph_rag import URI, USER, PWD
    return GraphDatabase.driver(URI, auth=(USER, PWD)).session()

def _first_page_text(path: str) -> str:
    txt = extract_text(path) or ""
    return txt[:3000]

def _guess_meta(txt: str, pdf_path: str, source: Optional[str]) -> Dict:
    meta = {"title": None, "year": None, "doi": None, "url": None, "source": source or "paper"}
    lines = [l.strip() for l in txt.splitlines() if l.strip()]
    if lines:
        meta["title"] = lines[0][:300]
    import re
    m = re.search(r"10\.[0-9]{4,9}/[-._;()/:A-Za-z0-9]+", txt)
    if m:
        meta["doi"] = m.group(0)[:200]
    ym = re.search(r"(19|20)[0-9]{2}", txt)
    if ym:
        meta["year"] = int(ym.group(0))
    if not meta["title"]:
        meta["title"] = os.path.basename(pdf_path)
    return meta

def _upsert_paper(meta: Dict, topics: List[str]) -> None:
    q = """
MERGE (p:Paper {title:$title})
SET p.year = $year, p.doi = $doi, p.url = $url, p.source = $source
WITH p
UNWIND $topics AS t
MERGE (topic:Topic {name: t})
MERGE (p)-[:ADDRESSES_TOPIC]->(topic)
"""
    with _neo_session() as s:
        s.run(q, title=meta.get("title"), year=meta.get("year"),
              doi=meta.get("doi"), url=meta.get("url"), source=meta.get("source"),
              topics=topics or [])

def _ingest_to_astra(pdf_path: str, source_tag: str):
    cmd = f"python -m app.vector.astra_doc_ingest --files {shlex.quote(pdf_path)} --source_tag {shlex.quote(source_tag)}"
    rc = subprocess.call(cmd, shell=True)
    if rc != 0:
        raise SystemExit(f"Astra ingestion failed for {pdf_path} (rc={rc})")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdf", help="single PDF path")
    ap.add_argument("--pdfs", nargs="*", help="multiple PDF paths/glob")
    ap.add_argument("--topics", default=None, help="comma-separated topics (e.g., 'ROP, stick-slip'). If omitted, topics are auto-derived.")
    ap.add_argument("--source", default="paper", help="source label (e.g., 'NETL' or 'arXiv')")
    args = ap.parse_args()

    paths: List[str] = []
    if args.pdf:
        paths.append(args.pdf)
    if args.pdfs:
        for p in args.pdfs:
            import glob as _glob
            paths.extend(_glob.glob(p))

    if not paths:
        raise SystemExit("No PDFs provided. Use --pdf or --pdfs.")

    topics = [t.strip() for t in args.topics.split(",")] if isinstance(args.topics, str) else []

    for p in paths:
        if not os.path.isfile(p):
            raise SystemExit(f"File not found: {p}")
        txt = _first_page_text(p)
        if not txt:
            raise SystemExit(f"Could not extract text from {p}. Aborting.")
        meta = _guess_meta(txt, p, args.source)
        use_topics = topics[:] if topics else suggest_topics(txt, top_k=6)
        _upsert_paper(meta, use_topics)
        _ingest_to_astra(p, source_tag=args.source)
        print(f"Uploaded Paper node + Astra doc: {meta.get('title')} ({p}) | topics={use_topics}")

if __name__ == "__main__":
    main()
