import os, csv
from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()
URI = os.getenv("NEO4J_URI"); USER = os.getenv("NEO4J_USER"); PWD = os.getenv("NEO4J_PASSWORD")

def run(tx, query, **kw): tx.run(query, **kw)

def main():
    driver = GraphDatabase.driver(URI, auth=(USER, PWD))
    with driver.session() as s:
        schema = open("app/graph/neo4j_schema.cypher").read()
        for q in [x.strip() for x in schema.split(";") if x.strip()]:
            s.execute_write(run, q)

        with open("data/wells.csv") as f:
            r = csv.DictReader(f)
            for row in r:
                s.execute_write(run, """MERGE (w:Well {well_id:$well_id, api_number:$api, trajectory_type:$traj, location:$loc})""",
                    well_id=row["well_id"], api=row["api_number"], traj=row["trajectory_type"], loc=row["location"])

        with open("data/formations.csv") as f:
            r = csv.DictReader(f)
            for row in r:
                s.execute_write(run, """
MERGE (f:Formation {name:$name})
SET f.depth_start=toFloat($ds), f.depth_end=toFloat($de), f.rock_strength=toFloat($rs), f.pore_pressure=toFloat($pp)
WITH f
MATCH (w:Well {well_id:$wid})
MERGE (w)-[:HAS_FORMATION]->(f)
""", name=row["name"], ds=row["depth_start"], de=row["depth_end"], rs=row["rock_strength"], pp=row["pore_pressure"], wid=row["well_id"])

        with open("data/bha_catalog.csv") as f:
            r = csv.DictReader(f)
            for row in r:
                s.execute_write(run, """
MERGE (b:BHATool {part_number:$pn})
SET b.tool_type:$tt, b.manufacturer:$mfr, b.operating_limits=$limits
""", pn=row["part_number"], tt=row["tool_type"], mfr=row["manufacturer"], limits=row["operating_limits"])

        with open("data/constraints.csv") as f:
            r = csv.DictReader(f)
            for row in r:
                s.execute_write(run, """
MERGE (c:EngineeringConstraint {constraint_id:$cid})
SET c.constraint_type:$ctype, c.description:$desc, c.limit_value:toFloat($lim), c.unit:$unit
WITH c
MATCH (b:BHATool {part_number:$pn})
MERGE (b)-[:HAS_CONSTRAINT]->(c)
""", cid=row["constraint_id"], ctype=row["constraint_type"], desc=row["description"], lim=row["limit_value"], unit=row["unit"], pn=row["target_part_number"])

        with open("data/historical_plans.csv") as f:
            r = csv.DictReader(f)
            for row in r:
                s.execute_write(run, """
MERGE (hp:HistoricalPlan {plan_id:$pid})
SET hp.well_id=$wid, hp.final_kpi_score=toFloat($kpi), hp.lessons_learned:$ll
WITH hp
MATCH (w:Well {well_id:$wid})
MERGE (w)-[:HAS_PLAN]->(hp)
""", pid=row["plan_id"], wid=row["well_id"], kpi=row["final_kpi_score"], ll=row["lessons_learned"])

    driver.close()
    print("Neo4j sample data loaded.")

if __name__ == "__main__":
    main()
