from prefect import flow, task
from neo4j import GraphDatabase
from dotenv import load_dotenv 
import os

load_dotenv()

@task
def run_neo4j_query():
    uri = os.getenv("NEO4J_URI")
    username = os.getenv("NEO4J_USERNAME")
    password = os.getenv("NEO4J_PASSWORD")

    driver = GraphDatabase.driver(uri, auth=(username, password))
    
    with driver.session() as session:
        result = session.run("MATCH (n) RETURN count(n) AS count")
        print(f"Total nodes in DB: {result.single()['count']}")

@flow
def neo4j_flow():
    run_neo4j_query()

if __name__ == "__main__":
    neo4j_flow()