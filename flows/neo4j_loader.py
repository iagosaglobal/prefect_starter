from prefect import flow, task
from neo4j import GraphDatabase
from prefect.blocks.system import Secret

@task
def load_data_to_neo4j():
    password = Secret.load("neo4j-password").get()
    driver = GraphDatabase.driver(
        uri="neo4j+ssc://102a2f57.databases.neo4j.io",
        auth=("neo4j", password)
    )
    with driver.session() as session:
        session.run("CREATE (p:Person {name: 'Prefect'})")

@flow
def neo4j_loader_flow():
    load_data_to_neo4j()