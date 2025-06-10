from neo4j import GraphDatabase
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

URI = "neo4j+ssc://102a2f57.databases.neo4j.io"
AUTH = ("neo4j", "gw6xdLdKC7ftrlyjT6p5FH9-1NcrOlkNUgaDBSIRJ7c")

try:
    logger.info("Attempting to connect to Neo4j...")
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        logger.info("Driver created successfully")
        driver.verify_connectivity()
        logger.info("Connection verified successfully")
except Exception as e:
    logger.error(f"Failed to connect to Neo4j: {str(e)}")
    raise