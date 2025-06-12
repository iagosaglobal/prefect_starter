from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector
from sqlalchemy import text

@task
def fetch_data(query: str):
    with SqlAlchemyConnector.load("postgres-connector", validate=False) as connector:
        with connector.get_connection(begin=False) as connection:
            result = connection.execute(text(query)).fetchall()
    return result

@flow
def postgres_flow(query: str = "SELECT * FROM customers LIMIT 10"):
    data = fetch_data(query)
    print(f"Retrieved {len(data)} rows")
    return data