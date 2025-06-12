from prefect_sqlalchemy import SqlAlchemyConnector

connector = SqlAlchemyConnector(
    connection_info={
        "url": "postgresql://user:password@host:5432/dbname",
        "pool_size": 5  # Optional connection pooling
    }
)
connector.save("postgres-connector-v2")  # Save for reuse