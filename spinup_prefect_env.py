import asyncio
from prefect import flow
from prefect.server.schemas.schedules import IntervalSchedule
from prefect_sqlalchemy import SqlAlchemyConnector
from prefect.blocks.system import Secret
from prefect.client.orchestration import get_client
from datetime import timedelta
from importlib import import_module
from prefect.client.schemas.actions import WorkPoolCreate
from prefect.filesystems import LocalFileSystem
from prefect import Flow
from pathlib import Path

import os
os.environ["PREFECT_API_URL"] = "http://127.0.0.1:4200/api"

CONFIG = {
    "postgres": {
        "block_name": "client-postgres-prod",
        "url": "postgresql+psycopg2://postgres:1234@localhost:5432/initial_db",
        "pool_size": 5
    },
    "neo4j": {
        "block_name": "client-neo4j-prod",
        "uri": "neo4j+ssc://102a2f57.databases.neo4j.io",
        "username": "neo4j",
        "password": "gw6xdLdKC7ftrlyjT6p5FH9-1NcrOlkNUgaDBSIRJ7c"
    },
    "deployments": {
        "postgres_flow_path": "flows/postgres_loader.py:postgres_flow",
        "neo4j_flow_path": "flows/neo4j_loader.py:neo4j_flow",
        "pool_name": "client-pool",
        "worker_name": "client-worker"
    }
}

async def create_blocks():
    """Create all required blocks"""
    # PostgreSQL Block
    # postgres_block = SqlAlchemyConnector(
    #     connection_url=CONFIG["postgres"]["url"]
    # )

    # await postgres_block.save(CONFIG["postgres"]["block_name"], overwrite=True)
    
    # Neo4j Password Block
    await Secret(value=CONFIG["neo4j"]["password"]).save(
        name="neo4j-password", 
        overwrite=True
    )
    
    print("✅ Blocks created successfully")

async def create_work_pool():
    """Create work pool if it doesn't exist"""
    async with get_client() as client:
        try:
            work_pool = WorkPoolCreate(
                name=CONFIG["deployments"]["pool_name"],
                type="process",
                base_job_template={
                    "job_configuration": {
                        "command": "python -m prefect.engine"
                    },
                    "variables": {
                        "type": "object",
                        "properties": {}
                    }
                }
            )
            await client.create_work_pool(work_pool)
            print(f"✅ Work pool '{CONFIG['deployments']['pool_name']}' created")
        except Exception as e:
            if "already exists" in str(e):
                print(f"ℹ️ Work pool already exists")
            else:
                raise

async def create_deployments():
    """Create deployments for both flows"""
    # Load and deploy postgres flow
    # postgres_module, postgres_func = CONFIG["deployments"]["postgres_flow_path"].split(":")
    # postgres_flow = getattr(import_module(postgres_module.replace("/", ".")), postgres_func)
    # await postgres_flow.deploy(
    #     name="postgres-loader",
    #     work_pool_name=CONFIG["deployments"]["pool_name"],
    #     schedule=IntervalSchedule(interval=timedelta(hours=1)),
    #     parameters={"query": "SELECT * FROM customers LIMIT 100"}
    # )

    # Load and deploy neo4j flow
    module_path, flow_func = CONFIG["deployments"]["neo4j_flow_path"].replace(".py", "").split(":")
    neo4j_flow = getattr(import_module(module_path.replace("/", ".")), flow_func)

    neo4j_flow = await Flow.from_source(
        source=str(Path(__file__).parent / "flows"),  # path to the folder containing your .py files
        entrypoint="neo4j_loader.py:neo4j_flow"       # file and flow function name
    )

    await neo4j_flow.deploy(
        name="neo4j-loader",
        work_pool_name=CONFIG["deployments"]["pool_name"]
    )

    print("✅ Deployments created successfully")

async def start_worker():
    """Start a worker for the pool"""
    import subprocess
    worker_process = subprocess.Popen(
        [
            "prefect", "worker", "start",
            "--pool", CONFIG["deployments"]["pool_name"],
            "--name", CONFIG["deployments"]["worker_name"],
            "--type", "process"
        ]
    )
    print(f"🚀 Worker started for pool '{CONFIG['deployments']['pool_name']}'")
    return worker_process

@flow(name="Setup Client Environment")
async def setup_client_environment():
    """Main setup flow"""
    print("🛠️ Starting client environment setup...")
    
    await create_blocks()
    await create_work_pool()
    await create_deployments()
    worker = await start_worker()
    
    print("🎉 Client environment setup complete!")
    return worker

if __name__ == "__main__":
    asyncio.run(setup_client_environment())