import os
import subprocess
import asyncio
import time
import httpx
from prefect.blocks.system import Secret
from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import WorkPoolCreate

# Set Prefect API URL
os.environ["PREFECT_API_URL"] = "http://127.0.0.1:4200/api"

def wait_for_server(url, timeout=60):
    print(f"⏳ Waiting for Prefect server at {url}...")
    start = time.time()
    while time.time() - start < timeout:
        try:
            r = httpx.get(url)
            if r.status_code == 200:
                print("✅ Prefect server is up.")
                return
        except Exception:
            pass
        time.sleep(2)
    raise Exception("❌ Prefect server not responding in time.")

async def create_blocks():
    print("🔧 Creating blocks...")
    await Secret(value="gw6xdLdKC7ftrlyjT6p5FH9-1NcrOlkNUgaDBSIRJ7c").save(
        name="neo4j-password", overwrite=True
    )
    print("✅ Blocks created.")

async def create_work_pool():
    print("🛠️ Creating work pool...")
    async with get_client() as client:
        pools = await client.read_work_pools()
        names = [pool.name for pool in pools]
        if "client-pool" not in names:
            await client.create_work_pool(
                WorkPoolCreate(
                    name="client-pool",
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
            )
            print("✅ Work pool 'client-pool' created.")
        else:
            print("ℹ️ Work pool 'client-pool' already exists.")

def start_prefect_server():
    print("🚀 Starting Prefect server...")
    subprocess.Popen(
        ["prefect", "server", "start", "--host", "0.0.0.0"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    time.sleep(5)

def deploy_flows():
    print("🚀 Deploying flows...")
    subprocess.run(["prefect", "deploy"], check=True)

def start_worker():
    print("👷 Starting worker...")
    subprocess.Popen(["prefect", "worker", "start", "--pool", "client-pool"])

async def main():
    start_prefect_server()
    wait_for_server("http://127.0.0.1:4200/api/health")
    await create_blocks()
    await create_work_pool()
    deploy_flows()
    start_worker()

if __name__ == "__main__":
    asyncio.run(main())
