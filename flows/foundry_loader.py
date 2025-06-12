from prefect import flow, task
import requests
from prefect.blocks.system import Secret

@task
def deploy_model(model_id: str):
    api_key = Secret.load("ai-foundry-key").get()
    url = f"https://your-ai-foundry-instance.com/api/v1/models/{model_id}/deploy"
    
    response = requests.post(
        url,
        headers={"Authorization": f"Bearer {api_key}"},
        json={"config": {"env": "production"}}
    )
    response.raise_for_status()
    return response.json()

@flow
def ai_foundry_deployment_flow(model_id: str):
    result = deploy_model(model_id)
    print(f"Deployment status: {result['status']}")