import json
from airflow.models import Variable
from mylibs.aws_secrets import get_secret
from databricks import sql
import requests
import base64
import os

def get_databricks_connection():
    secret_name = Variable.get("AWSName")
    print (secret_name)
    secret_raw = get_secret(secret_name)

    # Parse the JSON and extract the correct key
    try:
        parsed = json.loads(secret_raw)
        token = parsed.get("databricks") 
    except json.JSONDecodeError:
        token = secret_raw  # fallback in case it's raw text

    if not token:
        raise ValueError(f"❌ No token found in AWS secret  {secret_name}")

    host = Variable.get("DATABRICKS_HOST")
    http_path = Variable.get("DATABRICKS_HTTP_PATH")

    try:
        connection = sql.connect(
            server_hostname=host,
            http_path=http_path,
            access_token=token
        )
        print("✅ Databricks connection successful")
        return connection
    except Exception as e:
        print("❌ Failed to connect to Databricks:")
        print(f"🔧 Host: {host}")
        print(f"📡 HTTP Path: {http_path}")
        print(f"🛑 Error: {str(e)}")
        raise


def run_databricks_job(**kwargs):
    
    print ("starting job...")
    secret_name = Variable.get("AWSName")
    print (f"secret name = {secret_name}")
    secret_raw = get_secret(secret_name)
    try:
        parsed = json.loads(secret_raw)
        token = parsed.get("databricks") 
    except json.JSONDecodeError:
        token = secret_raw

    if not token:
        raise ValueError(f"❌ No token found in AWS secret {secret_name}")
    print(f"🧪 Token starts with: {token[:10]}")

    host = Variable.get("DATABRICKS_HOST")
    if not host.startswith("http"):
        host = "https://" + host
    print(f"🌍 Using Databricks host: {host}")

    workspace_path = "/Workspace/Users/edu.efs79@gmail.com/fraud_logreg_job.py"
    local_file_path = "/opt/airflow/dags/scripts/fraud_logreg_job.py"

    upload_to_workspace(local_file_path, workspace_path, host, token)

    # 3. Submit the job
    job_payload = {
        "run_name": "FraudModel_Notebook",
        "existing_cluster_id": "0521-165929-f2utrtse",
        "notebook_task": {
            "notebook_path": "/Workspace/Users/edu.efs79@gmail.com/fraud_logreg_job.py",
            "source": "WORKSPACE"
        }
    }

    headers = {
        "Authorization": f"Bearer {token}"
    }

    url = f"{host}/api/2.1/jobs/runs/submit"
    response = requests.post(url, headers=headers, json=job_payload)

    if response.status_code != 200:
        raise Exception(f"❌ Failed to start job: {response.text}")
    
    run_id = response.json().get("run_id")
    print(f"✅ Databricks job started successfully! Run ID = {run_id}")


def upload_to_dbfs(local_file_path, dbfs_path, host, token):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # Step 1: Read and encode file
    with open(local_file_path, "rb") as f:
        data = f.read()
        encoded = base64.b64encode(data).decode("utf-8")

    # Step 2: Upload
    put_url = f"{host}/api/2.0/dbfs/put"
    payload = {
        "path": dbfs_path,
        "overwrite": True,
        "contents": encoded
    }

    response = requests.post(put_url, headers=headers, json=payload)
    if response.status_code != 200:
        raise Exception(f"❌ Failed to upload to DBFS: {response.text}")

    print(f"✅ Script uploaded to {dbfs_path}")

def upload_to_workspace(local_path, workspace_path, host, token):
    with open(local_path, "rb") as f:
        content = base64.b64encode(f.read()).decode("utf-8")

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    payload = {
        "path": workspace_path,
        "format": "SOURCE",
        "language": "PYTHON",
        "overwrite": True,
        "content": content
    }

    url = f"{host}/api/2.0/workspace/import"
    response = requests.post(url, headers=headers, json=payload)

    if response.status_code != 200:
        raise Exception(f"❌ Workspace upload failed: {response.text}")
    
    print(f"✅ Uploaded script to Workspace path: {workspace_path}")