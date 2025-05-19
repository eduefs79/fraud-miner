import json
from airflow.models import Variable
from mylibs.aws_secrets import get_secret
from databricks import sql

def get_databricks_connection():
    secret_raw = get_secret("databricks/token")

    # Parse the JSON and extract the correct key
    try:
        parsed = json.loads(secret_raw)
        token = parsed.get("databricks") 
    except json.JSONDecodeError:
        token = secret_raw  # fallback in case it's raw text

    if not token:
        raise ValueError("❌ No token found in AWS secret 'databricks/token'")

    print(f"🧪 Token starts with: {token[:10]}")

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

