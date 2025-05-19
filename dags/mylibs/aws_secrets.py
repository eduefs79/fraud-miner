from airflow.hooks.base import BaseHook
import boto3

def get_secret(secret_name: str, aws_conn_id: str = "aws_s3_conn_fraud", region_name: str = "us-east-2") -> str:
    try:
        conn = BaseHook.get_connection(aws_conn_id)
        print(f"🔐 Retrieving secret '{secret_name}' using connection '{aws_conn_id}' in region '{region_name}'")

        session = boto3.session.Session(
            aws_access_key_id=conn.login,
            aws_secret_access_key=conn.password,
            region_name=region_name
        )

        client = session.client("secretsmanager")
        response = client.get_secret_value(SecretId=secret_name)

        print("✅ Secret retrieved successfully")
        return response["SecretString"]

    except Exception as e:
        print("❌ Failed to retrieve secret from AWS Secrets Manager")
        print(f"🔑 Secret name: {secret_name}")
        print(f"🌍 Region: {region_name}")
        print(f"🛑 Error: {str(e)}")
        raise

