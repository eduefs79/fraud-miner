from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook
from databricks import sql
from datetime import datetime
import pandas as pd
import geoip2.database
import os
from mylibs.databricks_utils import get_databricks_connection

# Config
DB_CATALOG = "fraud_miner"
SOURCE_VIEW = f"{DB_CATALOG}.silver.rdv_sat_transaction"
S3_PARQUET_KEY = "silver/sat/enriched_geo.parquet"
GEOIP_S3_KEY = "geoip/db/GeoLite2-City.mmdb"
GEOIP_S3_BUCKET = "fraud-miner"
LOCAL_GEOIP_PATH = "/tmp/GeoLite2-City.mmdb"
LOCAL_PARQUET_PATH = "/tmp/enriched_geo.parquet"


default_args = {
    'start_date': datetime(2025, 1, 1),
    'catchup': False
}

def enrich_with_geoip():
    # Step 1: Download GeoIP DB from S3
    s3 = S3Hook(aws_conn_id="aws_s3_conn_fraud")
    print("📥 Downloading GeoIP DB from S3...")
    s3.get_key(GEOIP_S3_KEY, bucket_name=GEOIP_S3_BUCKET).download_file(LOCAL_GEOIP_PATH)
    print("✅ GeoIP DB downloaded to /tmp")

    connection = get_databricks_connection()

    query = f"SELECT distinct HK_LINK_TRANSACTION, ip_address FROM {SOURCE_VIEW} where ip_address is not null"
    df = pd.read_sql(query, connection)
    print(f"🔍 Fetched {len(df)} rows from {SOURCE_VIEW}")

    # Step 3: GeoIP enrichment
    reader = geoip2.database.Reader(LOCAL_GEOIP_PATH)
    geo_data = []

    for _, row in df.iterrows():
        ip = row['ip_address']
        try:
            r = reader.city(ip)
            geo_data.append({
                'HK_LINK_TRANSACTION': row['HK_LINK_TRANSACTION'],
                'ip_address': ip,
                'geo_country': r.country.name,
                'geo_city': r.city.name,
                'geo_region': r.subdivisions.most_specific.name,
                'geo_lat': r.location.latitude,
                'geo_lon': r.location.longitude
            })
        except Exception:
            geo_data.append({
                'HK_LINK_TRANSACTION': row['HK_LINK_TRANSACTION'],
                'ip_address': ip,
                'geo_country': None,
                'geo_city': None,
                'geo_region': None,
                'geo_lat': None,
                'geo_lon': None
            })

    reader.close()

    # Step 4: Save to local parquet
    df_geo = pd.DataFrame(geo_data)
    df_geo.to_parquet(LOCAL_PARQUET_PATH, index=False)
    print(f"💾 Saved enriched data to {LOCAL_PARQUET_PATH}")

    # Step 5: Upload to S3
    s3.load_file(
        filename=LOCAL_PARQUET_PATH,
        key=S3_PARQUET_KEY,
        bucket_name=GEOIP_S3_BUCKET,
        replace=True
    )
    print(f"✅ Enriched file uploaded to s3://{GEOIP_S3_BUCKET}/{S3_PARQUET_KEY}")

    # print(f"✅ Creating or replacing table fraud_miner.silver.rdv_geoip_enriched_txn")
    cursor = connection.cursor()

    cursor.execute("""
    CREATE OR REPLACE VIEW fraud_miner.silver.rdv_geoip AS
    SELECT * FROM PARQUET.`dbfs:/Volumes/fraud_miner/silver/rdv_sat/enriched_geo.parquet`
    """)

    cursor.execute("""
                   CREATE OR REPLACE VIEW fraud_miner.silver.fraud_geo_view AS
                    SELECT
                    ffv.HK_LINK_TRANSACTION,
                    date_format(ffv.Transaction_Date , 'yyyy-MM-dd HH:mm:ss') AS Transaction_Date,
                    CAST(NULLIF(ffv.Transaction_Amount, 0) AS DOUBLE) AS Transaction_Amount,
                    ffv.Transaction_Currency,
                    ffv.Transaction_IP,
                    ffv.Transaction_Fraud,
                    ffv.Card_Number,
                    ffv.Card_Expiry_Date,
                    ffv.Card_Provider,
                    date_format(ffv.Card_Issued_Date , 'yyyy-MM-dd HH:mm:ss') AS Card_Issued_Date,
                    date_format(ffv.Card_Last_Update , 'yyyy-MM-dd HH:mm:ss') AS Card_Last_Update,
                    CAST(NULLIF(ffv.Card_Credit_Limit, 0) AS DOUBLE) AS Card_Credit_Limit,
                    ffv.Customer_Name,
                    ffv.Customer_Email,
                    ffv.Customer_Address,
                    ffv.Customer_Birth_Date,
                    date_format(ffv.Customer_Last_Update , 'yyyy-MM-dd HH:mm:ss') AS Customer_Last_Update,
                    ffv.Merchant_Address,
                    ffv.Merchant_Category,
                    ffv.Merchant_City,
                    ffv.Merchant_Country,
                    date_format(ffv.Merchant_Created_At , 'yyyy-MM-dd HH:mm:ss') AS Merchant_Created_At,
                    ffv.Merchant_Name,
                    COALESCE(rg.geo_city, 'Unknown') AS geo_city,
                    COALESCE(rg.geo_region, 'Unknown') AS geo_region,
                    CAST(NULLIF(rg.geo_lat, 0) AS DOUBLE) AS geo_lat,
                    CAST(NULLIF(rg.geo_lon, 0) AS DOUBLE) AS geo_lon,
                    COALESCE(rg.geo_country, 'Unknown') AS geo_country
                    FROM
                    fraud_miner.silver.fraud_flat_view ffv
                    INNER JOIN fraud_miner.silver.rdv_geoip rg
                    ON ffv.HK_LINK_TRANSACTION = rg.HK_LINK_TRANSACTION
""")


# DAG definition
with DAG('geoip_enrichment_fraud_flat_pandas_s3',
         schedule_interval=None,
         default_args=default_args,
         description='Enrich fraud_flat_view with GeoIP info using pandas and S3-hosted MMDB',
         tags=['geoip', 'fraud', 'pandas', 's3']) as dag:

    run_geoip = PythonOperator(
        task_id='geoip_enrich_task',
        python_callable=enrich_with_geoip
    )
