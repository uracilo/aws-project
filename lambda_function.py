import os
import json
import boto3
import pandas as pd
import sqlalchemy
from datetime import datetime

def lambda_handler(event, context):
    # Parámetros de conexión
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASS")
    db_host = os.getenv("DB_HOST")
    db_name = os.getenv("DB_NAME")
    s3_bucket = "mi-bucket-etl"
    
    # 1️⃣ Conexión a la base de datos
    engine = sqlalchemy.create_engine(f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}")
    query = "SELECT * FROM clientes;"  # Ejemplo
    
    df = pd.read_sql(query, engine)
    
    # 2️⃣ Convertir a Parquet
    file_name = f"clientes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    local_path = f"/tmp/{file_name}"
    df.to_parquet(local_path, index=False)
    
    # 3️⃣ Subir a S3
    s3 = boto3.client("s3")
    s3.upload_file(local_path, s3_bucket, f"raw_data/{file_name}")
    
    return {
        "statusCode": 200,
        "body": json.dumps(f"Archivo {file_name} subido a s3://{s3_bucket}/raw_data/")
    }
