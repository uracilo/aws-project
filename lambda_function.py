import os
import json
import boto3
import pandas as pd
import sqlalchemy
from datetime import datetime

def lambda_handler(event, context):
    # 🔐 Variables de entorno (defínelas en la Lambda)
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASS")
    db_host = os.getenv("DB_HOST")
    db_name = os.getenv("DB_NAME")
    
    s3_bucket = "xideralaws-curso-project"
    s3_prefix = "raw_data"

    # 1️⃣ Conexión a base de datos (ejemplo MySQL)
    engine = sqlalchemy.create_engine(f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}")
    query = "SELECT * FROM clientes;"  # ajusta según tu tabla
    
    df = pd.read_sql(query, engine)

    # 2️⃣ Generar nombre de archivo Parquet
    file_name = f"clientes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    local_path = f"/tmp/{file_name}"

    # 3️⃣ Escribir en Parquet con compresión ZSTD
    df.to_parquet(local_path, index=False, compression="zstd", engine="pyarrow")

    # 4️⃣ Subir a S3
    s3 = boto3.client("s3")
    s3.upload_file(local_path, s3_bucket, f"{s3_prefix}/{file_name}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": f"Archivo {file_name} subido correctamente a s3://{s3_bucket}/{s3_prefix}/",
            "rows": len(df)
        })
    }
