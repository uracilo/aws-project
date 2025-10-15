from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
import json
import boto3

# Crear sesión Spark
spark = SparkSession.builder \
    .appName("ETLClientes") \
    .getOrCreate()

# Ruta del bucket S3
s3_bucket = "s3://mi-bucket-etl"
raw_path = f"{s3_bucket}/raw_data/"
data_path = f"{s3_bucket}/data/"

# 1️⃣ Leer datos en formato Parquet
df = spark.read.parquet(raw_path)

# 2️⃣ Calcular frecuencia de ciudades
freq_ciudades = df.groupBy("ciudad").count().orderBy(col("count").desc())
freq_nombres = df.groupBy("nombre").count().orderBy(col("count").desc())

# 3️⃣ Escribir datos procesados a S3/data en formato Parquet
df.write.mode("overwrite").parquet(f"{data_path}/clientes_limpios.parquet")

# 4️⃣ Crear resumen en JSON
top_ciudades = freq_ciudades.limit(5).toPandas().to_dict(orient="records")
top_nombres = freq_nombres.limit(5).toPandas().to_dict(orient="records")

summary = {
    "fecha_procesamiento": "2025-10-15",
    "top_ciudades": top_ciudades,
    "top_nombres": top_nombres
}

# 5️⃣ Guardar JSON en S3
s3 = boto3.client("s3")
s3.put_object(
    Bucket="mi-bucket-etl",
    Key="data/summary.json",
    Body=json.dumps(summary, indent=2),
    ContentType="application/json"
)
