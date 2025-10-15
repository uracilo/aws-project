from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
import boto3
import json
from datetime import datetime

# 1️⃣ Inicializar Spark con compresión ZSTD
spark = SparkSession.builder \
    .appName("ETL_Transformacion_Clientes") \
    .config("spark.sql.parquet.compression.codec", "zstd") \
    .config("spark.sql.parquet.filterPushdown", "true") \
    .config("spark.sql.files.maxPartitionBytes", 134217728) \
    .getOrCreate()

# 2️⃣ Paths S3
bucket = "xideralaws-curso-project"
raw_path = f"s3://{bucket}/raw_data/"
data_path = f"s3://{bucket}/data/"

# 3️⃣ Leer todos los archivos Parquet del raw_data
df = spark.read.parquet(raw_path)

# 4️⃣ Calcular frecuencias
freq_ciudades = df.groupBy("ciudad").count().orderBy(col("count").desc())
freq_nombres = df.groupBy("nombre").count().orderBy(col("count").desc())

# 5️⃣ Escribir DataFrame limpio comprimido (ZSTD)
df.write.mode("overwrite").option("compression", "zstd").parquet(f"{data_path}/clientes_limpios.parquet")

# 6️⃣ Crear resumen en JSON
top_ciudades = freq_ciudades.limit(10).toPandas().to_dict(orient="records")
top_nombres = freq_nombres.limit(10).toPandas().to_dict(orient="records")

summary = {
    "fecha_procesamiento": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "total_registros": df.count(),
    "top_ciudades": top_ciudades,
    "top_nombres": top_nombres
}

# 7️⃣ Guardar JSON en S3
s3 = boto3.client("s3")
s3.put_object(
    Bucket=bucket,
    Key="data/summary.json",
    Body=json.dumps(summary, indent=2),
    ContentType="application/json"
)

print("✅ Transformación completada y summary.json guardado en S3.")
