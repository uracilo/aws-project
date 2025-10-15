import streamlit as st
import boto3
import json

st.title("📈 Dashboard de Clientes")

s3 = boto3.client("s3")
bucket_name = "xideralaws-curso-project"
key = "data/summary.json"

# Leer JSON desde S3
obj = s3.get_object(Bucket=bucket_name, Key=key)
data = json.loads(obj["Body"].read())

st.subheader("🏙️ Ciudades más frecuentes")
for c in data["top_ciudades"]:
    st.write(f"{c['ciudad']}: {c['count']} registros")

st.subheader("👤 Nombres más comunes")
for n in data["top_nombres"]:
    st.write(f"{n['nombre']}: {n['count']} registros")

st.caption(f"Último procesamiento: {data['fecha_procesamiento']}")
