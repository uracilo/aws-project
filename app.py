import streamlit as st
import boto3
import json

st.set_page_config(page_title="Dashboard Clientes", page_icon="📊", layout="wide")

st.title("📊 Dashboard de Clientes — xideralaws-curso-project")

# 1️⃣ Configuración de S3
bucket = "xideralaws-curso-project"
key = "data/summary.json"

# 2️⃣ Leer JSON desde S3
s3 = boto3.client("s3")
try:
    obj = s3.get_object(Bucket=bucket, Key=key)
    data = json.loads(obj["Body"].read())
except Exception as e:
    st.error(f"No se pudo leer el archivo de S3: {e}")
    st.stop()

# 3️⃣ Mostrar métricas generales
st.subheader("📅 Fecha de procesamiento")
st.write(data["fecha_procesamiento"])

st.metric("Total de registros procesados", data["total_registros"])

# 4️⃣ Mostrar Top Ciudades
st.subheader("🏙️ Ciudades más frecuentes")
for c in data["top_ciudades"]:
    st.write(f"• {c['ciudad']}: {c['count']} registros")

# 5️⃣ Mostrar Top Nombres
st.subheader("👤 Nombres más comunes")
for n in data["top_nombres"]:
    st.write(f"• {n['nombre']}: {n['count']} registros")

st.caption("Datos obtenidos desde S3 (bucket: xideralaws-curso-project)")
