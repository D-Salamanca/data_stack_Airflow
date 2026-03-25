# Tarea 2 – FHBD | Pipeline Spark + Iceberg orquestado con Airflow

## Descripción

Pipeline de datos completo que orquesta Apache Spark mediante Apache Airflow para cargar datos en formato **Apache Iceberg** usando **MinIO** como almacenamiento S3-compatible y **Project Nessie** como catálogo.

---

## Estructura del proyecto

```
data_stack_Airflow/
├── dags/
│   └── spark_load_to_iceberg_dag.py   # DAG de Airflow
├── scripts/
│   └── Spark-load-to-Iceberg.py       # Script Spark 
├── notebooks/                         # Notebooks de verificación
├── logs/                              # Logs de Airflow (auto-generado)
├── Dockerfile.airflow                 # Imagen custom de Airflow
├── Dockerfile.spark                   # Imagen custom de Spark
├── docker-compose.yaml                # Todos los servicios
├── spark-defaults.conf                # Configuración Spark
├── requirements.txt                   # Dependencias Python (Jupyter)
└── .env                               # Variables de entorno
```

---

## Servicios y puertos

| Servicio | URL | Credenciales |
|---|---|---|
| Airflow UI | http://localhost:8080 | airflow / airflow |
| Spark Master UI | http://localhost:8081 | — |
| Spark Worker UI | http://localhost:8082 | — |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin123 |
| MinIO S3 API | http://localhost:9000 | — |
| Nessie API | http://localhost:19120 | — |
| Jupyter Notebook | http://localhost:8888 | token: fhbd |
| ClickHouse HTTP | http://localhost:8123 | — |

---

## Inicio rápido

### 1. Clonar el repositorio

```bash
git clone https://github.com/D-Salamanca/data_stack_Airflow.git
cd data_stack_Airflow
```

### 2. Construir y levantar los servicios

```bash
docker compose build --no-cache
docker compose up -d
```

> Esperar ~2 minutos a que `airflow-init` termine antes de abrir la UI.

### 3. Copiar el DAG

```bash
cp spark_load_to_iceberg_dag.py ./dags/
```

---

## ⚠️ Configuración manual obligatoria: Conexión Spark en Airflow

> **IMPORTANTE:** Esta configuración se pierde si se hace `docker compose down -v`. Debe rehacerse cada vez que se reinicie desde cero.

Después de levantar los servicios, ir a Airflow UI → **Admin → Connections → (+)** y crear la conexión con los siguientes valores:

| Campo | Valor |
|---|---|
| **Conn Id** | `spark_default` |
| **Conn Type** | `Spark` |
| **Host** | `spark://spark-master` |
| **Port** | `7077` |

Guardar y verificar que aparece en la lista de conexiones.

---

## Ejecutar el pipeline

1. Abrir Airflow en http://localhost:8080 (airflow / airflow)
2. Buscar el DAG `spark_load_to_iceberg_dag`
3. Activar el toggle (pausa → activo)
4. Hacer clic en **Trigger DAG ▷**
5. Monitorear en **Graph View** y **Logs**

### Flujo del DAG

```
preparar_minio  ──►  run_spark_load_to_iceberg
```

| Task | Descripción |
|---|---|
| `preparar_minio` | Crea los buckets `raw` e `iceberg` en MinIO y descarga/sube el archivo parquet (~600MB, solo la primera vez) |
| `run_spark_load_to_iceberg` | Ejecuta el script Spark que lee el parquet desde MinIO y escribe la tabla en formato Iceberg registrándola en Nessie |

---

## Verificación del pipeline en Jupyter

Abrir Jupyter en **http://localhost:8888** con token `fhbd` y ejecutar las siguientes celdas:

### Celda 1 — Verificar que el bucket `iceberg` tiene datos

```python
import boto3
from botocore.client import Config

s3 = boto3.client(
    "s3",
    endpoint_url="http://fhbd-minio:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin123",
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",
)

# Listar TODO sin prefijo
response = s3.list_objects_v2(Bucket="iceberg")

if "Contents" in response:
    print(f"Archivos encontrados en bucket 'iceberg': {len(response['Contents'])}\n")
    for obj in response["Contents"]:
        print(f"  {obj['Key']}  ({obj['Size']} bytes)")
else:
    print("❌ El bucket 'iceberg' está completamente vacío")
```

### Celda 2 — Verificar que Nessie registró la tabla

```python
import requests

response = requests.get("http://fhbd-nessie:19120/api/v1/trees/tree/main/entries")
data = response.json()

entries = data.get("entries", [])
if entries:
    print(f"✅ Nessie tiene {len(entries)} tabla(s) registrada(s) en rama 'main':\n")
    for entry in entries:
        nombre = ".".join(entry["name"]["elements"])
        tipo = entry["type"]
        print(f"  {tipo}: {nombre}")
else:
    print("❌ No hay tablas registradas en Nessie")
```

### Celda 3 — Ver qué hay en el bucket `raw`

```python
response = s3.list_objects_v2(Bucket="raw")

if "Contents" in response:
    print(f"Archivos en bucket 'raw': {len(response['Contents'])}\n")
    for obj in response["Contents"]:
        print(f"  {obj['Key']}  ({obj['Size']} bytes)")
else:
    print("❌ El bucket 'raw' está vacío")
```

### Celda 4 — Leer y mostrar los datos de Iceberg

> ⚠️ Primero ejecutar la Celda 1 para obtener el prefijo exacto del UUID de la tabla, luego reemplazarlo en el `prefix` de abajo.

```python
import pandas as pd
import io

# Reemplazar con el prefijo real obtenido en la Celda 1
# Ejemplo: "nyc/yellow_tripdata_904d4fca-cd30-4b32-9cfb-780bf6447c12/data/"
prefix = "nyc/yellow_tripdata_<UUID>/data/"

response = s3.list_objects_v2(Bucket="iceberg", Prefix=prefix)

# Leer todos los parquet y concatenarlos
dfs = []
for obj in response["Contents"]:
    print(f"Leyendo: {obj['Key']}")
    raw = s3.get_object(Bucket="iceberg", Key=obj["Key"])
    dfs.append(pd.read_parquet(io.BytesIO(raw["Body"].read())))

df = pd.concat(dfs, ignore_index=True)

print(f"\n✅ Total de filas en tabla Iceberg: {len(df):,}")
print(f"✅ Columnas: {list(df.columns)}")
print(f"\nPrimeras 5 filas:")
df.head(5)
```

### Resultado esperado

| Evidencia | Estado |
|---|---|
| Bucket `raw` tiene el parquet original (~59 MB) | ✅ |
| Bucket `iceberg` tiene 4 archivos de datos parquet | ✅ |
| Bucket `iceberg` tiene `metadata.json` (catálogo Iceberg) | ✅ |
| Bucket `iceberg` tiene snapshot `.avro` | ✅ |
| Nessie registró `ICEBERG_TABLE: nyc.yellow_tripdata` | ✅ |

---

## Reiniciar desde cero

Para limpiar todos los datos y volver a empezar:

```bash
# Bajar contenedores y borrar volúmenes
docker compose down -v

# Levantar de nuevo
docker compose up -d
```

> ⚠️ Después de `down -v` se borra la base de datos de Airflow. Hay que **volver a crear la conexión `spark_default`** manualmente (ver sección de configuración manual).

---

## Stack tecnológico

| Tecnología | Versión | Rol |
|---|---|---|
| Apache Airflow | 2.10.3 | Orquestación del pipeline |
| Apache Spark | 3.5.2 | Procesamiento distribuido |
| Apache Iceberg | 1.6.1 | Formato de tabla open table format |
| Project Nessie | 0.95.0 | Catálogo Iceberg con versionado Git-like |
| MinIO | latest | Almacenamiento S3-compatible (Data Lake) |
| ClickHouse | latest | Motor de consultas analíticas |
| PostgreSQL | 13 | Metadata DB de Airflow |
| Redis | 7.2 | Broker de Celery para Airflow |
| Jupyter | base-notebook | Desarrollo y verificación |
