from pyspark.sql import SparkSession  # Importación de SparkSession para acceder a Azure storage

# Información de la cuenta de almacenamiento Azure Blob
blob_account_name = "Nombre del blob"
blob_container_name = "Nombre del container"
blob_relative_path = "raw" #donde escribiremos
blob_container_key = "llave del contenedor"
blob_sas_token = r"sas token"

# Permitir a Spark leer un Blob
wasbs_path = f'wasbs://{blob_container_name}@{blob_account_name}.blob.core.windows.net/{blob_relative_path}'
spark.conf.set( f'fs.azure.sas.{blob_container_name}.blob_account_name.blob.core.windows.net' , blob_sas_token)

# Inicialización de SparkSession
spark = SparkSession.builder.appName("AzureBlobStorage")\
    .config(f"fs.azure.account.key.{blob_account_name}.blob.core.windows.net", blob_container_key)\
    .getOrCreate()

# Construir la ruta de acceso al Blob Storage utilizando el protocolo WASBS
wasbs_path = f"wasbs://{blob_container_name}@{blob_account_name}.blob.core.windows.net/"

# 1. Data Extraction
import boto3  # Importa la biblioteca boto3 para interactuar con servicios de AWS (Amazon Web Services).
from pyspark.sql import SparkSession  # Importa la clase SparkSession de PySpark para trabajar con Spark.
                                      # Asume que ya tienes Spark configurado en tu entorno.

s3 = boto3.client('s3', aws_access_key_id='', aws_secret_access_key='')  
# Crea un cliente S3 de AWS con credenciales vacías.
# Nota: Necesitarás proporcionar tus propias credenciales.

s3._request_signer.sign = lambda *args, **kwargs: None  
# Anula la firma de la solicitud para evitar la autenticación.
# Esto se hace para acceder a buckets de S3 públicos o sin autenticación.

data = {}  
# Inicializa un diccionario vacío para almacenar datos procesados.

def process_object(obj):
    key_parts = obj['Key'].split('Processed Traffic Data for ML Algorithms/')
    if len(key_parts) > 1 and key_parts[1] != '':
        file_name = key_parts[1].split('_')[0]
        print(file_name)
        print(f"{obj['Key']} - {obj['Size']/1048576} MB")
        df = spark.read.format("csv").option("header", "true").load(f"s3a://cse-cic-ids2018/{obj['Key']}")
        print(df.count())
        if df.count() < 64:
            print("El DataFrame tiene menos de 64 filas.")
        else:
            data[file_name] = df

for obj in s3.list_objects_v2(Bucket='cse-cic-ids2018')['Contents']:
    process_object(obj)
# Itera sobre los objetos en el bucket 'cse-cic-ids2018'.
    # Procesa cada objeto utilizando la función definida anteriormente.
display(df) #te permitira ver el set de datos


# Configuración del token SAS para la cuenta de almacenamiento Azure Blob
spark.conf.set(f"fs.azure.account.key{blob_account_name}.blob.core.windows.net",blob_sas_token)

# Iteración sobre los DataFrames en el diccionario 'data' y guardado en formato Parquet
for i, df in data.items():
    # Construcción de la ruta de salida para cada DataFrame en formato Parquet
    parquet_path = f"{wasbs_path}/date={i}"
    
    # Escritura del DataFrame en formato Parquet en Azure Blob Storage
    df.write.parquet(parquet_path, mode="overwrite")
    

# Visualización de los archivos en el sistema de archivos de Azure Blob Storage
display(dbutils.fs.ls(wasbs_path))
