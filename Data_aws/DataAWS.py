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
import boto3  # Importación de la biblioteca boto3 para interactuar con Amazon S3
from pyspark.sql.functions import lit

# Creación de una instancia de cliente S3 de boto3
s3 = boto3.client('s3', aws_access_key_id='', aws_secret_access_key='') 
# Anulación de la firma de solicitud para evitar errores de autenticación
s3._request_signer.sign = (lambda *args, **kwargs: None) 
# Inicialización de un diccionario para almacenar los datos
data = {}

# Recorrido de los objetos en el bucket de S3 'cse-cic-ids2018'
for obj in s3.list_objects_v2(Bucket='cse-cic-ids2018')['Contents']:
    # Verificación y procesamiento de los archivos específicos
    if(len(f"{obj['Key']}".split('Processed Traffic Data for ML Algorithms/'))>1 and f"{obj['Key']}".split('Processed Traffic Data for ML Algorithms/')[1] != ''):
        fileName = f"{obj['Key']}".split('Processed Traffic Data for ML Algorithms/')[1]
        print(fileName.split('_')[0])  # Impresión del nombre del archivo
        print(f"{obj['Key']} - {obj['Size']/1048576} MB")  # Impresión del nombre y tamaño del archivo 
        # Lectura del archivo CSV desde S3 y carga en un DataFrame de Spark
        df = spark.read.format("csv").option("header", "true").load(f"s3a://cse-cic-ids2018/{obj['Key']}")
        data[fileName.split('_')[0]] = df  # Almacenamiento del DataFrame en el diccionario bajo la clave del nombre del archivo
