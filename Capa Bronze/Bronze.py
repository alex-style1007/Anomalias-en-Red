from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *   # Importación de funciones de Spark SQL

# Información de acceso a Azure Storage
blob_account_name = "nombre de tu cuenta"
blob_container_name = "nombre del contenedor"
blob_origin_path = "aws" #carpeta donde guardamos los datos de aws
blob_destination_path = "bronze" #carpeta donde guardaremos los datos en la capa bronze
blob_sas_token = r"Sas token"

# Configuración de la conexión utilizando el token SAS
spark.conf.set(f'fs.azure.sas.{blob_container_name}.{blob_account_name}.blob.core.windows.net', blob_sas_token)

# Configuración de SparkSession
spark = SparkSession.builder \
    .appName("AzureBlobStorage") \
    .config(f"fs.azure.account.key.{blob_account_name}.blob.core.windows.net", "") \
    .getOrCreate()

# Construcción de la ruta de acceso al Blob Storage
wasbs_path = f"wasbs://{blob_container_name}@{blob_account_name}.blob.core.windows.net/"

# Lectura de datos desde el Blob Storage en formato Parquet y filtrado de registros
data = spark.read.parquet(wasbs_path + blob_origin_path).where("Label!='Label'")

# 1. Exploración de datos aun de una forma muy abstracta

display(data)

data.printSchema()

display(data.describe().show())

#2. Generar un esquema para los datos

# definicion del equema original 
Original_schema = StructType([
    StructField('Dst Port', IntegerType(), True),
    StructField('Protocol', IntegerType(), True),
    StructField('Timestamp', TimestampType(), True),
    StructField('Flow Duration', IntegerType(), True),
    StructField('Tot Fwd Pkts', IntegerType(), True),
    StructField('Tot Bwd Pkts', IntegerType(), True),
    StructField('TotLen Fwd Pkts', DoubleType(), True),
    StructField('TotLen Bwd Pkts', DoubleType(), True),
    StructField('Fwd Pkt Len Max', IntegerType(), True),
    StructField('Fwd Pkt Len Min', IntegerType(), True),
    StructField('Fwd Pkt Len Mean', DoubleType(), True),
    StructField('Fwd Pkt Len Std', DoubleType(), True),
    StructField('Bwd Pkt Len Max', IntegerType(), True),
    StructField('Bwd Pkt Len Min', IntegerType(), True),
    StructField('Bwd Pkt Len Mean', DoubleType(), True),
    StructField('Bwd Pkt Len Std', DoubleType(), True),
    StructField('Flow Byts/s', DoubleType(), True),
    StructField('Flow Pkts/s', DoubleType(), True),
    StructField('Flow IAT Mean', DoubleType(), True),
    StructField('Flow IAT Std', DoubleType(), True),
    StructField('Flow IAT Max', DoubleType(), True),
    StructField('Flow IAT Min', DoubleType(), True),
    StructField('Fwd IAT Tot', DoubleType(), True),
    StructField('Fwd IAT Mean', DoubleType(), True),
    StructField('Fwd IAT Std', DoubleType(), True),
    StructField('Fwd IAT Max', DoubleType(), True),
    StructField('Fwd IAT Min', DoubleType(), True),
    StructField('Bwd IAT Tot', DoubleType(), True),
    StructField('Bwd IAT Mean', DoubleType(), True),
    StructField('Bwd IAT Std', DoubleType(), True),
    StructField('Bwd IAT Max', DoubleType(), True),
    StructField('Bwd IAT Min', DoubleType(), True),
    StructField('Fwd PSH Flags', IntegerType(), True),
    StructField('Bwd PSH Flags', IntegerType(), True),
    StructField('Fwd URG Flags', IntegerType(), True),
    StructField('Bwd URG Flags', IntegerType(), True),
    StructField('Fwd Header Len', IntegerType(), True),
    StructField('Bwd Header Len', IntegerType(), True),
    StructField('Fwd Pkts/s', DoubleType(), True),
    StructField('Bwd Pkts/s', DoubleType(), True),
    StructField('Pkt Len Min', IntegerType(), True),
    StructField('Pkt Len Max', IntegerType(), True),
    StructField('Pkt Len Mean', DoubleType(), True),
    StructField('Pkt Len Std', DoubleType(), True),
    StructField('Pkt Len Var', DoubleType(), True),
    StructField('FIN Flag Cnt', IntegerType(), True),
    StructField('SYN Flag Cnt', IntegerType(), True),
    StructField('RST Flag Cnt', IntegerType(), True),
    StructField('PSH Flag Cnt', IntegerType(), True),
    StructField('ACK Flag Cnt', IntegerType(), True),
    StructField('URG Flag Cnt', IntegerType(), True),
    StructField('CWE Flag Count', IntegerType(), True),
    StructField('ECE Flag Cnt', IntegerType(), True),
    StructField('Down/Up Ratio', DoubleType(), True),
    StructField('Pkt Size Avg', DoubleType(), True),
    StructField('Fwd Seg Size Avg', DoubleType(), True),
    StructField('Bwd Seg Size Avg', DoubleType(), True),
    StructField('Fwd Byts/b Avg', DoubleType(), True),
    StructField('Fwd Pkts/b Avg', DoubleType(), True),
    StructField('Fwd Blk Rate Avg', DoubleType(), True),
    StructField('Bwd Byts/b Avg', DoubleType(), True),
    StructField('Bwd Pkts/b Avg', DoubleType(), True),
    StructField('Bwd Blk Rate Avg', DoubleType(), True),
    StructField('Subflow Fwd Pkts', IntegerType(), True),
    StructField('Subflow Fwd Byts', IntegerType(), True),
    StructField('Subflow Bwd Pkts', IntegerType(), True),
    StructField('Subflow Bwd Byts', IntegerType(), True),
    StructField('Init Fwd Win Byts', IntegerType(), True),
    StructField('Init Bwd Win Byts', IntegerType(), True),
    StructField('Fwd Act Data Pkts', IntegerType(), True),
    StructField('Fwd Seg Size Min', IntegerType(), True),
    StructField('Active Mean', DoubleType(), True),
    StructField('Active Std', DoubleType(), True),
    StructField('Active Max', DoubleType(), True),
    StructField('Active Min', DoubleType(), True),
    StructField('Idle Mean', DoubleType(), True),
    StructField('Idle Std', DoubleType(), True),
    StructField('Idle Max', DoubleType(), True),
    StructField('Idle Min', DoubleType(), True),
    StructField('Label', StringType(), True),
    StructField('date', StringType(), True)
])


# Lectura de los datos utilizando el esquema original
data = spark.read.schema(Original_schema).parquet(wasbs_path + blob_origin_path)

# Conversión del campo 'Timestamp' al formato adecuado
data = data.withColumn("Timestamp", to_timestamp(data["Timestamp"], "dd/MM/yyyy HH:mm:ss"))

dataTypes = {'StringType()': 'string', 'DoubleType()': 'double', 'IntegerType()': 'integer', 'TimestampType()': 'timestamp'} #diccionario con tipos de datos
data = data.select([col(field.name).cast(dataTypes[str(field.dataType)]).alias(field.name) for field in Original_schema.fields]) #transformacion de los datos

#3. Guardar datos en databricks
# Directorio raíz para los datos de destino
destDataDirRoot = f"/mnt/bigdata/nombre de usuario/carpeta raiz/bronze/tmpdata/"

# Escribir los datos en formato Parquet, con modo de anexado y particionado por la columna 'date'
data.write.format("parquet").mode("append").partitionBy("date").save(destDataDirRoot)

# Mostrar los archivos en el directorio de destino
display(dbutils.fs.ls(destDataDirRoot))

#4. Lectura de datos con el equema corregido

# Lectura de los datos en formato Parquet, aplicando el esquema original y configurando opciones
data = spark.read.format("parquet").schema(Original_schema).option("header", True).load(destDataDirRoot).cache()

# Mostrar los datos filtrados por la columna 'date' con valor 'Friday-23-02-2018'
display(data.where("date='Friday-23-02-2018'"))

#5. Guardar los datos en azure

# Escribir los datos en formato Parquet, con modo de anexado y particionado por la columna 'date'
data.write.format("parquet").mode("append").partitionBy("date").save(wasbs_path + blob_destination_path)

# Mostrar los archivos en el directorio de destino en Azure Blob Storage
display(dbutils.fs.ls(wasbs_path + blob_destination_path))
