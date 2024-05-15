```python
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
```
# 1. Exploración de datos aun de una forma muy abstracta
```python
display(data)

data.printSchema()

display(data.describe().show())
```
#2. Generar un esquema para los datos
```python
# definicion del equema original 
Original_schema = StructType([
    StructField('Dst Port', IntegerType(), True),
    StructField('Protocol', IntegerType(), True),
    StructField('Timestamp', TimestampType(), True),
    ...
    StructField('Label', StringType(), True),
    StructField('date', StringType(), True)
])

# Lectura de los datos utilizando el esquema original
data = spark.read.schema(Original_schema).parquet(wasbs_path + blob_origin_path)

# Conversión del campo 'Timestamp' al formato adecuado
data = data.withColumn("Timestamp", to_timestamp(data["Timestamp"], "dd/MM/yyyy HH:mm:ss"))

dataTypes = {'StringType()': 'string', 'DoubleType()': 'double', 'IntegerType()': 'integer', 'TimestampType()': 'timestamp'} #diccionario con tipos de datos
data = data.select([col(field.name).cast(dataTypes[str(field.dataType)]).alias(field.name) for field in Original_schema.fields]) #transformacion de los datos
```
#3. Guardar datos en databricks
```python
# Directorio raíz para los datos de destino
destDataDirRoot = f"/mnt/bigdata/nombre de usuario/carpeta raiz/bronze/tmpdata/"

# Escribir los datos en formato Parquet, con modo de anexado y particionado por la columna 'date'
data.write.format("parquet").mode("append").partitionBy("date").save(destDataDirRoot)

# Mostrar los archivos en el directorio de destino
display(dbutils.fs.ls(destDataDirRoot))
```
#4. Lectura de datos con el equema corregido
```python
# Lectura de los datos en formato Parquet, aplicando el esquema original y configurando opciones
data = spark.read.format("parquet").schema(Original_schema).option("header", True).load(destDataDirRoot).cache()

# Mostrar los datos filtrados por la columna 'date' con valor 'Friday-23-02-2018'
display(data.where("date='Friday-23-02-2018'"))
```
#5. Guardar los datos en azure
```python
# Escribir los datos en formato Parquet, con modo de anexado y particionado por la columna 'date'
data.write.format("parquet").mode("append").partitionBy("date").save(wasbs_path + blob_destination_path)

# Mostrar los archivos en el directorio de destino en Azure Blob Storage
display(dbutils.fs.ls(wasbs_path + blob_destination_path))
