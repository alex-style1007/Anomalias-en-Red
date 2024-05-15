# Importar las librerias necesarias
from pyspark.ml import Pipeline  # Importa la clase Pipeline de PySpark ML para crear flujos de trabajo de Machine Learning
from pyspark.sql.functions import *  # Importa todas las funciones de SQL de PySpark para manipulación de DataFrames
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler  # Importa transformadores para preprocesamiento de datos
from pyspark.ml.regression import RandomForestRegressor, LinearRegression, GBTRegressor  # Importa modelos de regresión de PySpark ML
from pyspark.ml.evaluation import RegressionEvaluator  # Importa la clase para evaluar modelos de regresión
import matplotlib.pyplot as plt  # Importa Matplotlib para la visualización de datos
import shutil  # Importa el módulo shutil para operaciones de archivo y directorio
import os  # Importa el módulo os para interactuar con el sistema operativo
from azureml.core import Workspace, Experiment, Run  # Importa clases de Azure ML para gestionar espacios de trabajo, experimentos y ejecuciones
from azureml.core.authentication import InteractiveLoginAuthentication  # Importa el método de autenticación interactiva de Azure ML
from pyspark.sql import SparkSession  # Importa la clase SparkSession para crear sesiones de Spark
from pyspark.ml.classification import *  # Importa todas las clases de clasificación de PySpark ML
from pyspark.ml.evaluation import *  # Importa todas las clases de evaluación de PySpark ML


# Información para acceder a Azure Storage
blob_account_name = ""
blob_container_name = ""
blob_origin_path = "Golden"
blob_sas_token = r""

spark.conf.set(f'fs.azure.sas.{blob_container_name}.{blob_account_name}.blob.core.windows.net', blob_sas_token)

# Configuración de SparkSession
spark = SparkSession.builder \
    .appName("AzureBlobStorage") \
    .config(f"fs.azure.account.key.{blob_account_name}.blob.core.windows.net", "") \
    .getOrCreate()
wasbs_path = f"wasbs://{blob_container_name}@{blob_account_name}.blob.core.windows.net/"

# Definir las proporciones de división
split_defaults = [.6, .2, .2]  # Define las proporciones para dividir el DataFrame en entrenamiento, validación y prueba (60%, 20%, 20%)
# Ruta al archivo Delta
delta_path = wasbs_path + blob_origin_path  # Construye la ruta completa al archivo Delta combinando el prefijo wasbs_path y el blob_origin_path
# Leer el DataFrame desde Delta
df = spark.read.format("delta").load(delta_path)  # Lee el DataFrame desde un archivo Delta usando Spark
# Semilla para la generación de números aleatorios
random_seed = 35092  # Define una semilla para asegurar la reproducibilidad en la división aleatoria del DataFrame
# Dividir el DataFrame en conjuntos de entrenamiento, validación y prueba
train, valid, test = df.randomSplit(split_defaults, seed=random_seed)  # Divide el DataFrame en tres conjuntos: entrenamiento, validación y prueba según las proporciones definidas
# Mostrar la cantidad de filas en cada conjunto
display(train.count(), valid.count(), test.count())  # Muestra el número de filas en cada uno de los conjuntos (entrenamiento, validación y prueba)

Columns =['Protocol',
 'Fwd_Pkt_Len_Max',
 'Fwd_Pkt_Len_Min',
 'Fwd_Pkt_Len_Mean',
 'Fwd_Pkt_Len_Std',
 'Bwd_Pkt_Len_Max',
 'Bwd_Pkt_Len_Min',
 'Bwd_Pkt_Len_Mean',
 'Flow_Pkts/s',
 'Fwd_Pkts/s',
 'Bwd_Pkts/s',
 'Pkt_Len_Min',
 'Pkt_Len_Max',
 'Pkt_Len_Mean',
 'Pkt_Len_Std',
 'PSH_Flag_Cnt',
 'Pkt_Size_Avg',
 'Fwd_Seg_Size_Avg',
 'Bwd_Seg_Size_Avg',
 'Init_Fwd_Win_Byts',
 'Init_Bwd_Win_Byts',
 'Fwd_Seg_Size_Min','Ataque']

# Añadir una nueva columna o modificar una existente
train = train.withColumn("Ataque", col("Ataque"))  # Crea o reemplaza la columna "Ataque" en el DataFrame de entrenamiento con sus valores actuales


# Crear un indexador para la columna 'Ataque'
indexer_ataque = StringIndexer(inputCol="Ataque", outputCol="Ataque_idx")  # Crea un StringIndexer para convertir la columna categórica 'Ataque' en una columna numérica 'Ataque_idx'

# Finalmente, convertir estas columnas a un vector (junto con otras columnas)
vectorizer = VectorAssembler(inputCols=Columns, outputCol='Ataque2')  # Crea un VectorAssembler para combinar varias columnas en una sola columna vectorial 'Ataque2'

# Crear un pipeline de características con las etapas del indexador y el vectorizador
feature_pipeline = Pipeline(stages=[indexer_ataque, vectorizer])  # Define un Pipeline que incluye las etapas de indexación y vectorización

# Ajustar el pipeline a los datos de entrenamiento
feature_model = feature_pipeline.fit(train)  # Ajusta el pipeline a los datos de entrenamiento y crea un modelo de características

# Crear el modelo RandomForestClassifier
forest = RandomForestClassifier(labelCol="Ataque_idx", featuresCol="Ataque2")  # Define un modelo RandomForestClassifier utilizando 'Ataque_idx' como la columna de etiquetas y 'Ataque2' como la columna de características

# Crear el pipeline
pipeline = Pipeline(stages=[indexer_ataque, vectorizer, forest])  # Crea un Pipeline que incluye las etapas de indexación, vectorización y el clasificador RandomForest

# Entrenar el modelo
modelo = pipeline.fit(train)  # Ajusta el pipeline a los datos de entrenamiento para entrenar el modelo

# Realizar predicciones en el conjunto de validación
prediccion = modelo.transform(valid)  # Aplica el modelo entrenado al conjunto de validación para hacer predicciones

# Evaluar el modelo
evaluador = MulticlassClassificationEvaluator(labelCol="Ataque_idx", predictionCol="prediction")  # Crea un evaluador para clasificaciones multiclase utilizando 'Ataque_idx' como la columna de etiquetas y 'prediction' como la columna de predicciones

# Calcular y mostrar métricas
exactitud = evaluador.evaluate(prediccion, {evaluador.metricName: "accuracy"})  # Calcula la exactitud del modelo en el conjunto de validación
precision = evaluador.evaluate(prediccion, {evaluador.metricName: "weightedPrecision"})  # Calcula la precisión ponderada del modelo
recall = evaluador.evaluate(prediccion, {evaluador.metricName: "weightedRecall"})  # Calcula el recall ponderado del modelo
f1_score = evaluador.evaluate(prediccion, {evaluador.metricName: "f1"})  # Calcula la puntuación F1 ponderada del modelo

# Imprimir métricas
print("Accuracy: {:.2f}".format(exactitud))  # Imprime la exactitud del modelo
print("Precision: {:.2f}".format(precision))  # Imprime la precisión del modelo
print("Recall: {:.2f}".format(recall))  # Imprime el recall del modelo
print("F1 Score: {:.2f}".format(f1_score))  # Imprime la puntuación F1 del modelo

# Obtener etiquetas únicas
unique_labels = df.select('Ataque').distinct().collect()  # Selecciona las etiquetas únicas de la columna 'Ataque' del DataFrame original

# Imprimir las etiquetas únicas
for row in unique_labels:
    print(row['Ataque'])  # Imprime cada etiqueta única en la columna 'Ataque'


# Concatenar las características en una sola columna 'features'
feature_columns = df.columns[:-1]  # Obtener todas las columnas excepto la última ('Ataque')
assembler = VectorAssembler(inputCols=feature_columns, outputCol='features')  # Utilizar VectorAssembler para concatenar las características en una sola columna 'features'

# Transformar los conjuntos de datos de entrenamiento, validación y prueba
train_assembled = assembler.transform(train)  # Transformar el conjunto de entrenamiento
valid_assembled = assembler.transform(valid)  # Transformar el conjunto de validación
test_assembled = assembler.transform(test)    # Transformar el conjunto de prueba

# Definir el modelo de red neuronal
num_classes = len(df.select('Ataque').distinct().collect())  # Obtener el número de clases
layers = [len(feature_columns), 64, 32, num_classes]  # Definir la estructura de capas de la red neuronal (entrada, ocultas, salida)
mlp = MultilayerPerceptronClassifier(layers=layers, seed=1234, labelCol='Ataque', featuresCol='features')  # Crear el modelo de red neuronal

# Entrenar el modelo
model = mlp.fit(train_assembled)  # Entrenar el modelo utilizando el conjunto de entrenamiento

# Realizar predicciones en el conjunto de validación
valid_predictions = model.transform(valid_assembled)  # Realizar predicciones en el conjunto de validación

# Evaluar el rendimiento del modelo en el conjunto de validación
evaluator = MulticlassClassificationEvaluator(labelCol='Ataque', metricName='accuracy')  # Utilizar el evaluador de clasificación multiclase para calcular la precisión
accuracy = evaluator.evaluate(valid_predictions)  # Calcular la precisión en el conjunto de validación
print(f'Accuracy on validation set: {accuracy}')  # Imprimir la precisión en el conjunto de validación

# Realizar predicciones en el conjunto de prueba
test_predictions = model.transform(test_assembled)  # Realizar predicciones en el conjunto de prueba

# Evaluar el rendimiento del modelo en el conjunto de prueba
accuracy_test = evaluator.evaluate(test_predictions)  # Calcular la precisión en el conjunto de prueba
print(f'Accuracy on test set: {accuracy_test}')  # Imprimir la precisión en el conjunto de prueba


# Definir el modelo de árbol de decisión
dt = DecisionTreeClassifier(labelCol='Ataque', featuresCol='features', maxDepth=5, maxBins=32)  # Crea un modelo de árbol de decisión con las columnas de etiquetas y características, y especifica la profundidad máxima y el número máximo de bins

# Entrenar el modelo
dt_model = dt.fit(train_assembled)  # Entrena el modelo de árbol de decisión utilizando el conjunto de datos de entrenamiento

# Realizar predicciones en el conjunto de validación
valid_predictions = dt_model.transform(valid_assembled)  # Realiza predicciones en el conjunto de datos de validación

# Evaluar el rendimiento del modelo en el conjunto de validación
evaluator = MulticlassClassificationEvaluator(labelCol='Ataque', metricName='accuracy')  # Crea un evaluador de clasificación multiclase para calcular la precisión
accuracy = evaluator.evaluate(valid_predictions)  # Calcula la precisión en el conjunto de datos de validación
print(f'Accuracy on validation set: {accuracy}')  # Imprime la precisión en el conjunto de datos de validación

# Realizar predicciones en el conjunto de prueba
test_predictions = dt_model.transform(test_assembled)  # Realiza predicciones en el conjunto de datos de prueba

# Evaluar el rendimiento del modelo en el conjunto de prueba
accuracy_test = evaluator.evaluate(test_predictions)  # Calcula la precisión en el conjunto de datos de prueba
print(f'Accuracy on test set: {accuracy_test}')  # Imprime la precisión en el conjunto de datos de prueba

