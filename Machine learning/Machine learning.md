# Importar las librerias necesarias
Este código importa diversas librerías necesarias para construir y evaluar modelos de Machine Learning utilizando PySpark y Azure ML. A continuación, te explico brevemente para qué se usa cada una:

* Pipeline: Para crear una secuencia de etapas de transformación y modelado.
* SQL Functions: Para manipulación y transformación de datos en DataFrames.
* StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler: Para preprocesamiento de características.
* RandomForestRegressor, LinearRegression, GBTRegressor: Para modelos de regresión.
* RegressionEvaluator: Para evaluar el rendimiento de los modelos de regresión.
* matplotlib.pyplot: Para crear gráficos y visualizaciones.
* shutil, os: Para operaciones con archivos y directorios.
* Workspace, Experiment, Run: Para gestionar componentes en Azure Machine Learning.
* InteractiveLoginAuthentication: Para autenticación en Azure.
* SparkSession: Para iniciar una sesión de Spark.
* Classification y Evaluation: Para construir y evaluar modelos de clasificación.
```python
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

```

## Configuras azure

```python
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
```
##Datos
1. Definir las proporciones de división: Se establecen las proporciones para dividir el DataFrame en conjuntos de entrenamiento, validación y prueba. En este caso, se han asignado las proporciones predeterminadas de 60% para entrenamiento, 20% para validación y 20% para prueba.
2. Ruta al archivo Delta: Se especifica la ruta al archivo Delta. En este caso, se utiliza la variable delta_path, que parece estar compuesta por una ruta base (wasbs_path + blob_origin_path).
3. Leer el DataFrame desde Delta: Se utiliza Spark para leer el DataFrame desde el formato Delta en la ruta especificada.
4. Semilla para la generación de números aleatorios: Se establece una semilla para la generación de números aleatorios. Esta semilla se utiliza para garantizar reproducibilidad en la división aleatoria del DataFrame..
5. Dividir el DataFrame en conjuntos de entrenamiento, validación y prueba: Se utiliza el método randomSplit de PySpark para dividir el DataFrame en los conjuntos de entrenamiento, validación y prueba, según las proporciones establecidas y la semilla aleatoria
6. Mostrar la cantidad de filas en cada conjunto: Se muestra la cantidad de filas en cada uno de los conjuntos (entrenamiento, validación, prueba) utilizando la función count().

```python
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
```
Este código realiza las siguientes acciones:

1. Define las proporciones para dividir el DataFrame en tres conjuntos.
2. Construye la ruta al archivo Delta donde se almacenan los datos.
3. Lee el DataFrame desde el archivo Delta.
4. Define una semilla para asegurar que la división del DataFrame sea reproducible.
5. Divide el DataFrame en conjuntos de entrenamiento, validación y prueba usando las proporciones definidas.
6. Muestra el número de filas en cada uno de estos conjuntos.

Creamos un diccionario llamado Columns
```python
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

```
Crear un indexador para la columna 'Ataque':

Utiliza StringIndexer para transformar la columna categórica "Ataque" en una columna numérica llamada "Ataque_idx".
Esto es útil para preparar datos categóricos para su uso en modelos de Machine Learning que requieren datos numéricos.

* Convertir varias columnas a un vector:

Utiliza VectorAssembler para combinar múltiples columnas en una sola columna vectorial llamada "Ataque2".
La lista Columns contiene los nombres de las columnas que se quieren combinar.

* Crear un pipeline de características:

Define un Pipeline que incluye las etapas del indexador y el vectorizador.
Esto permite aplicar varias transformaciones de manera secuencial y sistemática.

* Ajustar el pipeline a los datos de entrenamiento:

Ajusta (entrena) el pipeline utilizando los datos de entrenamiento train.
Esto genera un modelo de características que se puede aplicar a los datos de entrenamiento y de prueba.

```python
# Crear un indexador para la columna 'Ataque'
indexer_ataque = StringIndexer(inputCol="Ataque", outputCol="Ataque_idx")  # Crea un StringIndexer para convertir la columna categórica 'Ataque' en una columna numérica 'Ataque_idx'

# Finalmente, convertir estas columnas a un vector (junto con otras columnas)
vectorizer = VectorAssembler(inputCols=Columns, outputCol='Ataque2')  # Crea un VectorAssembler para combinar varias columnas en una sola columna vectorial 'Ataque2'

# Crear un pipeline de características con las etapas del indexador y el vectorizador
feature_pipeline = Pipeline(stages=[indexer_ataque, vectorizer])  # Define un Pipeline que incluye las etapas de indexación y vectorización

# Ajustar el pipeline a los datos de entrenamiento
feature_model = feature_pipeline.fit(train)  # Ajusta el pipeline a los datos de entrenamiento y crea un modelo de características
```
# Random Forest
![Animacion](https://github.com/alex-style1007/Anomalias-en-Red/blob/main/Images/Random.gif)
**Random Forest** es un algoritmo de aprendizaje supervisado que se utiliza en machine learning y estadística. Es una combinación (o “ensamble”) de árboles de decisión, generalmente entrenados con el método de “bagging”. La idea principal del bagging es que la combinación de modelos de aprendizaje aumenta el resultado general.

1. ¿Cómo funciona?
* Selección aleatoria de muestras: Para cada árbol en el ensamble, se selecciona una muestra de los datos de entrenamiento con reemplazo (es decir, algunos datos pueden seleccionarse varias veces y otros pueden no seleccionarse en absoluto). Esto se conoce como “bootstrap sample”.
* Construcción del árbol de decisión: Cada árbol se construye de la siguiente manera:
- Se selecciona un subconjunto de características de forma aleatoria.
- Se elige la característica que proporciona la mejor división, según una función objetivo (por ejemplo, minimizar la impureza de Gini).
- Se repiten los pasos anteriores para las ramas generadas por la división, y así sucesivamente hasta que se alcanza un límite predefinido.
2. Predicción: Para una nueva entrada, cada árbol en el ensamble hace una predicción. La predicción final se obtiene por mayoría de votos (para clasificación) o promedio (para regresión).
3. Ventajas
* Robusto a los datos de entrenamiento: Debido a la aleatoriedad en la selección de muestras y características, los Random Forest son menos propensos a sobreajustar los datos de entrenamiento que un solo árbol de decisión.
* Manejo de características no lineales y de interacciones entre características: Al igual que los árboles de decisión, los Random Forest pueden manejar características no lineales e interacciones entre características.
* Importancia de las características: Los Random Forest proporcionan una medida de la importancia de las características, que puede ser útil para la selección de características.
4.Desventajas
* Complejidad y tiempo de entrenamiento: Los Random Forest pueden ser bastante grandes y tomar mucho tiempo para entrenar, especialmente si el número de árboles es grande.
* Interpretabilidad: Aunque un solo árbol de decisión puede ser fácil de interpretar, un Random Forest, como ensamble de muchos árboles, es más difícil de interpretar.
```python
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

```
| Métrica    | Valor |
|------------|-------|
| Accuracy   | 0.95  |
| Precision  | 0.94  |
| Recall     | 0.95  |
| F1 Score   | 0.94  |

# Concatenar características en una sola columna 'features'

Utiliza `VectorAssembler` para combinar todas las características en una sola columna llamada 'features'.

# Definir y entrenar el modelo de red neuronal

![Animacion](https://github.com/alex-style1007/Anomalias-en-Red/blob/main/Images/red.gif)

Una **red neuronal** en ciencia de datos es un modelo inspirado en el funcionamiento del cerebro humano. Está compuesta por unidades de procesamiento, llamadas "neuronas", organizadas en capas. Cada neurona recibe una serie de entradas, las pondera en función de unos pesos (que son los parámetros que aprende el modelo) y aplica una función de activación para producir una salida. La información fluye desde la capa de entrada hasta la capa de salida, posiblemente a través de una o más capas ocultas. El aprendizaje se realiza ajustando los pesos para minimizar una función de pérdida.

En este caso, se esta definiendo la estructura de capas para el modelo de perceptrón multicapa (MLP), que es un tipo de red neuronal. Luego, se entrena el modelo utilizando los datos de entrenamiento.

## Realizar predicciones y evaluar el rendimiento

Realiza predicciones en los conjuntos de validación y prueba. Evalúa el rendimiento del modelo utilizando la precisión (accuracy) en ambos conjuntos.

```python
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
```

# Definir el modelo de árbol de decisión
![Animacion](https://github.com/alex-style1007/Anomalias-en-Red/blob/main/Images/desicion.png)
Un **árbol de decisión** es un modelo de aprendizaje supervisado que se utiliza en machine learning y estadística. Se representa como un árbol donde cada nodo interno representa una característica (o atributo), cada rama representa una regla de decisión y cada hoja representa un resultado, es decir, la predicción del modelo.

El objetivo de un árbol de decisión es dividir los datos en grupos homogéneos basándose en las características. La "mejor" división es la que reduce más la incertidumbre (por ejemplo, minimiza la entropía o la impureza de Gini).

## ¿Cómo funciona?

1. **Elección de la mejor característica**: En cada nodo del árbol, el modelo elige la característica que proporciona la mejor división, según una función objetivo.

2. **División de los datos**: Los datos se dividen en subconjuntos según la regla de decisión asociada a la característica elegida.

3. **Recursión**: Los pasos anteriores se repiten de manera recursiva para cada subconjunto de datos hasta que se alcanza un criterio de parada, como una profundidad máxima del árbol o un número mínimo de muestras por hoja.

En este caso,se esta utilizando `DecisionTreeClassifier` para crear un modelo de árbol de decisión, especificando las columnas de etiquetas y características, así como la profundidad máxima y el número máximo de bins.

## Entrenar el modelo

Entrena el modelo de árbol de decisión utilizando el conjunto de datos de entrenamiento.

## Realizar predicciones y evaluar el rendimiento

Realiza predicciones en los conjuntos de validación y prueba. Evalúa el rendimiento del modelo utilizando la precisión (accuracy) en ambos conjuntos.

```python
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

```
