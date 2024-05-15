# Proyecto de Detección de Anomalías en Red con CSE-CIC-IDS2018

## Descripción del Proyecto

Este proyecto tiene como objetivo desarrollar un sistema de detección de anomalías en la red utilizando el conjunto de datos **CSE-CIC-IDS2018**. Este conjunto de datos, proporcionado por la Communications Security Establishment (CSE) y el Canadian Institute for Cybersecurity (CIC), incluye siete escenarios de ataque diferentes, a saber, Brute-force, Heartbleed, Botnet, DoS, DDoS, ataques web e infiltración de la red desde el interior.

## Conjunto de Datos

El conjunto de datos CSE-CIC-IDS2018 se encuentra en el bucket `cse-cic-ids2018` en AWS. Incluye el tráfico de red y los archivos de registro de cada máquina del lado de la víctima, junto con 80 características de tráfico de red extraídas del tráfico capturado utilizando CICFlowMeter-V3.

## Metodología

El proyecto sigue los siguientes pasos:

1. **Preprocesamiento de Datos**: Los datos se limpian y se normalizan antes de ser utilizados para el entrenamiento del modelo.

2. **Entrenamiento del Modelo**: Se entrena un modelo de aprendizaje automático utilizando los datos de entrenamiento.

3. **Evaluación del Modelo**: El modelo se evalúa utilizando un conjunto de datos de prueba independiente.

4. **Detección de Anomalías**: El modelo entrenado se utiliza para detectar anomalías en nuevos datos de red.

# Estructura de Carpetas del Proyecto

Este proyecto sigue una estructura de carpetas específica que facilita la organización y el flujo de trabajo de los datos. A continuación, se detalla cada una de las carpetas:

## Data AWS

Esta carpeta se utiliza para la **extracción de datos** desde el bucket `cse-cic-ids2018` en AWS. Contiene scripts y notebooks que se utilizan para acceder al bucket, extraer los datos y guardarlos localmente para su posterior procesamiento.

## Bronze

La carpeta `Bronze` representa la **capa Bronze** en la ingeniería de datos. Esta capa se utiliza para el almacenamiento de los datos en bruto tal como se extraen del bucket `cse-cic-ids2018` en AWS. Los datos en esta capa no han sido limpiados ni transformados, y representan el estado original de los datos. Esta carpeta puede contener los datos en bruto, así como scripts y notebooks para la extracción inicial de los datos.

## Silver

La carpeta `Silver` representa la **capa Silver** en la ingeniería de datos. Esta capa se utiliza para el preprocesamiento y la transformación de los datos extraídos. Los datos en esta capa se limpian, se normalizan y se transforman en un formato que puede ser utilizado para el análisis y el modelado. Esta carpeta puede contener scripts y notebooks para el preprocesamiento de datos, así como los datos transformados.

## Golden

La carpeta `Golden` representa la **capa Golden** en la ingeniería de datos. Esta capa se utiliza para el almacenamiento de los datos que están listos para el análisis y el modelado. Los datos en esta capa son los que se utilizan para el entrenamiento y la evaluación de los modelos de machine learning. Esta carpeta puede contener los datos finales en un formato adecuado para el modelado, así como scripts y notebooks para la exploración y el análisis de los datos.

## Machine Learning

La carpeta `Machine Learning` se utiliza para el **entrenamiento de modelos** de machine learning. Contiene scripts y notebooks para la definición, el entrenamiento y la evaluación de los modelos, así como para la realización de predicciones. También puede contener los modelos entrenados y los resultados de las evaluaciones.

Espero que esta descripción te sea útil para entender la estructura de carpetas de este proyecto. 

