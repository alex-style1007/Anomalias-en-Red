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


