# Lambda_Arch_Spark
Este es el codigo fuente de mi proyecto de master de Big Data.

# Requisitos
Arrancar procesos:
* MongoDB
* HDFS
* Zookeeper
* Kafka
* InfluxDB
* Grafana

# Señal de entrada
Arrancar ___json-data-generator___ con el fichero de
simulacion _JobsSimConfig.json_ y el de workflow _JobsWorkflow.json_

Esto lanza las señales de entrada al sistema, pero han de ser procesadas por ___JobETL.py___
antes de poder ser tratadas por la capa de streaming.

