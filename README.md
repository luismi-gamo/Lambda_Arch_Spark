# Lambda_Arch_Spark
Este es el codigo fuente de mi proyecto de master de Big Data.

# Requisitos
Arrancar procesos:
* MongoDB
> sudo service mongod start

* HDFS
> start-dfs.sh

* Zookeeper
> bin/zookeeper-server-start.sh config/zookeeper.properties

* Kafka
>  bin/kafka-server-start.sh config/server.properties

* InfluxDB
> sudo service influxd start
> GUI -> http://localhost:8083


# Señal de entrada
## Simulacion de entrada
La simulacion usa la aplicacion ___json-data-generator___ para generar
un con el fichero de
simulacion _JobsSimConfig.json_ y el de workflow _JobsWorkflow.json_
> java -jar json-data-generator-1.2.1.jar JobsSimConfig.json

Esto lanza las señales de entrada al sistema, pero han de ser procesadas por ___JobETL.py___
antes de poder ser tratadas por la capa de streaming.
> /usr/bin/python2.7 $LAMBDA_DIR/JobETL.py

# Puesta en marcha
Para poner en marcha la arquitectura lambda hay que ejecutar el comando:
> spark-submit --jars /opt/spark-1.6.2-bin-hadoop2.6/lib/spark-streaming-kafka-assembly_2.10-1.6.2.jar Manager.py


# Visualizacion

## Superset

Arrancar el servicio
> superset runserver -p 9999

Conectarse con el navegador a http://localhost:9999/

## Grafana
Arrancar el servicio
> sudo service grafana-server start

Conectarse con el navegador a http://localhost:3000

