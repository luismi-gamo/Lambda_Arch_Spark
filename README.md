# Arquitectura Lambda en PySpark
Se han establecido como objetivos dos casos de uso típicos del mundo del Big Data:
* Poner en marcha un sistema de monitorización en tiempo real de la producción de lentes de una empresa.
* Poner en marcha un sistema de análisis de datos hibrido (batch + streaming) mediante una arquitectura lambda.

Ademas se desarrollará una capa de visualización, para facilitar el acceso al estudio de los datos.

# Requisitos para la puesta en marcha del sistema
Instalar y/o arrancar los siguientes servicios:
* MongoDB [Instalar en Ubuntu](https://docs.mongodb.com/manual/tutorial/install-mongodb-on-ubuntu/)
> sudo service mongod start

* HDFS [Instalar en Ubuntu](https://www.digitalocean.com/community/tutorials/how-to-install-hadoop-in-stand-alone-mode-on-ubuntu-16-04)
> start-dfs.sh

* Zookeeper [Instalar Zookeeper y Kafka en Ubuntu](https://devops.profitbricks.com/tutorials/install-and-configure-apache-kafka-on-ubuntu-1604-1/)
> bin/zookeeper-server-start.sh config/zookeeper.properties

* Kafka
>  bin/kafka-server-start.sh config/server.properties

* InfluxDB [Instalar](https://docs.influxdata.com/influxdb/v1.2/introduction/installation/)
> sudo service influxd start
> GUI -> http://localhost:8083

* Spark 1.6 o superior [Instalar en Ubuntu](http://www.informit.com/articles/article.aspx?p=2755929&seqNum=3)

* Python 2.7

# Señal de entrada
## Simulacion de entrada
Los datos llegan al sistema a través de un topic de Kafka, denominado ‘job_raw_data’, alimentado por la aplicacion [***json-data-generator***.](https://github.com/acesinc/json-data-generator/releases)

Este programa envia a Kafka un mensaje JSON con los datos del trabajo, y utiliza como ficheros de configuración:
* Configuración de la señal: _JobsSimConfig.json_
* Contenido del mensaje: _JobsWorkflow.json_

Para ejecutar la simulación:
> java -jar json-data-generator-1.2.1.jar JobsSimConfig.json

## Ingesta de datos
El proceso iniciado lanza las señales de entrada al sistema, pero han de ser procesadas por ___JobETL.py___ antes de poder ser tratadas por la capa de streaming.

Para lanzar el proceso encargado de la ETL:
> /usr/bin/python2.7 $LAMBDA_DIR/JobETL.py

___JobETL.py___  leerá continuamente mensajes del topic de entrada y los procesará de dos maneras diferentes, dependiendo del tipo de uso que se le dará a los datos:
* Generará una cadena con los datos formateados adecuadamente para ser enviados a InfluxDB mediante una petición HTTP. Estos serán los datos usados en la monitorización de la producción de los laboratorios.
* Generará un objeto JSON que pueda ser analizado adecuadamente en los procesos batch y streaming de la arquitectura lambda. Este objeto será depositado en otro topic de Kafka, denominado ‘job_json_data’.


# Arquitectura lambda
Para poner en marcha la arquitectura lambda hay que ejecutar el comando:
> spark-submit --jars /opt/spark-1.6.2-bin-hadoop2.6/lib/spark-streaming-kafka-assembly_2.10-1.6.2.jar Manager.py


# Visualizacion

## Distribución de índices de refracción en función de la potencia

Arrancar el servicio __Superset__ [Instalar](http://airbnb.io/superset/installation.html#)
> superset runserver -p 9999

Conectarse con el navegador -> http://localhost:9999/

## Monitorización en tiempo real de la producción
Arrancar el servicio __Grafana__ [Instalar en Debian](http://docs.grafana.org/installation/debian/)
> sudo service grafana-server start

Conectarse con el navegador -> http://localhost:3000

