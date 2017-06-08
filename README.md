# Arquitectura Lambda en PySpark
Se han establecido como objetivos dos casos de uso típicos del mundo del Big Data:

* Poner en marcha un sistema de monitorización en tiempo real de la producción de lentes de una empresa.
* Poner en marcha un sistema de análisis de datos hibrido (batch + streaming) mediante una arquitectura lambda.

Ademas se desarrollará una capa de visualización, para facilitar el acceso al estudio de los datos.

En este [enlace](https://www.dropbox.com/s/r8lelw8vo6aa6ds/PresentacionPFM.pdf?dl=0) se puede descargar la presentacion defendida ante el tribunal del Master.

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
Los datos llegan al sistema a través de un topic de Kafka, denominado ‘_job___raw_ __data_’, alimentado por la aplicacion [***json-data-generator***.](https://github.com/acesinc/json-data-generator/releases)

Este programa envia a Kafka un mensaje JSON con los datos del trabajo, y utiliza como ficheros de configuración:

* Configuración de la señal: _JobsSimConfig.json_
* Contenido del mensaje: _JobsWorkflow.json_

Para ejecutar la simulación:
> java -jar json-data-generator-1.2.1.jar JobsSimConfig.json

El formato de los datos de entrada al sistema es un diccionario JSON con los siguientes campos:   

* _timestamp_: instante en el que se generó el trabajo.  
* _lab___id_: licencia del laboratorio.  
* _data_: cadena de bytes con los datos del trabajo. 

> {   
> timestamp: 3434349743,     
> lab_id: 'thelaboratoryid',  
> data: 0x0000FF1A...  
> }

## Ingesta de datos
El proceso iniciado lanza las señales de entrada al sistema, pero han de ser procesadas por ___JobETL.py___ antes de poder ser tratadas por la capa de streaming.

Para lanzar el proceso encargado de la ETL:
> /usr/bin/python2.7 $LAMBDA_DIR/JobETL.py

___JobETL.py___  leerá continuamente mensajes del topic de entrada y los procesará de dos maneras diferentes, dependiendo del tipo de uso que se le dará a los datos:  

* Generará una cadena con los datos formateados adecuadamente para ser enviados a InfluxDB mediante una petición HTTP. Estos serán los datos usados en la monitorización de la producción de los laboratorios.  
* Generará un objeto JSON que pueda ser analizado adecuadamente en los procesos batch y streaming de la arquitectura lambda. Este objeto será depositado en otro topic de Kafka, denominado ‘_job__ _json___data_’.


# Arquitectura lambda
## Datos de entrada
Para el análisis de los datos tanto en la arquitectura lambda (procesos de batch y de streaming), hay que enriquecer el dato de entrada al sistema. El formato de salida del enriquecimiento es otro objeto JSON.  
Para el presente estudio, dicho objeto contendrá datos sobre las prescripciones y sobre el índice de refracción de un trabajo. A continuación se enumeran los campos del objeto JSON:   

* _timestamp_: instante en el que se generó el trabajo. Es el mismo que el del dato raw.  
* _lab___id_: licencia del laboratorio. Es el mismo que el del dato raw.  
* _prescription_: diccionario que define una prescripción. Contiene los campos _sph_, _cyl_, _axis_ y _add_.   
* _index_: índice de refracción

> {   
> timestamp: 3434349743,     
> lab_id: 'thelaboratoryid',   
> prescription: {  
>     sph : 5.25,  
>     cyl: 0.50,  
>     axis: 90,  
>     add: 0  
> },  
> index: '1.67'  
> }

## Puesta en marcha
Para poner en marcha la arquitectura lambda hay que ejecutar el comando:
> spark-submit --jars /opt/spark-1.6.2-bin-hadoop2.6/lib/spark-streaming-kafka-assembly_2.10-1.6.2.jar Manager.py

## Datos de salida
Como salida, la arquitectura lambda produce 3 tablas (colecciones) de MongoDB:  

* Una tabla de la vista de _batch_
* Dos tablas para las vistas generadas por el proceso _streaming_.

Las tres tablas comparten esquema, ya que para la obtencion de la salida final de la arquitectura lambda han de mezclarse la tabla de _batch_ con una de las tablas de _stremaing_. 

Como este proyecto está interesado en obtener la distribucion de potencia de las lentes en funcion del indice de refraccion, el esquema usado es:

* _meridian_: meridiano de maxima potencia de la lente -> max(sph,cyl)  
* _index_: indice de refraccion
* _lab_: id del laboratorio
* _count_: numero de lentes

> {"meridian": 5.75,  
> "index": '1.56',  
> "lab" : 'thelaboratoryid',   
> 'count': 145  
> }


# Monitorizacion de la producción
Para hacer posible la monitorización de la producción de lentes mediante el uso de InfluxDB hay que traducir los datos al formato de entrada adecuado. Dicho formato está descrito en el API HTTP de InfluxDB. 

En nuestro caso, como queremos monitorizar el número total de lentes de cada laboratorio, la información que es necesario incluir en el mensaje es: 

* _serie temporal_: es como una tabla SQL 
* _lab___id_: identificador del laboratorio. Es el mismo que el del dato raw
* _index_: índice de refracción
* _valor_: es 1, porque estamos contando lentes producidas
* _timestamp_: instante en el que se generó el trabajo. Es el mismo que el del dato raw.

La peticion sería algo del tipo: 

> curl -i -XPOST 'http://INFLUX-HOST:8086/write?db=databasename' --data-binary 'serietemporal,lab_id='thelabid',index='1.67' value=1 1434055562000000000'

# Visualizacion

## Distribución de índices de refracción en función de la potencia de la lente

Arrancar el servicio __Superset__ [Instalar](http://airbnb.io/superset/installation.html#)
> superset runserver -p 9999

Conectarse con el navegador -> http://localhost:9999/

## Monitorización en tiempo real de la producción
Arrancar el servicio __Grafana__ [Instalar en Debian](http://docs.grafana.org/installation/debian/)
> sudo service grafana-server start

Conectarse con el navegador -> http://localhost:3000


