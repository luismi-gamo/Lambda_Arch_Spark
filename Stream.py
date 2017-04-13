import time
import sqlite3
import json
import os
import datetime
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


import threading

class StreamClass (threading.Thread):

    # Singleton for DB connections
    bd = None
    # Singleton for HDFS
    streamDir = None

    @staticmethod
    def dbConnection():
        return sqlite3.connect(StreamClass.bd)

    def __init__(self, sc, streamDir, db, topic, brokers, windowlen):
        threading.Thread.__init__(self)
        self.name = "Stream"
        #Sets the streaming data storage directory
        self.streamDir = streamDir
        if StreamClass.streamDir is None:
            StreamClass.streamDir = streamDir
            print "Output dir for streaming data set to: " + StreamClass.streamDir
        #Sets the DB parameters
        self.db = db
        if StreamClass.bd is None:
            StreamClass.bd = db
        #sets Kafka parameters
        self.topic = topic
        self.brokers = brokers

        #Streams for operation
        self.stream = None
        self.stream2 = None
        self.json_objects= None

        #para llevar la cuenta de los batchs views de cada flujo
        self.countS1=0
        self.countS2=-1

        #al iniciar el flujo activo es el 1
        self.active=1

        #crear contexto Spark Streaming
        self.scc= StreamingContext(sc, windowlen)

    def run(self):
        print "Starting " + self.name

        #Kafka connection
        kvs = KafkaUtils.createDirectStream(self.scc, [self.topic], {"metadata.broker.list": self.brokers})

        # kafka emits tuples: json data comes at the second position of the tuple
        # Besides, the data is formatted into JSON
        self.json_objects = kvs.map(lambda z: json.loads(z[1])).cache()
        self.json_objects.pprint()

        #As data is going to be stored as plain text, JSON must be formatted
        # into string representation
        toHDFS = self.json_objects.map(lambda z: json.dumps(z))



        #nuevos datos para ser procesados por la capa de streaming (speed layer)
        #self.stream = StreamClass.logic(self.kvs)
        #self.stream.pprint()
        #self.stream.foreachRDD(self.saveRTView1)

        #arranca el flujo2 aunque el primer ciclo no se tiene en cuenta
        #self.stream2 = StreamClass.logic(self.kvs)
        #self.stream2.foreachRDD(self.saveRTView2)


        #flujo para almacenar en master dataset
        toHDFS.foreachRDD(self.saveStream)

        self.scc.start()
        self.scc.awaitTermination()



    #Speed layer logic
    @staticmethod
    def logic(stream):
        lines = stream.map(lambda x: x[1]) \
                    .flatMap(lambda x: x.split(" ")) \
                    .map(lambda w: (w,1)) \
                    .reduceByKey(lambda a,b: a+b)
        return lines

    def active_view(self):
        if self.active==1:
            return "worcount_rt1"
        else:
            return "worcount_rt2"

    def batchFinish(self):
        print "Se notifica a la speed layer que Batch finish"
        print "counts1: " +str(self.countS1)
        print "counts2: " +str(self.countS2)

        self.countS1 = self.countS1 +1
        if self.countS1 ==2:
            self.active=2
            self.removeRTView(1)
            self.countS1=0

        if self.countS2==-1:
            print "Arranca flujo2"
            self.removeRTView(2)



        self.countS2 = self.countS2 +1
        if self.countS2 ==2:
            self.active=1
            self.removeRTView(2)
            self.countS2=0



    def removeRTView(self, i):
        conn = sqlite3.connect(self.db)
        c = conn.cursor()
        table='wordcount_rt'+str(i)
        c.execute("DELETE FROM '%s' " %table)


    #Saves new data into a temporal dir so batch layer can process it at the next iteration
    @staticmethod
    def saveStream(rdd):
       if rdd.count() > 0:
            #prefix = StreamClass.streamDir+ str(time.time())
            prefix = os.path.join(StreamClass.streamDir, datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
            print prefix
            rdd.saveAsTextFile(prefix)

    #
    # Actualiza la RT view con los resultados de los datos en streaming en cada RDD del DStream
    #
    @staticmethod
    def saveRTView1(rdd):

        results= rdd.collect()
        conn = StreamClass.dbConnection()
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS wordcount_rt1 (word, count)''')
        for r in results:
            #comprobar si ya existe ese ID en la rt_view
            id= [r[0]]
            c.execute('SELECT * FROM wordcount_rt1 WHERE word =?',id)
            data = c.fetchall()
            existe = len (data) > 0

            if not existe:
                c.execute('INSERT INTO wordcount_rt1 VALUES (?,?)',r)
                print "Creando fila en wordcount_rt1 "+ str(r)
            else:
                row= data[0]
                update=[]
                update.append(row[0])
                update.append (row[1]+r[1])
                c.execute('UPDATE wordcount_rt1 SET count =? WHERE word=?', update)
                print "Actualizando fila en wordcount_rt1 "+ str(update)

        conn.commit()
        conn.close()

    #
    # Actualiza la RT view con los resultados de los datos en streaming en cada RDD del DStream
    #
    @staticmethod
    def saveRTView2(rdd):

        results= rdd.collect()
        conn = StreamClass.dbConnection()
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS wordcount_rt2 (word, count)''')
        for r in results:
            #comprobar si ya existe ese ID en la rt_view
            id= [r[0]]
            c.execute('SELECT * FROM wordcount_rt2 WHERE word =?',id)
            data = c.fetchall()
            existe = len (data) > 0

            if not existe:
                c.execute('INSERT INTO wordcount_rt2 VALUES (?,?)',r)
                print "Creando fila en wordcount_rt2 "+ str(r)
            else:
                row= data[0]
                update=[]
                update.append(row[0])
                update.append (row[1]+r[1])
                c.execute('UPDATE wordcount_rt2 SET count =? WHERE word=?', update)
                print "Actualizando fila en wordcount_rt2 "+ str(update)

        conn.commit()
        conn.close()
