import time
import sqlite3
import json
import os
import datetime
import Utils
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

        #Spark Streaming Context
        self.ssc= StreamingContext(sc, windowlen)

    def run(self):
        print "Starting " + self.name
        self.ssc.checkpoint("checkpoint")
        #Kafka connection
        kvs = KafkaUtils.createDirectStream(self.ssc, [self.topic], {"metadata.broker.list": self.brokers})

        # kafka emits tuples: json data comes at the second position of the tuple
        # Besides, the data is formatted into JSON
        self.json_objects = kvs.map(lambda z: json.loads(z[1])).cache()
        #self.json_objects.pprint()

        #As data is going to be stored as plain text, JSON must be formatted
        # into string representation
        toHDFS = self.json_objects.map(lambda z: json.dumps(z))



        #New data to be processed through the speed layer
        self.stream = StreamClass.logic(self.json_objects)
        self.stream.pprint()
        self.stream.foreachRDD(self.saveRTView1)

        #arranca el flujo2 aunque el primer ciclo no se tiene en cuenta
        #self.stream2 = StreamClass.logic(self.kvs)
        #self.stream2.foreachRDD(self.saveRTView2)


        # Master dataset storage
        #toHDFS.foreachRDD(self.saveStream)

        self.ssc.start()
        self.ssc.awaitTermination()



    #Speed layer logic: builds a temporal series of products (name, type, count, timestamp)
    #TODO


    # Speed layer logic: returns the total amount of each product along the window interval (sorted DESC)
    @staticmethod
    def logic(stream):
        products = stream.map(lambda x: x['design'][0])
        productCount = products.map(lambda x: (Utils.productAsString(x), 1))\
            .reduceByKey(lambda a, b: a + b)\
            .transform(lambda x: x.sortBy(lambda (k, v): -v))
        return productCount

    #Contador total de disegnos
    # @staticmethod
    # def logic(stream):
    #     designs = stream.map(lambda x: x['design'][0])
    #     designCount = designs.map(lambda x: (Utils.designsAsStrings(x), 1)) \
    #         .reduceByKey(lambda a, b: a + b) \
    #         .transform(lambda x: x.sortBy(lambda (k, v): -v))
    #     return designCount

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
            prefix = os.path.join(StreamClass.streamDir, datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
            rdd.saveAsTextFile(prefix)

    # Updates la RT view 1 with the results of the logic() function on every RDD from the DStream
    @staticmethod
    def saveRTView1(rdd):
        #Collects the results from the rdd into an array of tuples.
        #As we had to create a string from the product name and type to sum products, now we have to divide both features
        results= rdd.map(lambda z: Utils.productStringAsTuple(z)).collect()

        conn = StreamClass.dbConnection()
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS ProductCount_rt1 (name, type, count)''')
        for r in results:
            #Check if the product (name, type) already exists within rt_view
            c.execute('SELECT * FROM ProductCount_rt1 WHERE name =? AND type =?',tuple(r[0:2]))
            data = c.fetchall()
            found = len (data) > 0
            if not found:
                c.execute('INSERT INTO ProductCount_rt1 VALUES (?,?,?)',r)
                # print "New row at ProductCount_rt1 "+ str(r)
            else:
                row = data[0]
                #Creation od the record to update the DB. The query shows it is: (count, name, type)
                update=[]
                update.append(int(row[2]) + int(r[2]))
                update.append(row[0])
                update.append(row[1])
                c.execute('UPDATE ProductCount_rt1 SET count =? WHERE name=? AND type =?', update)
                # print "Updating row at ProductCount_rt1 "+ str(update)

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
