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
        self.json_objects.pprint()

        #As data is going to be stored as plain text, JSON must be formatted
        # into string representation
        toHDFS = self.json_objects.map(lambda z: json.dumps(z))

        # #New data to be processed through the speed layer
        # self.stream = StreamClass.logic(self.json_objects)
        # self.stream.pprint()
        # self.stream.foreachRDD(self.saveRTView1)
        #
        # #Second flow, not taken into account until 2nd loop
        # self.stream2 = StreamClass.logic(self.json_objects)
        # #self.stream2.pprint()
        # self.stream2.foreachRDD(self.saveRTView2)
        #
        #
        # Master dataset storage
        toHDFS.foreachRDD(self.saveStream)

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

    #Signals the end of the batch process, and RTViews must be changed
    def batchFinish(self):

        self.countS1 = self.countS1 +1
        if self.countS1 == 2:
            self.active = 2
            self.removeRTView('ProductCount_rt1')
            self.countS1 = 0

        if self.countS2 == -1:
            print "Arranca flujo2"
            self.removeRTView('ProductCount_rt2')

        self.countS2 = self.countS2 +1
        if self.countS2 == 2:
            self.active = 1
            self.removeRTView('ProductCount_rt2')
            self.countS2 = 0


    #Removes content from a table
    def removeRTView(self, table):
        conn = sqlite3.connect(self.db)
        c = conn.cursor()
        c.execute("DELETE FROM '"+table+"' ")
        conn.commit()
        conn.close()


    #Saves new data into a temporal dir so batch layer can process it at the next iteration
    @staticmethod
    def saveStream(rdd):
       if rdd.count() > 0:
            prefix = os.path.join(StreamClass.streamDir, datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
            rdd.saveAsTextFile(prefix)

    # Updates the RT view 1 with the results of the logic() function on every RDD from the DStream
    @staticmethod
    def saveRTView1(rdd):
        #Collects the results from the rdd into an array of tuples.
        #As we had to create a string from the product name and type to sum products, now we have to divide both features
        results= rdd.map(lambda z: Utils.productStringAsTuple(z)).collect()
        StreamClass.saveRTView(results,'ProductCount_rt1')

    # Updates the RT view 2 with the results of the logic() function on every RDD from the DStream
    @staticmethod
    def saveRTView2(rdd):
        # Collects the results from the rdd into an array of tuples.
        # As we had to create a string from the product name and type to sum products, now we have to divide both features
        results = rdd.map(lambda z: Utils.productStringAsTuple(z)).collect()
        StreamClass.saveRTView(results, 'ProductCount_rt2')

    @staticmethod
    def saveRTView(results, table):
        conn = StreamClass.dbConnection()
        c = conn.cursor()
        c.execute('CREATE TABLE IF NOT EXISTS ' + table + ' (name CHAR(200), type CHAR(50), count INT)')
        for r in results:
            #Check if the product (name, type) already exists within rt_view
            c.execute("SELECT * FROM " + table + " WHERE name = '"+r[0]+"' AND type = '"+r[1]+"'")
            data = c.fetchall()
            found = len (data) > 0
            if not found:
                #Insertion order: (name, type,count)
                c.execute("INSERT INTO " + table + " VALUES ('"+r[0]+"','"+r[1]+"',"+str(r[2])+")")
                # print "New row at ProductCount_rt1 "+ str(r)
            else:
                row = data[0]
                #Creation od the record to update the DB.
                c.execute("UPDATE " + table + " SET count = " + str(int(row[2]) + int(r[2])) + " WHERE name = '"+ str(row[0]) + "' AND type = '" + str(row[1])+"'")
                # print "Updating row at ProductCount_rt1 "+ str(update)

        conn.commit()
        conn.close()
