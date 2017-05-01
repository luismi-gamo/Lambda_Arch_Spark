import glob
import shutil
import json
import sqlite3
import threading
import datetime
import os
import Definitions
import time
from pyspark import SparkContext


class BatchClass (threading.Thread):

    def __init__(self, sc, masterDir, streamDir, db):
        threading.Thread.__init__(self)
        self.name = "Batch"
        self.streamDir = streamDir
        self.masterDir = masterDir
        self.db =db
        self.sc= sc
        self.json_objects = None

    def run(self):
        print "Starting " + self.name

        #files will store the list of files processed to be sent to HDFS
        #so we can remove them later
        dirs=glob.glob(self.streamDir+'/*')
        files= []
        #Read new files coming from speed layer
        for i in dirs:
            dir_files= glob.glob(i+'/p*')
            if len(dir_files) >0:
                file = dir_files[0]
                files.append(file)
        #print files

        #reads the contents of the files to be sento to HDFS
        #and writes the content to a single file in HDFS.
        if files:
            toHDFS = self.sc.textFile(','.join(files))
            prefix = os.path.join(self.masterDir, datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
            toHDFS.saveAsTextFile(prefix)
            #print toHDFS.collect()

            #Removes speed layer directory contents
            for d in dirs:
                shutil.rmtree(d)


        # #Batch layer logic: reads all files in storage and tranforms them into JSON objects
        batch = self.sc.textFile(self.masterDir+'/*')
        self.json_objects = batch.map(json.loads)
        #print self.json_objects.take(5)
        #Historic data of products
        indexes = self.indexLogic(self.json_objects)
        print indexes.take(5)
        powers = self.powerLogic(self.json_objects)
        print powers.take(5)
        #Save BatchViews to DB
        self.saveIndexesBView(indexes)
        self.savePowersBView(powers)

        #Waits 20 seconds because as there are not a big amount of files the batch process is very fast
        time.sleep(20)

        print "Exiting " + self.name


    def saveIndexesBView(self, rdd):
        # Collects the results from the rdd into an array of tuples with format:
        # ((index,lab),count) -> (index,lab,count)
        results = rdd.map(lambda z: (z[0][0], z[0][1], z[1])).collect()
        conn = sqlite3.connect(self.db)
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS IndexCount_bv  (n_index CHAR(10), lab CHAR(50), count INT)''')
        c.execute("DELETE FROM IndexCount_bv")
        for r in results:
            c.execute('INSERT INTO IndexCount_bv VALUES (?,?,?)', r)
        conn.commit()
        conn.close()

    def savePowersBView(self, rdd):
        # Collects the results from the rdd into an array of tuples with format:
        # ((meridian, index,lab),count) -> (meridian, index,lab,count)
        results = rdd.map(lambda z: (z[0][0], z[0][1], z[0][2], z[1])).collect()
        conn = sqlite3.connect(self.db)
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS PowerCount_bv  (meridian INT, n_index CHAR(10), lab CHAR(50), count INT)''')
        c.execute("DELETE FROM PowerCount_bv")
        for r in results:
            c.execute('INSERT INTO PowerCount_bv VALUES (?,?,?,?)', r)
        conn.commit()
        conn.close()

    # Batch layer logic for index distribution by lab: returns the total amount of each index for each lab
    #The tuple format for reducing is: ((u'1.67', u'Vitolen'), 44)
    def indexLogic(self, rdd):
        byindex = rdd.map(lambda x: ((x['index'], x['lab_id']), 1)) \
            .reduceByKey(lambda a, b: a + b) \
            .sortBy(lambda (k, v): -v)
        return byindex

    # Batch layer logic for power distribution by lab nad index: returns the total amount of each max_meridian for each index and lab
    # The tuple format for reducing is:((5.5, u'1.67', u'LOA'), 10)
    def powerLogic(self, rdd):
        byindex = rdd.map(lambda x: ((Definitions.getMaxPowerMeridian(x['prescription']),x['index'], x['lab_id']), 1))\
            .reduceByKey(lambda a, b: a + b) \
            .sortBy(lambda (k, v): -v)
        return byindex


