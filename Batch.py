import glob
import shutil
import json
import sqlite3
import threading
import Utils
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

        dirs=glob.glob(self.streamDir+'/*')
        files= []

        #Read new files coming from speed layer
        for i in dirs:
            dir_files= glob.glob(i+'/p*')
            if len(dir_files) >0:
                file=dir_files[0]
                files.append(file)

        #Move new files to Master directory
        for src in files:
            #Replace 'new' with 'master' on the path
            dest = src.replace('new','master',1)
            #Removes '/' from the path so evey file remains in master directory
            #copies only part-XXXXXX files
            newDir = ''
            li = dest.rsplit('/', 1)
            shutil.move(src, newDir.join(li))
        #Removes speed layer directory contents
        for d in dirs:
            shutil.rmtree(d)


        #Batch layer logic: reads all files in storage and tranforms them into JSON objects
        batch = self.sc.textFile(self.masterDir)
        self.json_objects = batch.map(json.loads)

        #Historic data of products
        products = self.logic(self.json_objects)
        print products.take(3)

        #Save BatchView to DB
        self.saveBView(products)

        #Waits 20 seconds because as there are not a big amount of files the batch process is very fast
        time.sleep(20)

        print "Exiting " + self.name

    def saveBView(self, rdd):
        # Collects the results from the rdd into an array of tuples.
        # As we had to create a string from the product name and type to sum products, now we have to divide both features
        results= rdd.map(lambda z: Utils.productStringAsTuple(z)).collect()
        conn = sqlite3.connect(self.db)
        c = conn.cursor()

        c.execute('''CREATE TABLE IF NOT EXISTS ProductCount_bv (name, type, count)''')
        c.execute("DELETE FROM ProductCount_bv")
        for r in results:
            c.execute('INSERT INTO ProductCount_bv VALUES (?,?,?)', r)
        conn.commit()
        conn.close()

    # Batch layer logic: returns the total amount of each product (historic data)
    def logic(self, rdd):
        products = rdd.map(lambda x: x['design'][0])
        productCount = products.map(lambda x: (Utils.productAsString(x), 1)) \
             .reduceByKey(lambda a, b: a + b) \
             .sortBy(lambda (k, v): -v)
        return productCount