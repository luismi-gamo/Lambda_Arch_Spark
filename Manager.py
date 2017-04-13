import time
import sqlite3

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import os.path
import glob
import shutil
from pyspark import SparkContext
import time
import sqlite3

from batch import BatchClass
from Stream import StreamClass

HDFS_LOCATION = 'hdfs'
STREAM_DATA_STORAGE_DIR = 'new'
BATCH_DATA_INGESTION_DIR = 'master'
DB_LOCATION = 'db/LensesDB.db'
WINDOW_LENGTH = 10
TOPIC = 'LensJobs'
BROKERS = '192.168.1.111:9092'

if __name__ == "__main__":
    streamDir = os.path.join(HDFS_LOCATION, STREAM_DATA_STORAGE_DIR)
    # streamDir= '/home/ruben/PycharmProjects/LambdaArchitecture/hdfs/new/'
    # masterDir='/home/ruben/PycharmProjects/LambdaArchitecture/hdfs/master/'
    # db ='/home/ruben/PycharmProjects/LambdaArchitecture/views.db'
    #
    sc = SparkContext(appName="LambdaLMG")
    #
    # batch =  BatchClass(masterDir, streamDir,db, sc)
    stream = StreamClass(sc, streamDir, DB_LOCATION, TOPIC, BROKERS, WINDOW_LENGTH)
    #
    # batch.start()
    stream.start()
    #
    # while True:
    #     if not batch.is_alive():
    #         stream.batchFinish()
    #         batch =  BatchClass(masterDir, streamDir,db, sc)
    #         batch.start()


#input data, JSON formatted
#[ {"timestamp":"2017-03-03T16:59:01.771Z",
# "product_id":2,
# "prescription":
#   {"sph":-14,
#   "cyl":9,
#   "axis":88,
#   "Add":1},
# "design":[
#   {"name":"FreeStyle","type":"D"}
#   ],
# "lab":[
#   {"name":"LOA",
#   "country":"Spain"}
# ]
# }
# ]