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

from Batch import BatchClass
from Stream import StreamClass
from Query import QueryClass
import Definitions


if __name__ == "__main__":
    streamDir = os.path.join(Definitions.LOCAL_FS_LOCATION, Definitions.STREAM_DATA_STORAGE_DIR)
    masterDir = os.path.join(Definitions.HDFS_LOCATION, Definitions.BATCH_DATA_INGESTION_DIR)

    sc = SparkContext(appName="LambdaLMG")

    batchF = 1
    streamF = 1
    queryF = 1

    if queryF != 0:
        consulta = QueryClass(Definitions.RT_B_VIEWS_DB_LOCATION,
                              Definitions.MONGO_LOCATION,
                              10)
        consulta.start()
        # while True:
        #     time.sleep(10)
        #     consulta.changeTable()

    if batchF != 0:
        batch = BatchClass(sc, masterDir, streamDir, Definitions.RT_B_VIEWS_DB_LOCATION, Definitions.MONGO_LOCATION)
        batch.start()


    if streamF != 0:
        stream = StreamClass(sc, streamDir,
                             Definitions.RT_B_VIEWS_DB_LOCATION,
                             Definitions.JSON_TOPIC,
                             Definitions.KAFKA_BROKERS,
                             Definitions.STREAMING_WINDOW_LENGTH,
                             Definitions.MONGO_LOCATION)
        stream.start()

    if streamF != 0 and batchF != 0:
        while True:
            if not batch.is_alive():
                stream.batchFinish()
                if queryF != 0:
                    consulta.changeTable()
                batch = BatchClass(sc, masterDir, streamDir, Definitions.RT_B_VIEWS_DB_LOCATION, Definitions.MONGO_LOCATION)
                batch.start()





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