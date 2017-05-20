#HDFS
HDFS_LOCATION = 'hdfs://localhost:9000/lambda/hdfs'
LOCAL_FS_LOCATION = './hdfs'
STREAM_DATA_STORAGE_DIR = 'new'
#Reads all directories with files
BATCH_DATA_INGESTION_DIR = 'master'

#Batch & Streaming Views
RT_B_VIEWS_DB_LOCATION = 'db/LensesDB.db'
GROUP_ID = 'JobsGID'
#STREAMING WINDOW
STREAMING_WINDOW_LENGTH = 10

#KAFKA
RAW_TOPIC = 'job_raw_data'
JSON_TOPIC = 'job_json_data'
#KAFKA_BROKERS = ['192.168.1.111:9092']
KAFKA_BROKERS = '192.168.1.109:9092'

#InfluxDB
INFLUX_DB_LOCATION = 'http://localhost:8086/write?db=production_data_db'
SERIES = 'total_production'

#MongoDB
MONGO_LOCATION = 'mongodb://localhost:27017/'

# Gets the maximum power of both meridian in absolute value
def getMaxPowerMeridian(prescription):
    sph = prescription['sph']
    cyl = prescription['cyl']
    max_power = max(abs(sph), abs(sph + cyl))
    return max_power