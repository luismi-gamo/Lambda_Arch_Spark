import requests
from kafka import KafkaConsumer
import json
import datetime
import time
import Definitions
###########################
### WORKS WITH json-data-generator running LensSimConfig.json
###########################

def generateWebJob(inputmessage, series):
    # InfluxDB works with nanosecond timestamps. Python uses miliseconds
    timestamp = str(int(inputmessage['timestamp'] * 1e6))
    lab_id = 'lab_id="' + inputmessage['lab_id'] + '"'
    #Total data to be posted: the timestamp and the lab
    data = series + ',' + lab_id + ' value=1 ' + timestamp
    return data


def postNewPoint(theurl, theseries, thedata):
    # POST some form-encoded data:
    post_data = generateWebJob(thedata, series=theseries)
    print 'Posting: \t' + post_data
    post_response = requests.post(url=theurl, data=post_data)

if __name__ == "__main__":
    # To consume latest messages and auto-commit offsets
    consumer = KafkaConsumer(Definitions.RAW_TOPIC,
                         group_id='LensJobsGID',
                         bootstrap_servers=[Definitions.KAFKA_BROKERS])

    #print datetime.datetime.fromtimestamp(1492184973393)
    for message in consumer:
        postNewPoint(Definitions.INFLUX_DB_LOCATION, Definitions.SERIES, json.loads(message.value))



