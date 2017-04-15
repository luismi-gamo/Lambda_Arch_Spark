import requests
import random
from kafka import KafkaConsumer
import json
import datetime
import time
import calendar

INFLUX_DB_LOCATION = 'http://localhost:8086/write?db=mydb'
#Manages Influx DB operations
# Format of the data passed to the Influx new point function
#  {"timestamp":"2017-04-14T12:57:10.772Z",
#   "product_id":682,
#   "prescription":{"sph":14,"cyl":-11,"axis":109,"Add":3},
#   "design":[{"name":"FreeStyle","type":"D"}],
#   "lab":[{"name":"Greiche&Scaff","country":"Canada"}]}

def getInfluxDataFromProductDictionary(dict, series):
    #Este que trae del json data creator no vale, esta fuera de rango ???
    timestamp = dict['timestamp']
    date = datetime.datetime.now()
    print date
    prod_name = 'product_name="' + dict['design'][0]['name']+'"'
    prod_type = 'product_type="' + dict['design'][0]['type']+'"'
    lab_name = 'laboratory_name="' + dict['lab'][0]['name']+'"'
    lab_country = 'laboratory_country="' + dict['lab'][0]['country']+'"'
    data = series + ','+prod_name+','+prod_type+ ','+lab_name+','+lab_country + ' value=1 ' + str(int(timestamp * 1e6))
    return data


def postNewPoint(theurl, theseries, thedata):
    # POST some form-encoded data:
    post_data = getInfluxDataFromProductDictionary(thedata, series=theseries)
    print 'Posting: \t' + post_data
    post_response = requests.post(url=theurl, data=post_data)


# def getData(theurl):
#     get_response = requests.get(url=theurl)


SERIES = 'influx_kafka'
PRICES_DB_LOCATION = '../db/Prices.db'

if __name__ == "__main__":
    # To consume latest messages and auto-commit offsets
    consumer = KafkaConsumer('LensJobs',
                         group_id='LensJobsGID',
                         bootstrap_servers=['192.168.1.111:9092'])
    #print datetime.datetime.fromtimestamp(1492184973393)
    for message in consumer:
        postNewPoint(INFLUX_DB_LOCATION, SERIES, json.loads(message.value))



