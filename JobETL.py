import random
from kafka import KafkaConsumer
from kafka import KafkaProducer
import json
import requests
import Definitions

#output data, JSON formatted
#[ {"timestamp":"2017-03-03T16:59:01.771Z",
#   "prescription":
#     {"sph":-14,
#     "cyl":9,
#     "axis":88,
#     "Add":1},
#   "index" : 1.5,
#   "labid":"LOA"
# }
# ]

#Creates a random Job depending on the lab
def generateJSONJob(inputmessage):
    timestamp = inputmessage['timestamp']
    labid = inputmessage['lab_id']
    prescription = dict()
    (prescription['prescription'], prescription['index']) = generatePrescriptionAndIndex()
    prescription['timestamp'] = timestamp
    prescription['lab_id'] = labid
    return prescription

def generatePrescriptionAndIndex():
    decimal = [0.0, 0.25, 0.5, 0.75]
    sph = generateDioptres() + decimal[random.randint(0,3)]
    cyl = -abs(generateDioptres() + decimal[random.randint(0,3)])
    add = generateAddition() + decimal[random.randint(0,3)]

    axisvalues =  range(0, 90, 5)
    axis = axisvalues[random.randint(0,len(axisvalues)-1)]

    index = generateIndex(max(abs(sph), abs(sph + cyl)))

    prescription = dict()
    prescription['sph'] = sph
    prescription['cyl'] = cyl
    prescription['axis'] = axis
    prescription['add'] = add

    return (prescription, index)

def generateDioptres():
    number = random.randint(0,100)
    sph = 0
    if number < 6:
        sph = 0
    elif number < 23:
        sph = 1
    elif number < 39:
        sph = 2
    elif number < 54:
        sph = 3
    elif number < 68:
        sph = 4
    elif number < 78:
        sph = 5
    elif number < 86:
        sph = 6
    elif number < 91:
        sph = 7
    elif number < 96:
        sph = 8
    elif number < 98:
        sph = 9
    else:
        sph = 10

    sign = random.randint(0,1)
    if sign == 0:
        return -sph
    else:
        return sph

def generateAddition():
    number = random.randint(0,100)
    sph = 0
    if number < 20:
        sph = 0
    elif number < 47:
        sph = 1
    elif number < 77:
        sph = 2
    else:
        sph = 3
    return sph

def generateIndex(max_power):
    #number = random.randint(0, 3)
    index = '1.5'
    if max_power > 1 and max_power <= 2.5:
        index = '1.53'
    elif max_power <= 4.5:
        index = '1.6'
    elif max_power <= 8:
        index = '1.67'
    elif max_power > 8:
        index = '1.74'

    return index

def generateWebJob(inputmessage, series):
    # InfluxDB works with nanosecond timestamps. Python uses miliseconds
    timestamp = str(int(inputmessage['timestamp'] * 1e6))
    lab_id = 'lab_id="' + inputmessage['lab_id'] + '"'
    #Total data to be posted: the timestamp and the lab
    data = series + ',' + lab_id + ' value=1 ' + timestamp
    return data


if __name__ == "__main__":
    servers = Definitions.KAFKA_BROKERS
    # To consume latest messages and auto-commit offsets
    #reads raw data coming from the server
    consumer = KafkaConsumer(Definitions.RAW_TOPIC,
                             group_id=Definitions.GROUP_ID,
                             bootstrap_servers=servers)
    # To produce JSON messages for the streaming layer
    producer = KafkaProducer(bootstrap_servers=servers)

    for message in consumer:
        #ETL -> JSON and sends the message back to kafka
        json_job = generateJSONJob(json.loads(message.value))
        producer.send(Definitions.JSON_TOPIC, json.dumps(json_job))
        #ETL -> InfluxDB
        web_job = generateWebJob(json.loads(message.value), Definitions.SERIES)
        post_response = requests.post(Definitions.INFLUX_DB_LOCATION, web_job)

        print json_job
        print web_job