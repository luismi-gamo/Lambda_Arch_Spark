from kafka import KafkaConsumer

# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer('LaboratoryJobs',
                         group_id='LensJobsGID',
                         bootstrap_servers=['192.168.1.111:9092'])
# consumer = KafkaConsumer('LensJobs',
#                         group_id='LensJobsGID',
#                         bootstrap_servers=['192.168.1.111:9092'])

for message in consumer:
       print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value))