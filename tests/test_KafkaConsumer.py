from kafka import KafkaConsumer
import Definitions
# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer(Definitions.RAW_TOPIC,
                         group_id='LensJobsGID',
                         bootstrap_servers=[Definitions.KAFKA_BROKERS])
# consumer = KafkaConsumer('LensJobs',
#                         group_id='LensJobsGID',
#                         bootstrap_servers=['192.168.1.111:9092'])

for message in consumer:
       print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value))