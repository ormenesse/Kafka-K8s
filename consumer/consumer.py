# consumer.py
from kafka import KafkaConsumer
import json
consumer = KafkaConsumer(
    'posts',
    bootstrap_servers=['localhost:9093'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)
# note that this for loop will block forever to wait for the next message
#for message in consumer:
#    print(message.value)
while True:
    try:
        records = consumer.poll(60 * 1000) # timeout
        record_list = []
        for tp, consumer_records in records.items():
            for consumer_record in consumer_records:
                record_list.append(consumer_record.value)
        print(record_list)
    except:
        print('Erro')
