# producer.py
from kafka import KafkaProducer
from datetime import datetime
import json
producer = KafkaProducer(
    bootstrap_servers=['localhost:9093'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
print('Sending Message to Kafka...')
producer.send('posts', {'author': 'Orma', 'content': 'Kafka is cool! Blabla!', 'created_at': datetime.now().isoformat()})
print('Message sent successfully!')
