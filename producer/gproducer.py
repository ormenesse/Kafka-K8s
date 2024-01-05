from confluent_kafka import Producer
import datetime

# Kafka broker(s) configuration
conf = {
    'bootstrap.servers': 'localhost:9093'  # Update with your Kafka broker address
}

# Create a Kafka producer instance
producer = Producer(conf)

# Define the topic to which you want to send messages
topic = 'posts'

# Produce a simple message
message = {'author': 'Orma', 'Message' : "Hello, Kafka!" + datetime.datetime.now().isoformat() }

# Convert the message to bytes (Kafka expects bytes)
message_bytes = str(message).encode('utf-8')

# Produce the message to the specified topic
producer.produce(topic, value=message_bytes)

# Flush the producer to ensure all messages are delivered
producer.flush()

# Close the producer instance
#producer.close()
