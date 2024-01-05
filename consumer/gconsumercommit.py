from confluent_kafka import Consumer, KafkaException
import sys

# Kafka broker(s) configuration
conf = {
    'bootstrap.servers': 'localhost:9093',  # Update with your Kafka broker address
    'group.id': 'test-consumer-group',  # Update with your consumer group
    'auto.offset.reset': 'earliest'
}

# Create a Kafka consumer instance
consumer = Consumer(conf)

# Subscribe to the topic
topic = 'posts'
consumer.subscribe([topic])

running = True

def process_message(msg):
    # Perform your task with the received message
    try:
        # Process the message
        print('Received message: {}'.format(msg.value().decode('utf-8')))
        # Execute your task here

        # If the task is successful, commit the offset to mark the message as processed
        consumer.commit(msg)

    except Exception as e:
        print(f"Error processing message: {e}")
        # Handle exceptions and optionally retry or log the error

try:
    while running:
        msg = consumer.poll(timeout=1.0)  # Poll for messages
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaException._PARTITION_EOF:
                # End of partition event
                sys.stderr.write('%% {} [{}] reached end at offset {}\n'.format(msg.topic(), msg.partition(), msg.offset()))
            else:
                # Handle other errors
                print("Consumer error: {}".format(msg.error()))
            continue

        process_message(msg)

except KeyboardInterrupt:
    pass

finally:
    # Close down consumer to commit final offsets.
    consumer.close()
