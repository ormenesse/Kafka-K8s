from confluent_kafka import Consumer

conf = {
    'bootstrap.servers': 'localhost:9093',
    'group.id': 'consumer-first-test-group',
    'auto.offset.reset': 'latest'
}

consumer = Consumer(conf)

consumer.subscribe(['posts'])

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    print('Received message: {}'.format(msg.value().decode('utf-8')))
    # Process the message

    # Commit the message offset to indicate it's processed
    consumer.commit()

consumer.close()
