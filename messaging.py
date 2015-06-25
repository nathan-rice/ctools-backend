__author__ = 'nathan'

from kombu import Connection, Exchange, Queue

process_management_exchange = Exchange('process_management', 'direct')
test_queue = Queue('test', exchange=process_management_exchange, durable=False)

def process_message(body, message):
    print body
    message.ack()

with Connection('amqp://localhost') as conn:
    producer = conn.Producer()
    producer.publish("testing!", exchange=process_management_exchange, routing_key='test', declare=[test_queue])
    with conn.Consumer(test_queue, callbacks=[process_message]) as consumer:
        while True:
            conn.drain_events()

