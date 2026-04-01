import os
import confluent_kafka


class Consumer:

    def __init__(self, topic, group_id = None):
        self.topic = topic
        self.group_id = group_id
        if self.group_id is None:
           self.group_id = 'consumer-group-' + str(os.getpid())
        self.consumer = confluent_kafka.Consumer({
            'bootstrap.servers': os.getenv('PFM_EVENT_SERVERS','localhost:19092,localhost:29092,localhost:39092'),
            'security.protocol': os.getenv('PFM_EVENT_SECURITY_PROTOCOL','SASL_PLAINTEXT'),
            'sasl.mechanism': os.getenv('PFM_EVENT_SASL_MECHANISM','SCRAM-SHA-256'),
            'sasl.username': os.getenv('PFM_EVENT_SASL_USERNAME','superuser'),
            'sasl.password': os.getenv('PFM_EVENT_SASL_PASSWORD','secretpassword'),
            'group.id': self.group_id,
            'auto.offset.reset': 'earliest'
        })
        self.consumer.subscribe([topic])

    def poll(self, timeout = 1.0):
        return self.consumer.poll(timeout)

    def close(self):
        return self.consumer.close()
