import os
import confluent_kafka


# TODO: Provide contextmanager functionality
class Producer:
    def __init__(self, topic, client_id=None):
        self.topic = topic
        self.client_id = client_id
        if self.client_id is None:
            self.client_id = "producer-" + str(os.getpid())
        self.producer = confluent_kafka.Producer(
            {
                "bootstrap.servers": os.getenv(
                    "PFM_EVENT_SERVERS",
                    "localhost:19092,localhost:29092,localhost:39092",
                ),
                "security.protocol": os.getenv(
                    "PFM_EVENT_SECURITY_PROTOCOL", "SASL_PLAINTEXT"
                ),
                "sasl.mechanism": os.getenv(
                    "PFM_EVENT_SASL_MECHANISM", "SCRAM-SHA-256"
                ),
                "sasl.username": os.getenv("PFM_EVENT_SASL_USERNAME", "superuser"),
                "sasl.password": os.getenv("PFM_EVENT_SASL_PASSWORD", "secretpassword"),
                "client.id": self.client_id,
            }
        )

    def produce(self, key=None, value=None, headers=[], callback=None):
        return self.producer.produce(
            topic=self.topic, key=key, value=value, headers=headers, callback=callback
        )

    def flush(self):
        return self.producer.flush()
