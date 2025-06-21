import os
from typing import TypeVar, Generic

import confluent_kafka

T = TypeVar("T")


class Consumer(Generic[T]):
    _consumer: confluent_kafka.Consumer | None = None

    def __init__(self, topic, group_id=None, timeout=2.0):
        self.topic = topic
        self.timeout = timeout
        self.group_id = group_id
        if self.group_id is None:
            self.group_id = "consumer-group-" + str(
                os.getpid()
            )  # TODO: OS-agnostic default
        self._should_exit = False

    @property
    def consumer(self) -> confluent_kafka.Consumer:
        if not self._consumer:
            self._consumer = confluent_kafka.Consumer(
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
                    "sasl.password": os.getenv(
                        "PFM_EVENT_SASL_PASSWORD", "secretpassword"
                    ),
                    "group.id": self.group_id,
                    "auto.offset.reset": "earliest",
                }
            )
        self._consumer.subscribe([self.topic])
        return self._consumer

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self._consumer:
            self._consumer.close()
            self._consumer = None

    def __iter__(self) -> T:
        while not self._should_exit:
            msg = self.consumer.poll(self.timeout)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == confluent_kafka.KafkaError._PARTITION_EOF:
                    continue
            yield msg.value()  # type: ignore

    def poll(self, timeout=1.0):
        return self.consumer.poll(timeout)
