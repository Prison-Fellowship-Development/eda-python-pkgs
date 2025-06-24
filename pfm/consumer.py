import os
from typing import TypeVar, Generic

import confluent_kafka
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from pydantic import BaseModel

from .settings import KafkaSettings

T = TypeVar("T", bound=BaseModel)

settings = KafkaSettings()
print(settings.generate_consumer_configuration(indent=2))


class Consumer(Generic[T]):
    _consumer: confluent_kafka.Consumer | None = None

    def __init__(
        self, topic, group_id=None, timeout=2.0, avro_schema: str | None = None
    ):
        self.topic = topic
        self.timeout = timeout
        self.group_id = group_id
        self.avro_schema = avro_schema
        self._deserializer: AvroDeserializer | None = None
        self.group_id = (
            group_id or f"consumer-group-{os.getpid()}"
        )  # Default group ID based on process ID

        self._should_exit = False

    @property
    def consumer(self) -> confluent_kafka.Consumer:
        if not self._consumer:
            self._consumer = confluent_kafka.Consumer(
                settings.generate_consumer_configuration(self.group_id)
            )
        self._consumer.subscribe([self.topic])
        return self._consumer

    @property
    def deserializer(self):
        if self.avro_schema:
            self._deserializer = AvroDeserializer(
                SchemaRegistryClient({"url": settings.schema_registry_url}),
                self.avro_schema,
                lambda msg, _: T.model_validate(msg),
            )
        return self._deserializer

    def __enter__(self):
        return self

    def stop(self):
        self._should_exit = True

    def is_active(self) -> bool:
        return not self._should_exit

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
            yield (
                msg.value()
                if self.avro_schema is None
                else self.deserializer(
                    msg.value(), SerializationContext(self.topic, MessageField.VALUE)
                )
            )

    def poll(self, timeout=1.0):
        return self.consumer.poll(timeout)
