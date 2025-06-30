import os
from typing import TypeVar, Generic, Iterator, Type

from confluent_kafka import KafkaError, Consumer as KafkaConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from dataclasses_avroschema.pydantic import AvroBaseModel

from .settings import KafkaSettings

T = TypeVar("T", bound=AvroBaseModel)

settings = KafkaSettings()


class TombstoneRecord(AvroBaseModel):
    key: str | None = None


class Consumer(Generic[T]):
    _consumer: KafkaConsumer | None = None

    def __init__(
        self, topic, group_id=None, timeout=2.0, model_class: Type[T] | None = None
    ):
        self.topic = topic
        self.timeout = timeout
        self.group_id = group_id
        self.model_class = model_class
        self._deserializer: AvroDeserializer | None = None
        self.group_id = (
            group_id or f"consumer-group-{os.getpid()}"
        )  # Default group ID based on process ID

        self._should_exit = False

    @property
    def consumer(self) -> KafkaConsumer:
        if not self._consumer:
            self._consumer = KafkaConsumer(
                settings.generate_consumer_configuration(self.group_id)
            )

        return self._consumer

    @property
    def deserializer(self):
        if not self._deserializer:
            self._deserializer = AvroDeserializer(
                schema_registry_client=SchemaRegistryClient(
                    {"url": settings.schema_registry_url}
                ),
                schema_str=self.model_class.avro_schema(),
                from_dict=lambda msg, ctx: self.model_class.model_validate(msg),
            )
        return self._deserializer

    def __enter__(self):
        self.consumer.subscribe([self.topic])
        return self

    def stop(self):
        self._should_exit = True

    def is_active(self) -> bool:
        return not self._should_exit

    def __exit__(self, exc_type, exc_value, traceback):
        self._consumer.close()
        self._consumer = None

    def __iter__(self) -> Iterator[T | TombstoneRecord]:
        while not self._should_exit:
            msg = self.consumer.poll(self.timeout)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue

            if msg.value() is None:
                yield TombstoneRecord(key=msg.key())
            else:
                yield (
                    msg.value()
                    if self.model_class is None
                    else self.deserializer(
                        msg.value(),
                        SerializationContext(self.topic, MessageField.VALUE),
                    )
                )

    def poll(self, timeout=1.0):
        return self.consumer.poll(timeout)
