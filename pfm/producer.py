import os
from typing import TypeVar, Generic, Type

from confluent_kafka import Producer as ConfluentKafkaProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from pfm.settings import KafkaSettings

settings = KafkaSettings()

T = TypeVar("T", bound="AvroBaseModel")


class Producer(Generic[T]):
    def __init__(self, topic, client_id=None, model_class: Type[T] | None = None):
        self.topic = topic
        self.client_id = client_id or f"producer-{os.getpid()}"
        self._producer = ConfluentKafkaProducer(
            settings.generate_producer_configuration()
        )
        self._model_class = model_class
        self._serializer: AvroSerializer | None = None

    @property
    def serializer(self):
        if not self._serializer:
            self._serializer = AvroSerializer(
                SchemaRegistryClient({"url": settings.schema_registry_url}),
                self._model_class.avro_schema(),
                self._model_class.model_dump,
            )
        return self._serializer

    def produce(
        self, key=None, value: T | str | bytes = None, headers=None, callback=None
    ):
        return self._producer.produce(
            topic=self.topic,
            key=key,
            value=self.serializer(value) if self._model_class else value,
            headers=headers,
            callback=callback,
        )

    def flush(self):
        return self._producer.flush()
