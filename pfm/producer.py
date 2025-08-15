import os
from typing import TypeVar, Generic, Type

from confluent_kafka import Producer as ConfluentKafkaProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from dataclasses_avroschema.pydantic import AvroBaseModel

from .settings import KafkaSettings

settings = KafkaSettings()

T = TypeVar("T", bound=AvroBaseModel)


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
                SchemaRegistryClient({
                    "url": settings.schema_registry_url,
                    "basic.auth.user.info": settings.sasl_username + ":" + settings.sasl_password,
                }),
                self._model_class.avro_schema(),
                lambda msg, _: msg.model_dump(),
            )
        return self._serializer

    def produce(
        self, key=None, value: T | str | bytes = None, headers=None, callback=None
    ):
        self._producer.poll(0)

        return self._producer.produce(
            topic=self.topic,
            key=key,
            value=self.serializer(
                value, SerializationContext(self.topic, MessageField.VALUE)
            )
            if self._model_class
            else value,
            headers=headers,
            callback=callback,
        )

    def flush(self):
        return self._producer.flush()
