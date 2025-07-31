import os
import json

from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient, record_subject_name_strategy
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka import avro


class Serializer:

    def __init__(self, subject_name, format = 'AVRO'):
        self.subject_name = subject_name
        self.format = format
        self.username_password = os.getenv('PFM_EVENT_SASL_USERNAME','superuser') + ':' + os.getenv('PFM_EVENT_SASL_PASSWORD','secretpassword')
        self.schema_registry_client = SchemaRegistryClient(
            conf = {
                'url': os.getenv('PFM_EVENT_SCHEMA_REGISTRY_URL','http://localhost:18081,http://localhost:28081,http://localhost:38081'),
                'basic.auth.user.info': self.username_password
            }
        )
        self.schema = self.schema_registry_client.get_latest_version(self.subject_name)
        self.avro_serializer = AvroSerializer(
            schema_registry_client = self.schema_registry_client,
            schema_str = self.schema.schema.schema_str,
            to_dict = self.json_to_dict
        )
        self.avro_deserializer = AvroDeserializer(
            schema_registry_client = self.schema_registry_client,
            schema_str = self.schema.schema.schema_str,
            from_dict = self.dict_to_json
        )

    def encode(self, topic, data):
        return self.avro_serializer(data, SerializationContext(topic, MessageField.VALUE))

    def decode(self, topic, data):
        return self.avro_deserializer(data, SerializationContext(topic, MessageField.VALUE))

    def json_to_dict(self, json_str, ctx):
        return json.loads(json_str)

    def dict_to_json(self, obj, ctx):
        if obj is None:
            return None
        tmp_str = json.dumps(obj)
        return json.loads(tmp_str)
