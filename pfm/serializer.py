import os
import json

from struct import unpack

from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient, record_subject_name_strategy
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka import avro


class Serializer:

    def __init__(self, subject_name = None, format = None):
        # setup schema registry client
        self.username_password = os.getenv('PFM_EVENT_SASL_USERNAME','superuser') + ':' + os.getenv('PFM_EVENT_SASL_PASSWORD','secretpassword')
        self.schema_registry_client = SchemaRegistryClient(
            conf = {
                'url': os.getenv('PFM_EVENT_SCHEMA_REGISTRY_URL','http://localhost:18081,http://localhost:28081,http://localhost:38081'),
                'basic.auth.user.info': self.username_password
            }
        )

        # set subject name and format
        self.subject_name = subject_name
        self.format = format
        
        # default class vars to None
        self.schema = None
        self.schema_id = None
        self.avro_serializer = None
        self.avro_deserializer = None

        # if subject name was passed in, then setup schema and avro seralizers
        if self.subject_name is not None:
            self.schema = self.schema_registry_client.get_latest_version(self.subject_name)
            self.format = self.schema.schema.schema_type
            if self.format == 'AVRO':
                self.avro_serializer = AvroSerializer(
                    schema_registry_client = self.schema_registry_client,
                    schema_str = self.schema.schema.schema_str,
                    conf = {
                        'auto.register.schemas': False,
                        'subject.name.strategy.type': 'RECORD'
                    },
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

    def get_schema_id(self, data):
        # try to determine schema using confluent's schema registry wire format
        #+-------------+-------------------+
        #| Byte Offset | Content           |
        #+-------------+-------------------+
        #| 0           | Magic byte (0x00) |
        #| 1 – 4       | Schema ID (int32) |
        #| 5 – end     | Avro/JSON payload |
        #+-------------+-------------------+
        prefix = data[:5]
        magic, schema_id = unpack('>bI', prefix)
        if magic == 0 and schema_id > 0:
            return schema_id
        else:
            return None

    def get_subject_name(self, schema_id):
        # get all subjects for the schema id
        subjects = self.schema_registry_client.get_subjects_by_schema_id(schema_id)
        return subjects[0]

    def get_schema(self, subject_name):
        # get the schema for the latest version of the subject
        return self.schema_registry_client.get_latest_version(subject_name)

    def auto_decode(self, topic, data):
        # try to get schema id from message payload (ie, data)
        schema_id = self.get_schema_id(data)
        if schema_id is None or schema_id == 0:
            # no schema in message so assume it was just plain JSON
            return json.loads(data)
        
        # if this schema id different from the previous one, then setup new schema for decoding
        if self.schema_id != schema_id:
            self.schema_id = schema_id
            self.subject_name = self.get_subject_name(self.schema_id)
            self.schema = self.get_schema(self.subject_name)
            self.format = self.schema.schema.schema_type
            if self.format == 'AVRO':
                # if schema format is AVRO, setup the deserializer
                self.avro_deserializer = AvroDeserializer(
                    schema_registry_client = self.schema_registry_client,
                    schema_str = self.schema.schema.schema_str,
                    from_dict = self.dict_to_json
                )
            else:
                self.avro_deserializer = None

        if self.format == 'AVRO':
            return self.decode(topic, data)
        else:
            return json.loads(data)
