import json
import unittest
from struct import pack
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from confluent_kafka.serialization import MessageField, SerializationContext

from pfm.serializer import Serializer


AVRO_RECORD_SCHEMA = json.dumps(
    {
        "type": "record",
        "name": "Example",
        "namespace": "com.example",
        "fields": [{"name": "name", "type": "string"}],
    }
)


def _registered_avro(schema_str=AVRO_RECORD_SCHEMA):
    """Match RegisteredSchema nesting used by Serializer: .schema.schema.{schema_type,schema_str}."""
    inner = SimpleNamespace(schema_type="AVRO", schema_str=schema_str)
    middle = SimpleNamespace(schema=inner)
    return SimpleNamespace(schema=middle)


class TestSerializerInit(unittest.TestCase):
    @patch("pfm.serializer.SchemaRegistryClient")
    def test_init_without_subject_does_not_fetch_schema(self, mock_client_cls):
        s = Serializer()
        self.assertIsNone(s.subject_name)
        self.assertIsNone(s.schema)
        self.assertIsNone(s.avro_serializer)
        self.assertIsNone(s.avro_deserializer)
        mock_client_cls.return_value.get_latest_version.assert_not_called()

    @patch("pfm.serializer.AvroDeserializer")
    @patch("pfm.serializer.AvroSerializer")
    @patch("pfm.serializer.SchemaRegistryClient")
    def test_init_with_avro_subject_configures_serializer(
        self, mock_client_cls, mock_avro_ser_cls, mock_avro_de_cls
    ):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.get_latest_version.return_value = _registered_avro()

        Serializer(subject_name="com.example.Example")

        mock_client.get_latest_version.assert_called_once_with("com.example.Example")


class TestSerializerHelpers(unittest.TestCase):
    def test_json_to_dict(self):
        s = Serializer()
        ctx = SerializationContext("t", MessageField.VALUE)
        self.assertEqual(s.json_to_dict('{"a": 1}', ctx), {"a": 1})

    def test_dict_to_json_roundtrip(self):
        s = Serializer()
        ctx = SerializationContext("t", MessageField.VALUE)
        self.assertEqual(s.dict_to_json({"x": "y"}, ctx), {"x": "y"})

    def test_dict_to_json_none(self):
        s = Serializer()
        ctx = SerializationContext("t", MessageField.VALUE)
        self.assertIsNone(s.dict_to_json(None, ctx))

    def test_get_schema_id_valid_wire_format(self):
        s = Serializer()
        payload = pack(">bI", 0, 42) + b"rest"
        self.assertEqual(s.get_schema_id(payload), 42)

    def test_get_schema_id_bad_magic(self):
        s = Serializer()
        payload = pack(">bI", 1, 42) + b"rest"
        self.assertIsNone(s.get_schema_id(payload))

    def test_get_schema_id_zero_id(self):
        s = Serializer()
        payload = pack(">bI", 0, 0) + b"rest"
        self.assertIsNone(s.get_schema_id(payload))


class TestSerializerRegistryCalls(unittest.TestCase):
    @patch("pfm.serializer.SchemaRegistryClient")
    def test_get_subject_name(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.get_subjects_by_schema_id.return_value = ["sub-a", "sub-b"]

        s = Serializer()
        self.assertEqual(s.get_subject_name(99), "sub-a")
        mock_client.get_subjects_by_schema_id.assert_called_once_with(99)

    @patch("pfm.serializer.SchemaRegistryClient")
    def test_get_schema(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        expected = _registered_avro()
        mock_client.get_latest_version.return_value = expected

        s = Serializer()
        self.assertIs(s.get_schema("my.subject"), expected)
        mock_client.get_latest_version.assert_called_with("my.subject")


class TestSerializerAutoDecode(unittest.TestCase):
    @patch("pfm.serializer.SchemaRegistryClient")
    def test_auto_decode_plain_json_returns_tuple_without_subject(self, mock_client_cls):
        s = Serializer()
        raw = json.dumps({"plain": True}).encode("utf-8")
        decoded, subject = s.auto_decode("t", raw)
        self.assertEqual(decoded, {"plain": True})
        self.assertIsNone(subject)


if __name__ == "__main__":
    unittest.main()
