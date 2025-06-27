from unittest import TestCase
from unittest.mock import MagicMock

import pfm.producer
from tests.test_avro_model import TestAvroModel


class TestProducer(TestCase):
    def setUp(self):
        pfm.producer.ConfluentKafkaProducer = MagicMock()
        pfm.producer.AvroSerializer = MagicMock()
        self.producer = pfm.producer.Producer[TestAvroModel](
            "test_topic", model_class=TestAvroModel
        )

    def test_serializer_intialized_with_schema_registry_url(self):
        self.producer.produce()

        [schema_registry_url, *_], _ = pfm.producer.AvroSerializer.call_args
        self.assertEqual(schema_registry_url, pfm.producer.settings.schema_registry_url)

    def test_serializer_intialized_with_schema_string(self):
        self.producer.produce()

        [_, schema_str, _], _ = pfm.producer.AvroSerializer.call_args
        self.assertEqual(schema_str, TestAvroModel.avro_schema())

    def test_serializer_intialized_with_avro_base_model_dump(self):
        self.producer.produce()

        [*_, model_dump], _ = pfm.producer.AvroSerializer.call_args
        self.assertIs(model_dump, TestAvroModel.model_dump)

    def test_produce_serializes_message_before_producing_when_avro_base_model_given(
        self,
    ):
        serializer = pfm.producer.AvroSerializer()

        self.producer.produce()

        serializer.assert_called_once_with(None)
        
    def test_serializer_instantiated_only_one_time(self):
        self.producer.produce()
        self.producer.produce()

        self.assertEqual(pfm.producer.AvroSerializer.call_count, 1)

    def test_serializer_not_used_when_model_class_not_given(self):
        producer = pfm.producer.Producer("test_topic")

        producer.produce()

        self.assertFalse(pfm.producer.AvroSerializer.called)