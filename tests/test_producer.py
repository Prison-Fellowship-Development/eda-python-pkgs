from unittest import TestCase
from unittest.mock import patch

import pfm.producer
from tests.example_avro_model import ExampleAvroModel


class TestProducer(TestCase):
    def setUp(self):
        confluent_kafka_producer_patcher = patch("pfm.producer.ConfluentKafkaProducer")
        confluent_kafka_producer_patcher.start()
        self.addCleanup(confluent_kafka_producer_patcher.stop)

        avro_serializer_patcher = patch("pfm.producer.AvroSerializer")
        avro_serializer_patcher.start()
        self.addCleanup(avro_serializer_patcher.stop)

        schema_registry_client_patcher = patch("pfm.producer.SchemaRegistryClient")
        schema_registry_client_patcher.start()
        self.addCleanup(schema_registry_client_patcher.stop)

        serialization_context_patcher = patch("pfm.producer.SerializationContext")
        serialization_context_patcher.start()
        self.addCleanup(serialization_context_patcher.stop)

        message_field_patcher = patch("pfm.producer.MessageField")
        message_field_patcher.start()
        self.addCleanup(message_field_patcher.stop)

        self._topic = "test_topic"
        self.producer = pfm.producer.Producer[ExampleAvroModel](
            self._topic, model_class=ExampleAvroModel
        )

    def test_serializer_intialized_with_schema_registry_client(self):
        self.producer.produce()

        [schema_registry_client, *_], _ = pfm.producer.AvroSerializer.call_args
        self.assertEqual(pfm.producer.SchemaRegistryClient(), schema_registry_client)

    def test_schema_registry_client_receives_proper_config(self):
        self.producer.produce()

        pfm.producer.SchemaRegistryClient.assert_called_once_with({
            "url": pfm.producer.settings.schema_registry_url
        })

    def test_serializer_intialized_with_schema_string(self):
        self.producer.produce()

        [_, schema_str, _], _ = pfm.producer.AvroSerializer.call_args
        self.assertEqual(ExampleAvroModel.avro_schema(), schema_str)

    def test_serializer_intialized_with_avro_base_model_dump(self):
        self.producer.produce()

        [*_, model_dump], _ = pfm.producer.AvroSerializer.call_args
        self.assertTrue(callable(model_dump))

    def test_produce_serializes_message_before_producing_when_avro_base_model_given(
        self,
    ):
        serializer = pfm.producer.AvroSerializer()

        self.producer.produce()

        [value, _], _ = serializer.call_args
        self.assertEqual(None, value)

    def test_serializer_instantiated_only_one_time(self):
        self.producer.produce()
        self.producer.produce()

        self.assertEqual(pfm.producer.AvroSerializer.call_count, 1)

    def test_serializer_not_used_when_model_class_not_given(self):
        producer = pfm.producer.Producer("test_topic")

        producer.produce()

        self.assertFalse(pfm.producer.AvroSerializer.called)

    def test_producer_polls_just_before_producing(self):
        self.producer.produce()

        pfm.producer.ConfluentKafkaProducer().poll.assert_called_once()

    def test_producer_polls_with_an_immediate_return(self):
        self.producer.produce()

        pfm.producer.ConfluentKafkaProducer().poll.assert_called_once_with(0)

    def test_serializer_called_with_serialization_context_including_topic(self):
        context = pfm.producer.SerializationContext

        self.producer.produce()

        [topic, _], _ = context.call_args
        self.assertEqual(self._topic, topic)

    def test_serializer_called_with_serialization_context_including_value_constant(
        self,
    ):
        context = pfm.producer.SerializationContext

        self.producer.produce()

        [_, value_constant], _ = context.call_args
        self.assertEqual(pfm.producer.MessageField.VALUE, value_constant)
