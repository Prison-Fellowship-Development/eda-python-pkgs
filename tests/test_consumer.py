from unittest import TestCase
from unittest.mock import patch, MagicMock

import pfm.consumer


class TestKafkaConsumer(TestCase):
    def setUp(self):
        kafka_consumer_patcher = patch("pfm.consumer.KafkaConsumer")
        kafka_consumer_patcher.start()
        self.addCleanup(kafka_consumer_patcher.stop)

        self._consumer = pfm.consumer.Consumer("test-topic")

    def test_consumer_subscribes_to_topic_on_context_enter(self):
        with self._consumer:
            pass

        self._consumer.consumer.subscribe.assert_called_once_with(["test-topic"])

    def test_consumer_closes_consumer_when_context_exits(self):
        with self._consumer:
            pass

        self._consumer.consumer.close.assert_called_once()

    def test_consumer_returns_message(self):
        test_msg = MagicMock()
        test_msg.error.return_value = False
        test_msg.value.return_value = "test-message"

        with self._consumer:
            self._consumer.consumer.poll.return_value = test_msg
            message = next(iter(self._consumer))

        self.assertEqual(test_msg.value(), message)

    def test_consumer_returns_tombstone_message_when_value_is_none(self):
        test_msg = MagicMock()
        test_msg.error.return_value = False
        test_msg.key.return_value = "test-key"
        test_msg.value.return_value = None

        with self._consumer:
            self._consumer.consumer.poll.return_value = test_msg
            message = next(iter(self._consumer))

        self.assertIsInstance(message, pfm.consumer.TombstoneRecord)

    def test_tombstone_message_contains_message_key(self):
        test_msg = MagicMock()
        test_msg.error.return_value = False
        test_msg.key.return_value = "test-key"
        test_msg.value.return_value = None

        with self._consumer:
            self._consumer.consumer.poll.return_value = test_msg
            message = next(iter(self._consumer))

        self.assertEqual(test_msg.key(), message.key)

    def test_calling_close_closes_consumer_if_consumer_enabled(self):
        self._consumer.close()

        self._consumer.consumer.close.assert_called_once()

    def test_calling_close_does_not_close_consumer_if_consumer_disabled(self):
        self._consumer._consumer = None

        self._consumer.close()

        self._consumer.consumer.close.assert_not_called()

    def test_consumer_subscribes_to_topic_in_init_method_for_legacy_behavior(self):
        self._consumer.consumer.subscribe.assert_called_once_with(["test-topic"])
