from unittest import TestCase
from unittest.mock import patch

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