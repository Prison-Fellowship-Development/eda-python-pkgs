import os
from unittest import TestCase

from pfm.settings import KafkaSettings


class TestKafkaSettings(TestCase):
    def setUp(self): ...

    def tearDown(self):
        if "PFM_EVENT_SERVERS" in os.environ:
            del os.environ["PFM_EVENT_SERVERS"]

    def test_generate_producer_config_includes_bootstrap_servers(self):
        config = KafkaSettings().generate_producer_configuration()

        self.assertIn("bootstrap.servers", config)

    def test_generate_producer_config_includes_security_protocol(self):
        config = KafkaSettings().generate_producer_configuration()

        self.assertIn("security.protocol", config)

    def test_generate_producer_config_includes_sasl_mechanism(self):
        config = KafkaSettings().generate_producer_configuration()

        self.assertIn("sasl.mechanism", config)

    def test_generate_producer_config_includes_sasl_username(self):
        config = KafkaSettings().generate_producer_configuration()

        self.assertIn("sasl.username", config)

    def test_generate_producer_config_includes_sasl_password(self):
        config = KafkaSettings().generate_producer_configuration()

        self.assertIn("sasl.password", config)

    def test_generate_producer_config_excludes_empty_strings(self):
        os.environ["PFM_EVENT_SERVERS"] = ""

        config = KafkaSettings().generate_producer_configuration()

        self.assertNotIn("", config.values())
