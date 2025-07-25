from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class KafkaSettings(BaseSettings):
    servers: str = Field(default="localhost:9092")
    security_protocol: str = Field(default="SASL_PLAINTEXT")
    sasl_mechanism: str = Field(default="SCRAM-SHA-256")
    sasl_username: str = Field(default="sasl_user")
    sasl_password: str = Field(default="sasl_password")
    auto_offset_reset: str = Field(default="earliest")
    schema_registry_url: str = Field(default="http://localhost:8081")

    def generate_consumer_configuration(self, group_id: str) -> dict:
        config = {
            "bootstrap.servers": self.servers,
            "security.protocol": self.security_protocol,
            "sasl.mechanism": self.sasl_mechanism,
            "sasl.username": self.sasl_username,
            "sasl.password": self.sasl_password,
            "group.id": group_id,
            "auto.offset.reset": self.auto_offset_reset,
        }

        return {k: v for k, v in config.items() if v}

    def generate_producer_configuration(self) -> dict:
        config = {
            "bootstrap.servers": self.servers,
            "security.protocol": self.security_protocol,
            "sasl.mechanism": self.sasl_mechanism,
            "sasl.username": self.sasl_username,
            "sasl.password": self.sasl_password,
        }
        return {k: v for k, v in config.items() if v}

    model_config = SettingsConfigDict(env_prefix="PFM_EVENT_")
