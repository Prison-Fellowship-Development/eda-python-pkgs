from pydantic import BaseSettings, Field
from pydantic_settings import SettingsConfigDict


class KafkaSettings(BaseSettings):
    servers: str
    protocol: str = Field(default="SASL_PLAINTEXT")
    sasl_mechanism: str = Field(default="SCRAM-SHA-256")
    sasl_username: str = Field(default="superuser")
    sasl_password: str = Field(default="secretpassword")
    auto_offset_reset: str = Field(default="earliest")

    def generate_consumer_configuration(self, group_id: str) -> dict:
        return {
            "bootstrap.servers": self.servers,
            "security.protocol": self.protocol,
            "sasl.mechanism": self.sasl_mechanism,
            "sasl.username": self.sasl_username,
            "sasl.password": self.sasl_password,
            "group.id": group_id,
            "auto.offset.reset": self.auto_offset_reset,
        }

    model_config = SettingsConfigDict(env_prefix="PFM_EVENT")
