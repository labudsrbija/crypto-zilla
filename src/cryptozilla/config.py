from typing import Any, Literal

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


class AppSettings(Settings):
    cz_env: Literal["local", "gcp"] = "local"
    log_level: str = "INFO"
    log_format: Literal["text", "json"] = "text"


class KafkaSettings(Settings):
    model_config = SettingsConfigDict(env_prefix="KAFKA_")

    bootstrap_servers: str = "localhost:9092"
    client_id: str = "cryptozilla"
    security_protocol: Literal["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"] = "PLAINTEXT"
    sasl_mechanism: str | None = None
    sasl_username: str | None = None
    sasl_password: SecretStr | None = None

    def client_config(self) -> dict[str, Any]:
        conf: dict[str, Any] = {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": self.client_id,
            "security.protocol": self.security_protocol,
        }
        if self.sasl_mechanism:
            conf["sasl.mechanism"] = self.sasl_mechanism
        if self.sasl_username:
            conf["sasl.username"] = self.sasl_username
        if self.sasl_password:
            conf["sasl.password"] = self.sasl_password.get_secret_value()
        return conf


class IngestSettings(Settings):
    model_config = SettingsConfigDict(env_prefix="INGEST_")

    trades_topic: str = "trades.raw"
    stats_interval_s: float = 30.0
    reconnect_min_s: float = 1.0
    reconnect_max_s: float = 60.0
