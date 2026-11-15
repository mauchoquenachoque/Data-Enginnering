import os
from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Kafka
    kafka_bootstrap_servers: str = Field(default="localhost:9092", alias="KAFKA_BOOTSTRAP_SERVERS")
    kafka_sasl_username: Optional[str] = Field(default=None, alias="KAFKA_SASL_USERNAME")
    kafka_sasl_password: Optional[str] = Field(default=None, alias="KAFKA_SASL_PASSWORD")
    kafka_security_protocol: str = Field(default="PLAINTEXT", alias="KAFKA_SECURITY_PROTOCOL")
    kafka_sasl_mechanism: str = Field(default="PLAIN", alias="KAFKA_SASL_MECHANISM")
    kafka_topic_orders: str = Field(default="ecommerce.orders", alias="KAFKA_TOPIC_ORDERS")
    kafka_consumer_group: str = Field(default="byteflow-ingestion-workers", alias="KAFKA_CONSUMER_GROUP")

    # Cloud Storage & Warehousing
    motherduck_token: Optional[str] = Field(default=None, alias="MOTHERDUCK_TOKEN")
    motherduck_database: str = Field(default="byteflow_prod", alias="MOTHERDUCK_DATABASE")
    
    # Postgres
    postgres_host: str = Field(default="localhost", alias="POSTGRES_HOST")
    postgres_port: int = Field(default=5432, alias="POSTGRES_PORT")
    postgres_db: str = Field(default="postgres", alias="POSTGRES_DB")
    postgres_user: str = Field(default="postgres", alias="POSTGRES_USER")
    postgres_password: str = Field(default="postgres", alias="POSTGRES_PASSWORD")
    postgres_schema: str = Field(default="raw_staging", alias="POSTGRES_SCHEMA")

    class Config:
        env_file = ".env"
        extra = "ignore"

settings = Settings()
