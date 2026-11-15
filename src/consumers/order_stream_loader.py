import json
import logging
import time
from datetime import datetime
from typing import List, Dict, Any
from kafka import KafkaConsumer
import duckdb
from src.config import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
logger = logging.getLogger("OrderStreamLoader")

def create_kafka_consumer() -> KafkaConsumer:
    """Configures Kafka Consumer with auto-commit false for reliable at-least-once delivery."""
    kafka_kwargs = {
        "bootstrap_servers": settings.kafka_bootstrap_servers.split(","),
        "group_id": settings.kafka_consumer_group,
        "auto_offset_reset": "earliest",
        "enable_auto_commit": False,
        "value_deserializer": lambda m: json.loads(m.decode("utf-8")),
        "consumer_timeout_ms": 5000,
    }

    if settings.kafka_sasl_username and settings.kafka_sasl_password:
        kafka_kwargs.update({
            "security_protocol": settings.kafka_security_protocol,
            "sasl_mechanism": settings.kafka_sasl_mechanism,
            "sasl_plain_username": settings.kafka_sasl_username,
            "sasl_plain_password": settings.kafka_sasl_password,
        })

    return KafkaConsumer(settings.kafka_topic_orders, **kafka_kwargs)

def get_warehouse_connection():
    """Initializes DuckDB/MotherDuck connection and ensures raw staging tables exist."""
    if settings.motherduck_token:
        conn_str = f"md:{settings.motherduck_database}?motherduck_token={settings.motherduck_token}"
        logger.info("Connecting to Cloud MotherDuck Data Warehouse...")
    else:
        conn_str = "byteflow_local.duckdb"
        logger.info("Connecting to Local DuckDB Instance...")

    con = duckdb.connect(conn_str)
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_orders_stream (
            event_id VARCHAR PRIMARY KEY,
            order_id VARCHAR,
            customer_id VARCHAR,
            customer_email VARCHAR,
            customer_country VARCHAR,
            customer_city VARCHAR,
            order_timestamp TIMESTAMP,
            status VARCHAR,
            payment_method VARCHAR,
            total_order_value DOUBLE,
            currency VARCHAR,
            device_type VARCHAR,
            raw_payload JSON,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    return con

def flush_batch_to_warehouse(con, records: List[Dict[str, Any]]) -> int:
    """Inserts a batch of records idempotently into the raw staging table."""
    if not records:
        return 0

    inserted = 0
    for r in records:
        try:
            con.execute("""
                INSERT OR IGNORE INTO raw_orders_stream (
                    event_id, order_id, customer_id, customer_email, customer_country,
                    customer_city, order_timestamp, status, payment_method,
                    total_order_value, currency, device_type, raw_payload
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
            """, (
                r.get("event_id"),
                r.get("order_id"),
                r.get("customer_id"),
                r.get("customer_email"),
                r.get("customer_country"),
                r.get("customer_city"),
                r.get("order_timestamp"),
                r.get("status"),
                r.get("payment_method"),
                r.get("total_order_value"),
                r.get("currency"),
                r.get("device_type"),
                json.dumps(r)
            ))
            inserted += 1
        except Exception as e:
            logger.error(f"Failed to insert record {r.get('event_id')}: {e}")

    logger.info(f"Successfully flushed {inserted} records to raw_orders_stream.")
    return inserted

def run_loader(batch_size: int = 50, flush_interval_sec: float = 10.0, max_batches: int = 0):
    """
    Consumes messages from Kafka and flushes them to the data warehouse in micro-batches.
    """
    consumer = create_kafka_consumer()
    con = get_warehouse_connection()
    logger.info("Order Stream Loader started. Listening for incoming messages...")

    buffer: List[Dict[str, Any]] = []
    last_flush_time = time.time()
    batches_processed = 0

    try:
        while True:
            records = consumer.poll(timeout_ms=1000)
            for _, messages in records.items():
                for message in messages:
                    buffer.append(message.value)

            time_since_flush = time.time() - last_flush_time
            if len(buffer) >= batch_size or (buffer and time_since_flush >= flush_interval_sec):
                flush_batch_to_warehouse(con, buffer)
                consumer.commit()
                buffer.clear()
                last_flush_time = time.time()
                batches_processed += 1

                if max_batches and batches_processed >= max_batches:
                    logger.info(f"Reached max batches ({max_batches}). Shutting down loader.")
                    break

            time.sleep(0.1)
    except KeyboardInterrupt:
        logger.info("Consumer stopped by operator.")
    finally:
        if buffer:
            flush_batch_to_warehouse(con, buffer)
            consumer.commit()
        consumer.close()
        con.close()
        logger.info("Kafka Consumer cleanly stopped.")

if __name__ == "__main__":
    run_loader()
