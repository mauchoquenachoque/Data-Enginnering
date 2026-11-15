import json
import logging
import random
import time
import uuid
from datetime import datetime
from kafka import KafkaProducer
from src.config import settings
from src.models import OrderEvent, OrderItem, OrderStatus, PaymentMethod

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
logger = logging.getLogger("OrderProducer")

PRODUCTS_CATALOG = [
    {"sku": "LAP-001", "name": "ThinkPad Carbon Ultrabook", "category": "Electronics", "price": 1299.99},
    {"sku": "PHN-002", "name": "Pixel Pro Smartphone", "category": "Electronics", "price": 899.00},
    {"sku": "AUD-003", "name": "Noise Cancelling Headphones", "category": "Audio", "price": 249.50},
    {"sku": "MNT-004", "name": "4K 32-inch IPS Monitor", "category": "Computers", "price": 499.99},
    {"sku": "KBD-005", "name": "Mechanical Gaming Keyboard", "category": "Accessories", "price": 119.00},
    {"sku": "CHR-006", "name": "Ergonomic Desk Chair", "category": "Furniture", "price": 389.00},
    {"sku": "MUG-007", "name": "Smart Temperature Ceramic Mug", "category": "Lifestyle", "price": 95.00},
    {"sku": "BAK-008", "name": "Waterproof Tech Backpack", "category": "Travel", "price": 79.99},
]

COUNTRIES = [
    {"country": "United States", "cities": ["New York", "San Francisco", "Austin", "Seattle"]},
    {"country": "Germany", "cities": ["Berlin", "Munich", "Hamburg", "Frankfurt"]},
    {"country": "United Kingdom", "cities": ["London", "Manchester", "Edinburgh"]},
    {"country": "Spain", "cities": ["Madrid", "Barcelona", "Valencia"]},
    {"country": "Canada", "cities": ["Toronto", "Vancouver", "Montreal"]},
    {"country": "Japan", "cities": ["Tokyo", "Osaka", "Kyoto"]},
]

DEVICES = ["Desktop-Chrome", "Mobile-iOS", "Mobile-Android", "Desktop-Firefox", "Tablet-iPadOS"]

def create_kafka_producer() -> KafkaProducer:
    """Configures and initializes a Kafka Producer supporting SASL_SSL and PLAINTEXT."""
    kafka_kwargs = {
        "bootstrap_servers": settings.kafka_bootstrap_servers.split(","),
        "value_serializer": lambda v: json.dumps(v, default=str).encode("utf-8"),
        "key_serializer": lambda k: k.encode("utf-8") if k else None,
        "acks": "all",
        "retries": 5,
        "max_in_flight_requests_per_connection": 1,
    }

    if settings.kafka_sasl_username and settings.kafka_sasl_password:
        kafka_kwargs.update({
            "security_protocol": settings.kafka_security_protocol,
            "sasl_mechanism": settings.kafka_sasl_mechanism,
            "sasl_plain_username": settings.kafka_sasl_username,
            "sasl_plain_password": settings.kafka_sasl_password,
        })
        logger.info(f"Connecting to Cloud Kafka Broker at {settings.kafka_bootstrap_servers} via {settings.kafka_security_protocol}")
    else:
        logger.info(f"Connecting to Local Kafka Broker at {settings.kafka_bootstrap_servers}")

    return KafkaProducer(**kafka_kwargs)

def generate_random_order() -> OrderEvent:
    """Generates a realistic e-commerce order event."""
    customer_num = random.randint(1000, 9999)
    location = random.choice(COUNTRIES)
    city = random.choice(location["cities"])
    
    # Pick 1-4 items
    items_count = random.choices([1, 2, 3, 4], weights=[0.5, 0.3, 0.15, 0.05])[0]
    selected_products = random.sample(PRODUCTS_CATALOG, items_count)
    
    order_items = []
    total_val = 0.0
    for prod in selected_products:
        qty = random.choices([1, 2, 3], weights=[0.8, 0.15, 0.05])[0]
        amount = round(prod["price"] * qty, 2)
        total_val += amount
        order_items.append(
            OrderItem(
                sku=prod["sku"],
                product_name=prod["name"],
                category=prod["category"],
                unit_price=prod["price"],
                quantity=qty,
                total_amount=amount,
            )
        )
    
    status = random.choices(
        [OrderStatus.COMPLETED, OrderStatus.PENDING, OrderStatus.FAILED, OrderStatus.CANCELLED],
        weights=[0.85, 0.08, 0.04, 0.03]
    )[0]

    return OrderEvent(
        customer_id=f"CUST-{customer_num}",
        customer_email=f"user{customer_num}@example.com",
        customer_country=location["country"],
        customer_city=city,
        order_timestamp=datetime.utcnow(),
        status=status,
        payment_method=random.choice(list(PaymentMethod)),
        items=order_items,
        total_order_value=round(total_val, 2),
        currency="USD",
        ip_address=f"{random.randint(11, 200)}.{random.randint(1, 254)}.{random.randint(1, 254)}.{random.randint(1, 254)}",
        device_type=random.choice(DEVICES),
    )

def start_producer(batch_delay_seconds: float = 1.0, max_events: int = 0):
    """
    Main loop streaming events to Kafka.
    If max_events is 0, streams indefinitely.
    """
    producer = create_kafka_producer()
    topic = settings.kafka_topic_orders
    logger.info(f"Starting Order Event Stream to Topic: '{topic}'")
    
    events_sent = 0
    try:
        while True:
            order = generate_random_order()
            payload = order.model_dump()
            
            future = producer.send(topic, key=order.customer_id, value=payload)
            metadata = future.get(timeout=10)
            events_sent += 1

            logger.info(
                f"[Sent #{events_sent}] Order: {order.order_id} | Customer: {order.customer_id} | "
                f"Total: ${order.total_order_value:.2f} | Partition: {metadata.partition} Offset: {metadata.offset}"
            )

            if max_events and events_sent >= max_events:
                logger.info(f"Reached requested max_events ({max_events}). Stopping.")
                break

            time.sleep(batch_delay_seconds)
    except KeyboardInterrupt:
        logger.info("Streaming interrupted by operator.")
    finally:
        producer.flush()
        producer.close()
        logger.info("Kafka Producer cleanly closed.")

if __name__ == "__main__":
    start_producer()
