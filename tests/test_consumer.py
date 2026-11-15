import json
import duckdb
import pytest
from src.consumers.order_stream_loader import flush_batch_to_warehouse

@pytest.fixture
def in_memory_warehouse():
    con = duckdb.connect(":memory:")
    con.execute("""
        CREATE TABLE raw_orders_stream (
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
    yield con
    con.close()

def test_flush_batch_to_warehouse(in_memory_warehouse):
    mock_batch = [
        {
            "event_id": "evt-001",
            "order_id": "ORD-001",
            "customer_id": "CUST-100",
            "customer_email": "test@example.com",
            "customer_country": "Germany",
            "customer_city": "Berlin",
            "order_timestamp": "2025-01-01 12:00:00",
            "status": "COMPLETED",
            "payment_method": "CREDIT_CARD",
            "total_order_value": 150.00,
            "currency": "USD",
            "device_type": "Desktop-Chrome"
        },
        {
            "event_id": "evt-002",
            "order_id": "ORD-002",
            "customer_id": "CUST-101",
            "customer_email": "test2@example.com",
            "customer_country": "USA",
            "customer_city": "Austin",
            "order_timestamp": "2025-01-01 12:05:00",
            "status": "PENDING",
            "payment_method": "PAYPAL",
            "total_order_value": 85.50,
            "currency": "USD",
            "device_type": "Mobile-iOS"
        }
    ]

    inserted = flush_batch_to_warehouse(in_memory_warehouse, mock_batch)
    assert inserted == 2

    # Verify query
    count = in_memory_warehouse.execute("SELECT count(*) FROM raw_orders_stream").fetchone()[0]
    assert count == 2

    # Test idempotency (inserting same event_id does not throw or duplicate)
    re_inserted = flush_batch_to_warehouse(in_memory_warehouse, mock_batch)
    assert re_inserted == 2
    count_after = in_memory_warehouse.execute("SELECT count(*) FROM raw_orders_stream").fetchone()[0]
    assert count_after == 2
