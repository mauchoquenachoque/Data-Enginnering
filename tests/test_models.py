import pytest
from datetime import datetime
from src.models import OrderEvent, OrderItem, OrderStatus, PaymentMethod
from src.producers.order_stream_producer import generate_random_order

def test_order_item_creation():
    item = OrderItem(
        sku="TEST-SKU",
        product_name="Wireless Mouse",
        category="Accessories",
        unit_price=25.0,
        quantity=2,
        total_amount=50.0
    )
    assert item.sku == "TEST-SKU"
    assert item.total_amount == 50.0

def test_generate_random_order():
    order = generate_random_order()
    assert isinstance(order, OrderEvent)
    assert order.order_id.startswith("ORD-")
    assert order.customer_id.startswith("CUST-")
    assert len(order.items) >= 1
    assert order.total_order_value > 0
    assert order.status in list(OrderStatus)
    assert order.payment_method in list(PaymentMethod)

def test_order_serialization():
    order = generate_random_order()
    dumped = order.model_dump()
    assert "event_id" in dumped
    assert "order_id" in dumped
    assert isinstance(dumped["items"], list)
