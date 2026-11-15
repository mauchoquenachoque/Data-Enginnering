import uuid
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import List, Optional
from pydantic import BaseModel, Field

class OrderStatus(str, Enum):
    PENDING = "PENDING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

class PaymentMethod(str, Enum):
    CREDIT_CARD = "CREDIT_CARD"
    DEBIT_CARD = "DEBIT_CARD"
    PAYPAL = "PAYPAL"
    CRYPTO = "CRYPTO"
    BANK_TRANSFER = "BANK_TRANSFER"

class OrderItem(BaseModel):
    item_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    sku: str
    product_name: str
    category: str
    unit_price: float
    quantity: int
    total_amount: float

class OrderEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    order_id: str = Field(default_factory=lambda: f"ORD-{uuid.uuid4().hex[:10].upper()}")
    customer_id: str
    customer_email: str
    customer_country: str
    customer_city: str
    order_timestamp: datetime = Field(default_factory=datetime.utcnow)
    status: OrderStatus
    payment_method: PaymentMethod
    items: List[OrderItem]
    total_order_value: float
    currency: str = "USD"
    ip_address: Optional[str] = None
    device_type: Optional[str] = None

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
