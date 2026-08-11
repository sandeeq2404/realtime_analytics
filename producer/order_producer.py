#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════╗
║     🍔  FoodStream Analytics — Order Producer            ║
║                                                          ║
║  Simulates a real-time food delivery platform (Swiggy /  ║
║  Zomato style) and streams order events into Kafka.      ║
╚══════════════════════════════════════════════════════════╝
"""

import json
import os
import random
import time
import uuid
import logging
from datetime import datetime

from faker import Faker
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ─── Logging Setup ────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("FoodProducer")

fake = Faker("en_IN")  # Indian locale for realistic names & addresses

# ─── Config (from env or defaults) ───────────────────────────
KAFKA_BROKER        = os.getenv("KAFKA_BROKER", "localhost:29092")
RAW_TOPIC           = os.getenv("RAW_TOPIC", "raw-food-orders")
INTERVAL_MIN        = float(os.getenv("ORDER_INTERVAL_MIN", "0.5"))
INTERVAL_MAX        = float(os.getenv("ORDER_INTERVAL_MAX", "2.0"))

# ─── Reference Data ───────────────────────────────────────────
CITIES = [
    "Bangalore", "Mumbai", "Delhi", "Hyderabad", "Chennai",
    "Pune", "Kolkata", "Ahmedabad", "Jaipur", "Surat",
]

CUISINES: dict[str, list[str]] = {
    "Indian":       ["Butter Chicken", "Dal Makhani", "Biryani", "Paneer Tikka", "Chole Bhature", "Rajma Chawal"],
    "South Indian": ["Masala Dosa", "Idli Sambar", "Vada Sambar", "Uttapam", "Rava Kesari", "Bisi Bele Bath"],
    "Chinese":      ["Hakka Noodles", "Fried Rice", "Manchurian", "Spring Rolls", "Dim Sum", "Szechuan Fried Rice"],
    "Italian":      ["Margherita Pizza", "Pasta Arrabbiata", "Lasagna", "Risotto", "Bruschetta", "Penne Alfredo"],
    "Fast Food":    ["Chicken Burger", "French Fries", "Hot Dog", "Crispy Fried Chicken", "Onion Rings", "Nuggets"],
    "Desserts":     ["Gulab Jamun", "Rasgulla", "Chocolate Lava Cake", "Kulfi", "Kheer", "Brownie Sundae"],
    "Beverages":    ["Masala Chai", "Cold Coffee", "Mango Lassi", "Fresh Lime Soda", "Sugarcane Juice", "Smoothie"],
}

RESTAURANTS = [
    "Spice Garden", "The Biryani House", "Pizza Palace", "Dragon Wok",
    "Curry Leaf Kitchen", "The Desi Tadka", "Mainland China", "Barbeque Nation",
    "Haldiram's Express", "Moti Mahal", "The Great Punjab", "Zaika Restaurant",
    "Saravana Bhavan", "Meghna Foods", "Anand Sweets", "Burger Singh",
    "Wow! Momo", "Faasos", "Behrouz Biryani", "Box8",
]

STATUSES         = ["placed", "preparing", "out_for_delivery", "delivered", "cancelled"]
STATUS_WEIGHTS   = [0.15, 0.25, 0.15, 0.35, 0.10]
PAYMENT_METHODS  = ["UPI", "Credit Card", "Debit Card", "Cash on Delivery", "Net Banking", "Wallet"]
PAYMENT_WEIGHTS  = [0.40, 0.20, 0.15, 0.15, 0.05, 0.05]


def generate_order() -> dict:
    """Generate a single realistic food order event."""
    cuisine     = random.choice(list(CUISINES.keys()))
    items       = random.sample(CUISINES[cuisine], k=random.randint(1, 3))
    quantity    = random.randint(1, 6)
    price       = round(random.uniform(80.0, 1800.0), 2)
    status      = random.choices(STATUSES, weights=STATUS_WEIGHTS, k=1)[0]
    rating      = round(random.uniform(3.0, 5.0), 1) if status == "delivered" else None
    payment     = random.choices(PAYMENT_METHODS, weights=PAYMENT_WEIGHTS, k=1)[0]

    return {
        "order_id":           str(uuid.uuid4()),
        "timestamp":          int(datetime.now().timestamp() * 1000),  # epoch ms
        "customer_name":      fake.name(),
        "customer_phone":     fake.phone_number(),
        "city":               random.choice(CITIES),
        "area":               fake.city_suffix(),
        "restaurant_name":    random.choice(RESTAURANTS),
        "cuisine":            cuisine,
        "items":              ", ".join(items),
        "quantity":           quantity,
        "price":              price,
        "status":             status,
        "payment_method":     payment,
        "delivery_time_mins": random.randint(15, 65),
        "rating":             rating,
        "discount_applied":   random.choice([True, False]),
        "is_first_order":     random.random() < 0.10,   # 10% chance
    }


def connect_kafka(max_retries: int = 20) -> KafkaProducer:
    """Connect to Kafka with retry logic."""
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
            )
            logger.info(f"✅ Connected to Kafka at {KAFKA_BROKER}")
            return producer
        except NoBrokersAvailable:
            logger.warning(f"⏳ Kafka not ready (attempt {attempt}/{max_retries}), retrying in 5s...")
            time.sleep(5)
    raise RuntimeError("❌ Could not connect to Kafka after maximum retries.")


def main():
    logger.info("=" * 60)
    logger.info("  🍔  FoodStream Analytics — Order Producer")
    logger.info("=" * 60)
    logger.info(f"  Kafka Broker : {KAFKA_BROKER}")
    logger.info(f"  Topic        : {RAW_TOPIC}")
    logger.info(f"  Rate         : every {INTERVAL_MIN}–{INTERVAL_MAX}s")
    logger.info("=" * 60)

    producer   = connect_kafka()
    count      = 0
    total_rev  = 0.0

    try:
        while True:
            order = generate_order()
            producer.send(RAW_TOPIC, key=order["order_id"], value=order)
            producer.flush()

            count     += 1
            total_rev += order["price"]

            status_icon = {
                "placed":          "📥",
                "preparing":       "👨‍🍳",
                "out_for_delivery": "🛵",
                "delivered":       "✅",
                "cancelled":       "❌",
            }.get(order["status"], "❓")

            logger.info(
                f"#{count:>5} | {status_icon} {order['status']:<16} | "
                f"🏙 {order['city']:<12} | "
                f"🍽 {order['cuisine']:<12} | "
                f"💰 ₹{order['price']:>7.2f} | "
                f"{order['restaurant_name']}"
            )

            time.sleep(random.uniform(INTERVAL_MIN, INTERVAL_MAX))

    except KeyboardInterrupt:
        logger.info(f"\n🛑 Producer stopped. Sent {count} orders | Total revenue: ₹{total_rev:,.2f}")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
