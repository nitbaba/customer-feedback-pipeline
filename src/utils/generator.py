import uuid
import random
import json
from datetime import datetime, timezone

# list of mock data to randomly choose from
PRODUCTS = [f"PROD-{i}" for i in range(1001, 1010)]

# reviews to use for review_text
REVIEW_TEMPLATES = [
    "Absolutely love this product! It exceeded all my expectations.",
    "It works okay, but the setup process was incredibly confusing.",
    "The battery on this device dies within two hours. Extremely frustrating. Fix this!",
    "Shipping was delayed by a week. Product is fine, but customer service was unhelpful.",
    "Great value for the money. Highly recommend to anyone looking for an upgrade.",
    "Total waste of money. It broke on the very first day of use. I want a refund.",
]


def generate_mock_ticket():
    """generate a dictionary representing customer support ticket"""
    return {
        "ticket_id": str(uuid.uuid4()),
        "customer_id": f"CUST-{random.randint(5000,9999)}",
        "product_id": random.choice(PRODUCTS),
        "review_text": random.choice(REVIEW_TEMPLATES),
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


if __name__ == "__main__":
    print("Testing Mock Ticket Generator:\n")
    # test generator for n number tickets to terminal
    for i in range(3):
        ticket = generate_mock_ticket()
        print(json.dumps(ticket, indent=2))
