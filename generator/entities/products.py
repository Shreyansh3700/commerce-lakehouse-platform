from __future__ import annotations

import random
from datetime import datetime

from faker import Faker

PRODUCT_COLUMNS = ("product_id", "product_name", "category", "price", "stock_quantity", "created_at", "updated_at")

CATEGORIES = (
    "Electronics",
    "Apparel",
    "Home & Kitchen",
    "Books",
    "Beauty",
    "Sports & Outdoors",
    "Toys & Games",
    "Grocery",
    "Automotive",
    "Office Supplies",
)


def generate_product(product_id: int, fake: Faker, rng: random.Random, created_at: datetime) -> tuple:
    category = rng.choice(CATEGORIES)
    name = f"{fake.word().capitalize()} {category.split()[0]} {product_id}"
    price = round(rng.uniform(5.0, 500.0), 2)
    stock_quantity = rng.randint(0, 1000)
    return (product_id, name, category, price, stock_quantity, created_at, created_at)
