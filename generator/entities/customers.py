from __future__ import annotations

from datetime import datetime

from faker import Faker

CUSTOMER_COLUMNS = ("customer_id", "name", "email", "city", "country", "created_at", "updated_at")


def generate_customer(customer_id: int, fake: Faker, created_at: datetime) -> tuple:
    name = fake.name()
    # Deterministic uniqueness suffix instead of fake.unique.email() -- fake.unique
    # keeps an in-memory seen-set that exhausts (raises UniquenessException) across
    # a bulk load of 10k+ rows plus an indefinitely running simulator.
    email = f"{name.lower().replace(' ', '.')}.{customer_id}@{fake.free_email_domain()}"
    return (customer_id, name, email, fake.city(), fake.country(), created_at, created_at)
