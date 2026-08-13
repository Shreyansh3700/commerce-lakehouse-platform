from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    generator_db_host: str = "localhost"
    generator_db_port: int = 5432
    generator_db_name: str = "commerce"
    generator_db_user: str = "commerce_app"
    generator_db_password: str = "changeme_app"

    seed_customers: int = 10_000
    seed_products: int = 10_000
    seed_orders: int = 1_000_000
    seed_order_items_max_per_order: int = 4
    seed_batch_size: int = 25_000
    seed_random_seed: int = 42

    sim_new_customers_per_min: float = 5
    sim_new_orders_per_min: float = 50
    sim_order_status_changes_per_min: float = 40
    sim_payment_changes_per_min: float = 40
    sim_inventory_changes_per_min: float = 20
    sim_shipment_changes_per_min: float = 30
    sim_profile_updates_per_min: float = 5
    sim_price_stock_changes_per_min: float = 10

    @property
    def dsn(self) -> str:
        return (
            f"host={self.generator_db_host} port={self.generator_db_port} "
            f"dbname={self.generator_db_name} user={self.generator_db_user} "
            f"password={self.generator_db_password}"
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()
