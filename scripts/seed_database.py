#!/usr/bin/env python3
"""CLI entrypoint for bulk-seeding the commerce OLTP database.

Usage:
    uv run python scripts/seed_database.py [--reset]
"""
from generator.seeding.bulk_load import main

if __name__ == "__main__":
    main()
