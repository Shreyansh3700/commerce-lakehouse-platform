#!/usr/bin/env python3
"""CLI entrypoint for the long-running streaming data simulator.

Continuously produces realistic OLTP change traffic (new orders, status
transitions, profile/price updates, etc.) at rates configured via .env.
Runs until interrupted (Ctrl+C / SIGTERM).

Usage:
    uv run python scripts/generate_data.py
"""
from generator.streaming.simulator import main

if __name__ == "__main__":
    main()
