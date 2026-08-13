from __future__ import annotations

import logging

logger = logging.getLogger("generator.simulator")


def configure_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")


def log_mutation(event: str, **fields: object) -> None:
    detail = " ".join(f"{key}={value}" for key, value in fields.items())
    logger.info("%s %s", event, detail)
