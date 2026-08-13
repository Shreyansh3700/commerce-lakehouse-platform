from __future__ import annotations

from dataclasses import dataclass


@dataclass
class IdCounter:
    """A simple monotonic ID allocator.

    Used both during bulk load (starting at 1, in-process) and by the
    streaming simulator (initialized from MAX(id) + 1 read from the database
    at startup), so both code paths hand out non-colliding, ascending IDs
    without a round-trip per row.
    """

    next_id: int = 1

    def take_one(self) -> int:
        value = self.next_id
        self.next_id += 1
        return value

    def take(self, n: int) -> range:
        start = self.next_id
        self.next_id += n
        return range(start, start + n)
