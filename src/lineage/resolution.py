"""Column-lineage resolution taxonomy + coverage metrics (spec 5 / 9, T13/T14).

Every output column of a transformation lands in exactly one bucket:

    resolved      traced to a physical upstream storage column
    needs_schema  ``SELECT *`` — resolvable once the source schema is known
    no_upstream   literal / clock / sequence — no upstream column exists
    unresolved    parse error / no physical source / lineage error

The ``resolved`` fraction and a per-case taxonomy feed the CI coverage gate,
which fails if the fraction regresses or a case silently drops from
``resolved`` to ``no_upstream``/``unresolved`` (the CTAS under-report landmine).
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from enum import StrEnum


class Resolution(StrEnum):
    RESOLVED = "resolved"
    NEEDS_SCHEMA = "needs_schema"
    NO_UPSTREAM = "no_upstream"
    UNRESOLVED = "unresolved"


# Buckets that represent a column which *has* an upstream (used for the ratio).
_TRACEABLE = frozenset({Resolution.RESOLVED, Resolution.NEEDS_SCHEMA, Resolution.UNRESOLVED})


@dataclass
class CoverageMetrics:
    """Accumulates column resolutions for one run (or one corpus case)."""

    counts: Counter = field(default_factory=Counter)

    def add(self, status: Resolution) -> None:
        self.counts[status] += 1

    def merge(self, other: CoverageMetrics) -> None:
        self.counts.update(other.counts)

    @property
    def total(self) -> int:
        return sum(self.counts.values())

    @property
    def traceable(self) -> int:
        return sum(self.counts[s] for s in _TRACEABLE)

    @property
    def resolved(self) -> int:
        return self.counts[Resolution.RESOLVED]

    @property
    def resolved_fraction(self) -> float:
        """Resolved / (resolved + needs_schema + unresolved). 1.0 when nothing is traceable."""
        traceable = self.traceable
        return (self.resolved / traceable) if traceable else 1.0

    def as_dict(self) -> dict[str, int]:
        return {str(k): v for k, v in sorted(self.counts.items())}
