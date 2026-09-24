"""Severity model for the Phase 3 data-quality gate.

Three buckets (spec section 24: "distinguish critical failures, warnings,
and recoverable bad records"):

- CRITICAL: completeness (PK/FK not-null), referential-integrity, and
  business-rule checks. `mostly_threshold=0.0` -- any violation fails the
  check outright and blocks downstream publication.
- WARNING: validity/range checks. `mostly_threshold=0.01` (mostly=0.99) --
  a handful of stray rows don't block, but if the failure rate exceeds the
  threshold the check *escalates* to CRITICAL for blocking purposes (a
  systemic problem, not a tolerable data issue).
- RECOVERABLE: not a check-time severity a check is *declared* with -- it's
  the reporting label for a WARNING check that has some violations but
  stayed under its threshold (see `effective_severity` below). These rows
  are still written to the audit table with their sample PKs so they aren't
  silently dropped, matching the DLQ philosophy applied one layer up.
"""

from __future__ import annotations

import dataclasses
import enum


class Severity(str, enum.Enum):
    CRITICAL = "CRITICAL"
    WARNING = "WARNING"
    RECOVERABLE = "RECOVERABLE"


class Category(str, enum.Enum):
    COMPLETENESS = "completeness"
    VALIDITY = "validity"
    REFERENTIAL_INTEGRITY = "referential_integrity"
    BUSINESS_RULE = "business_rule"


@dataclasses.dataclass
class CheckResult:
    """One expectation's outcome against one Silver table.

    `severity` is the check's *declared* tier (CRITICAL or WARNING -- checks
    are never declared RECOVERABLE; that's a derived outcome, see
    `effective_severity`). `mostly_threshold` is the maximum tolerated
    failure fraction (0.0 for CRITICAL; 0.01 for WARNING's mostly=0.99).
    """

    table: str
    check_name: str
    category: Category
    severity: Severity
    element_count: int
    violation_count: int
    mostly_threshold: float
    sample_pks: list[str] = dataclasses.field(default_factory=list)

    @property
    def failure_rate(self) -> float:
        if self.element_count == 0:
            return 0.0
        return self.violation_count / self.element_count

    @property
    def passed(self) -> bool:
        """Whether the check stayed within its own declared threshold. A
        CRITICAL check's threshold is 0.0, so any violation fails it. A
        WARNING check passes as long as its failure rate stays under its
        `mostly_threshold`, even with some (recoverable) violations."""
        return self.failure_rate <= self.mostly_threshold

    @property
    def is_blocking(self) -> bool:
        """True iff this check should stop `make gold-build` from
        proceeding to the Gold build. A failed CRITICAL check always blocks. A
        failed WARNING check (i.e. one that exceeded its own mostly
        threshold) also blocks -- that's the WARNING -> CRITICAL escalation
        described in the plan. A WARNING check with some violations that
        still stayed under threshold does NOT block (its stragglers are
        RECOVERABLE, not blocking)."""
        return not self.passed

    @property
    def effective_severity(self) -> Severity:
        """Severity label for reporting/audit purposes, with the
        WARNING -> {RECOVERABLE, CRITICAL} split applied:
        - CRITICAL check: always reports CRITICAL.
        - WARNING check that failed its threshold: escalates to CRITICAL
          (systemic problem).
        - WARNING check that passed but still has some violating rows:
          reports RECOVERABLE (stragglers, logged for visibility, not
          blocking).
        - WARNING check with zero violations: reports WARNING (its own
          declared tier, clean run).
        """
        if self.severity is Severity.CRITICAL:
            return Severity.CRITICAL
        if not self.passed:
            return Severity.CRITICAL
        if self.violation_count > 0:
            return Severity.RECOVERABLE
        return Severity.WARNING
