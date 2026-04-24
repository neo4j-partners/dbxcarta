"""FK inference configuration: FKInferenceConfig and associated protocols.

All FK-specific tuning parameters live here so they can be overridden via
environment variables without touching phase code. Default values reproduce
the current hard-coded behavior exactly.

Protocols defined here:
  - CounterProtocol  — shared interface for all inference phase counters
  - NameMatchStrategy — pluggable column name-match callable (Phase 3)
  - CosineFn         — injectable cosine similarity callable (Phase 4)
  - InferencePhase   — structural type for a runnable inference phase

FKInferenceConfig bundles all FK tuning knobs in one dataclass. The existing
Settings fields for FK inference delegate to a config object produced by
Settings.fk_config. Operators can override any parameter through environment
variables without touching source code.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from dbxcarta.fk_common import NameMatchKind, PKEvidence

if TYPE_CHECKING:
    from dbxcarta.fk_common import ColumnMeta, DeclaredPair, InferredRef, PKIndex

# Type alias: the score table maps (NameMatchKind, PKEvidence, comment_present)
# triples to float confidence scores.
ScoreTable = dict[tuple[NameMatchKind, PKEvidence, bool], float]


# ---------------------------------------------------------------------------
# Protocols
# ---------------------------------------------------------------------------

@runtime_checkable
class CounterProtocol(Protocol):
    """Shared interface for all inference phase counter objects.

    Both InferenceCounters (Phase 3) and SemanticInferenceCounters (Phase 4)
    conform to this protocol. New phases add a new counter class that also
    conforms; RunSummary._build_row_counts can iterate over a list of
    (counter, prefix) pairs rather than naming each phase individually.
    """

    def as_summary_dict(self, prefix: str) -> dict[str, int]:
        ...


@runtime_checkable
class NameMatchStrategy(Protocol):
    """Pluggable column name-match strategy for Phase 3.

    The default strategy implements exact-column-name match and suffix
    matching (_id, _fk, _ref). Alternative strategies — prefix-based,
    camelCase, catalog-specific — implement this same interface and are
    passed via FKInferenceConfig.name_match_strategy.
    """

    def __call__(
        self,
        src_col: str,
        tgt_col: str,
        tgt_table: str,
    ) -> NameMatchKind | None:
        ...


class CosineFn(Protocol):
    """Injectable cosine similarity function for Phase 4.

    Default: pure-Python implementation in fk_semantic._cosine.
    Alternative: numpy/BLAS-backed version for larger catalogs, injected via
    the cosine_fn parameter of infer_semantic_pairs.
    """

    def __call__(self, a: object, b: object) -> float:
        ...


@runtime_checkable
class InferencePhase(Protocol):
    """Structural type for a runnable inference phase.

    Each phase receives the accumulated covered-pairs set (declared FKs plus
    all pairs emitted by earlier phases) and returns a list of InferredRef
    plus a counter object satisfying CounterProtocol. The pipeline runner
    in inference.py iterates over enabled phases in order, threading the
    accumulated covered set between them.
    """

    def __call__(
        self,
        covered: frozenset[DeclaredPair],
    ) -> tuple[list[InferredRef], CounterProtocol]:
        ...


# ---------------------------------------------------------------------------
# FKInferenceConfig
# ---------------------------------------------------------------------------

@dataclass
class FKInferenceConfig:
    """All FK inference tuning parameters in one place.

    Default values reproduce the current hard-coded behavior exactly, so
    existing deployments see no behavior change when FKInferenceConfig is
    constructed with no arguments. Operators override individual fields via
    environment variables surfaced by Settings.

    Phase 3 parameters
    ------------------
    score_table:
        Maps (NameMatchKind, PKEvidence, comment_present) triples to float
        confidence scores. The default is the approved scoring matrix from
        worklog/fk-gap-v3-build.md.
    stem_suffixes:
        Column-name suffixes that trigger a SUFFIX name-match. Default:
        (_id, _fk, _ref). Catalogs with custom conventions (e.g. _key, _no)
        extend this tuple.
    metadata_threshold:
        Minimum attenuated score to emit a Phase 3 inferred reference.
    attenuation_exponent:
        The exponent in score / n^exponent for tie-break attenuation.
        Default 0.5 = sqrt (current behavior). Lower values are more
        permissive; higher values are more aggressive.
    attenuation_top_n:
        Optional hard cap on how many candidates per source column survive
        tie-break attenuation. None = no cap (current behavior).

    Phase 4 parameters
    ------------------
    semantic_threshold:
        Minimum cosine similarity to consider a pair. Default 0.85.
    semantic_floor:
        Lower bound for clamp(similarity, floor, cap). Default 0.80.
    semantic_cap:
        Upper bound for clamp(similarity, floor, cap). Default 0.90.
    semantic_value_bonus:
        Confidence bump when value-overlap corroborates. Default 0.05.
    semantic_overlap_threshold:
        Minimum source-values-in-target ratio for corroboration. Default 0.5.

    Shared parameters
    -----------------
    extra_type_equiv:
        Additional entries merged into the default _TYPE_EQUIV map.
        Use for catalogs with vendor-specific type aliases not in the base
        map (e.g. {"NUMBER": "INTEGER", "TEXT": "STRING"}).
    pk_extra_patterns:
        Regex strings for additional PK-heuristic column name patterns.
        Each is compiled and matched against the lowercase column name.
        Columns matching any pattern are classified as UNIQUE_OR_HEUR when
        not covered by declared PKs or the built-in id / {table}_id rule.
    name_match_strategy:
        Pluggable name-match callable. When None, fk_inference uses its
        built-in exact + suffix rules.
    """

    # Phase 3: score table
    score_table: ScoreTable = field(default_factory=lambda: {
        (NameMatchKind.EXACT,  PKEvidence.DECLARED_PK,    True):  0.95,
        (NameMatchKind.EXACT,  PKEvidence.DECLARED_PK,    False): 0.90,
        (NameMatchKind.EXACT,  PKEvidence.UNIQUE_OR_HEUR, True):  0.88,
        (NameMatchKind.EXACT,  PKEvidence.UNIQUE_OR_HEUR, False): 0.83,
        (NameMatchKind.SUFFIX, PKEvidence.DECLARED_PK,    True):  0.88,
        (NameMatchKind.SUFFIX, PKEvidence.DECLARED_PK,    False): 0.83,
        (NameMatchKind.SUFFIX, PKEvidence.UNIQUE_OR_HEUR, True):  0.82,
        (NameMatchKind.SUFFIX, PKEvidence.UNIQUE_OR_HEUR, False): 0.78,
    })

    # Phase 3: stem suffixes for name matching
    stem_suffixes: tuple[str, ...] = ("_id", "_fk", "_ref")

    # Phase 3: metadata inference threshold
    metadata_threshold: float = 0.8

    # Phase 3: attenuation exponent (default sqrt = exponent 0.5)
    attenuation_exponent: float = 0.5

    # Phase 3: optional hard top-N cap per source column (None = no cap)
    attenuation_top_n: int | None = None

    # Phase 4: semantic confidence parameters
    semantic_threshold: float = 0.85
    semantic_floor: float = 0.80
    semantic_cap: float = 0.90
    semantic_value_bonus: float = 0.05
    semantic_overlap_threshold: float = 0.5

    # Shared: type-equivalence extension
    extra_type_equiv: dict[str, str] = field(default_factory=dict)

    # Shared: extra PK heuristic patterns (regex strings, matched case-insensitively)
    pk_extra_patterns: list[str] = field(default_factory=list)

    # Phase 3: pluggable name-match strategy (None = built-in exact+suffix)
    name_match_strategy: NameMatchStrategy | None = None

    def effective_type_equiv(self) -> dict[str, str] | None:
        """Return merged type-equivalence map, or None if no extras defined.

        Callers receive None when no extras are set so they can fast-path to
        the module-level _TYPE_EQUIV constant without dict allocation.
        """
        if not self.extra_type_equiv:
            return None
        from dbxcarta.fk_common import _TYPE_EQUIV
        merged = dict(_TYPE_EQUIV)
        merged.update(self.extra_type_equiv)
        return merged

    def compiled_pk_patterns(self) -> list[re.Pattern[str]]:
        """Return compiled regex patterns for extra PK heuristics."""
        return [re.compile(p, re.IGNORECASE) for p in self.pk_extra_patterns]
