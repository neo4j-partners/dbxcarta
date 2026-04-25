"""Fixture factory for semantic FK inference tests.

Produces hand-crafted 1024-dim vectors with exact cosine similarities via
axis-aligned orthogonal basis construction: for target cosine `c`, the
pair is `(1, 0, 0, ...)` and `(c, sqrt(1 - c²), 0, 0, ...)` — both unit
vectors, inner product = c.

The `phase4_embeddings_pkl` fixture materialises the worklog-mandated
pickle file in tmp_path so the test proves the on-disk format works
without checking in a binary artefact."""

from __future__ import annotations

import math
import pickle
from pathlib import Path

import pytest


DIM = 1024


def _basis(i: int) -> list[float]:
    """Unit vector along axis i in R^DIM."""
    v = [0.0] * DIM
    v[i] = 1.0
    return v


def _pair_with_cosine(cos: float, axis_a: int, axis_b: int) -> tuple[list[float], list[float]]:
    """Two unit vectors `a` and `b` with exact cosine `cos`, both living in
    span(e_axis_a, e_axis_b). `a` aligns with axis_a; `b` mixes the two."""
    a = _basis(axis_a)
    b = [0.0] * DIM
    b[axis_a] = cos
    b[axis_b] = math.sqrt(max(0.0, 1.0 - cos * cos))
    return a, b


@pytest.fixture(scope="session")
def phase4_vectors() -> dict[str, list[float]]:
    """col_id → 1024-float vector.

    Relationships:
      - cos(buyer_ref, customers.id) = 0.88 → semantic accepts, confidence 0.88
      - cos(purchase_ref, orders.id)  = 0.87 → semantic accepts, confidence 0.87
      - cos(unrelated, customers.id) = 0.60 → rejected as SUB_THRESHOLD
    """
    a_buyer, a_cust = _pair_with_cosine(0.88, 0, 1)
    a_purch, a_ord = _pair_with_cosine(0.87, 2, 3)
    a_unrel = _basis(4)  # orthogonal to customers.id → cos = 0
    return {
        "main.shop.orders.buyer_ref":    a_buyer,
        "main.shop.customers.id":        a_cust,
        "main.shop.order_items.purchase_ref": a_purch,
        "main.shop.orders.id":           a_ord,
        "main.shop.unrelated.payload":   a_unrel,
    }


@pytest.fixture(scope="session")
def phase4_embeddings_pkl(
    phase4_vectors: dict[str, list[float]], tmp_path_factory: pytest.TempPathFactory,
) -> Path:
    """Materialise the worklog-mandated pickle artefact.

    Test suite round-trips through this file so the pickle format is
    exercised end-to-end. Located in the session's tmp dir because the
    workstream's no-cloud-connectivity rule forbids checking in embeddings
    that came from a real endpoint."""
    path = tmp_path_factory.mktemp("phase4_fixture") / "phase4_embeddings.pkl"
    with path.open("wb") as f:
        pickle.dump(phase4_vectors, f)
    return path
