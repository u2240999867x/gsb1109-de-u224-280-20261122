from __future__ import annotations

from .engine import (
    Issue,
    Observation,
    ReconciliationReport,
    reconcile,
)
from .io import load_batch
from .report import render_json, render_markdown


class ReconciliationEngine:
    """Small object-oriented facade for callers that prefer an engine."""

    def __init__(self, batch: dict):
        self.batch = batch

    def run(self, corrections=None) -> ReconciliationReport:
        return reconcile(self.batch, corrections)


__all__ = [
    "Issue",
    "Observation",
    "ReconciliationEngine",
    "ReconciliationReport",
    "load_batch",
    "reconcile",
    "render_json",
    "render_markdown",
]
