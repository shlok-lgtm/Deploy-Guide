"""
Index Definition Schema
========================
Any risk index is defined as a configuration against the generic scoring engine.
SII was the first. PSI is the second. The schema is the same.

An index definition is a Python dict with:
- index_id: unique string identifier
- version: semantic version string
- name: human-readable name
- description: what this index measures
- entity_type: what kind of entity it scores (stablecoin, protocol, etc.)
- categories: dict of category_id -> {name, weight}
- components: dict of component_id -> {name, category, weight, normalization: {function, params}, data_source}
- aggregation: (optional) {formula, params} — see below

All category weights must sum to 1.0.
Component weights are relative within their category.

Aggregation
-----------
Aggregation behavior is declared per-index in the optional `aggregation` block::

    aggregation: {
        "formula": str,   # one of the names below
        "params":  dict,  # formula-specific parameters
    }

If absent, defaults to ``{"formula": "legacy_renormalize", "params": {}}`` so
the pre-registry behavior is preserved exactly and no existing definition
requires editing simultaneously with the registry. New or migrated indices
should declare explicitly.

Formulas (registered in ``app.composition.AGGREGATION_FORMULAS``):

- ``legacy_renormalize`` — silent renormalization over populated components
  at both category and overall levels. Canonical for every score record
  written before its index migrated its aggregation declaration; kept
  forever so historical scores remain reproducible.
- ``coverage_weighted`` — category scores still renormalize within category,
  but the overall weighted sum uses effective category weights scaled by
  each category's populated-weight fraction. Optional param ``min_coverage``
  (default ``0.0``): below this overall coverage, overall_score is withheld.
- ``coverage_withheld`` — same math as ``coverage_weighted``; required param
  ``coverage_threshold``. Below threshold, overall_score is ``None`` and
  ``withheld`` is ``True``. Category scores are still returned.
- ``strict_zero`` — missing components treated as 0. Category weights at full
  nominal. Not adopted by any index by default.
- ``strict_neutral`` — missing components imputed to 50. Category weights at
  full nominal. Not adopted by any index by default.

Every aggregate() return carries ``method``, ``formula_version``,
``effective_category_weights``, ``coverage``, and ``withheld`` alongside the
familiar ``overall_score`` and ``category_scores``. Downstream consumers
(CQI, RQS, attestation) must treat ``overall_score`` as nullable when any
index adopts ``coverage_withheld``.
"""
