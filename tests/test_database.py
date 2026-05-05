"""
Test: DB pool defaults are sized for enrichment pipeline concurrency.
"""


def test_pool_defaults_are_sized_for_enrichment_concurrency():
    """Pool max_conn must be >= 3x enrichment max_concurrent (currently 15)."""
    import inspect
    from app.database import init_pool
    sig = inspect.signature(init_pool)
    assert sig.parameters["max_conn"].default >= 45, (
        "Pool max_conn too low for enrichment pipeline; will cause "
        "'connection pool exhausted' fast-fails at startup."
    )


def test_pool_min_conn_provides_baseline():
    """Pool min_conn must be >= 5 for gate-check burst at cycle start."""
    import inspect
    from app.database import init_pool
    sig = inspect.signature(init_pool)
    assert sig.parameters["min_conn"].default >= 5, (
        "Pool min_conn too low; gate checks at cycle start will exhaust "
        "the pool before tasks even begin."
    )
