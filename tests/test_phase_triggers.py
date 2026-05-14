"""Unit tests for main.py's phase-trigger functions.

These tests verify that each `_run_<phase>_cycle_once` function can be
called in isolation with mocked collaborators, returns the expected
shape, and wires to the canonical module function — independent of the
time-gate scheduling logic in run_worker_loop.

Used by basis-protocol/scenarios-harness as a stable entry-point shape
so scenario fix-evaluation works regardless of how CC's fix is shaped
(canonical wrapper vs. in-place patch vs. alternative).
"""

import asyncio
import inspect
import sys
import types
from unittest.mock import MagicMock

import pytest


# ---------------------------------------------------------------------------
# Module-level harness: stub heavy imports so `import main` works in CI
# without a live DB or full app stack.
# ---------------------------------------------------------------------------

def _stub(name, **attrs):
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    sys.modules[name] = mod
    return mod


# Stubs that main.py imports at module level. Only attrs touched at
# import time need to exist; runtime attrs are set per-test below.
_stub("uvicorn")
_stub("app")
_stub(
    "app.database",
    init_pool=MagicMock(),
    close_pool=MagicMock(),
    health_check=MagicMock(),
    fetch_one=MagicMock(),
    fetch_all=MagicMock(),
    execute=MagicMock(),
)


class _SchemaDriftError(Exception):
    pass


_stub("app.schema_heal", verify_schema=MagicMock(), SchemaDriftError=_SchemaDriftError)
_stub("app.server", app=MagicMock())


import main  # noqa: E402  — after the stubs are in place


# ---------------------------------------------------------------------------
# _run_cda_cycle_once
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cda_cycle_calls_run_collection_scheduled():
    fake_result = {"status": "ran", "extracted": 7}

    async def _fake_run_collection_scheduled():
        return fake_result

    fake_mod = types.ModuleType("app.services.cda_collector")
    fake_mod.run_collection_scheduled = _fake_run_collection_scheduled
    sys.modules["app.services.cda_collector"] = fake_mod
    # Parent package must also exist for the `from app.services... import` form.
    sys.modules.setdefault("app.services", types.ModuleType("app.services"))

    result = await main._run_cda_cycle_once()

    assert result is fake_result
    assert result["status"] == "ran"


@pytest.mark.asyncio
async def test_cda_cycle_returns_result_when_wrapper_returns_no_result():
    """Wrapper may legitimately return None on a no-op cycle; the
    trigger must not raise."""
    async def _fake():
        return None

    fake_mod = types.ModuleType("app.services.cda_collector")
    fake_mod.run_collection_scheduled = _fake
    sys.modules["app.services.cda_collector"] = fake_mod

    result = await main._run_cda_cycle_once()
    assert result is None


# ---------------------------------------------------------------------------
# _run_edges_cycle_once
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_edges_cycle_calls_scheduled_with_chain_and_timestamp():
    captured = {}

    async def _fake_scheduled(chain, cycle_ts):
        captured["chain"] = chain
        captured["cycle_ts"] = cycle_ts
        return {
            "status": "ran",
            "wallets_processed": 200,
            "edges_upserted": 1234,
        }

    fake_mod = types.ModuleType("app.indexer.edges")
    fake_mod.run_edge_builder_scheduled = _fake_scheduled
    sys.modules["app.indexer.edges"] = fake_mod
    sys.modules.setdefault("app.indexer", types.ModuleType("app.indexer"))

    outcome = await main._run_edges_cycle_once("ethereum")

    assert captured["chain"] == "ethereum"
    assert isinstance(captured["cycle_ts"], float)
    assert outcome["wallets_processed"] == 200
    assert outcome["edges_upserted"] == 1234


@pytest.mark.asyncio
async def test_edges_cycle_propagates_exception_from_wrapper():
    async def _boom(chain, cycle_ts):
        raise RuntimeError("explorer down")

    fake_mod = types.ModuleType("app.indexer.edges")
    fake_mod.run_edge_builder_scheduled = _boom
    sys.modules["app.indexer.edges"] = fake_mod

    with pytest.raises(RuntimeError, match="explorer down"):
        await main._run_edges_cycle_once("base")


# ---------------------------------------------------------------------------
# _run_agent_cycle_once
# ---------------------------------------------------------------------------

def test_agent_cycle_calls_run_agent_cycle_and_returns_result():
    fake_result = {"assessments": 5, "severities": {"silent": 5}}
    fake_mod = types.ModuleType("app.agent.watcher")
    fake_mod.run_agent_cycle = MagicMock(return_value=fake_result)
    sys.modules["app.agent.watcher"] = fake_mod
    sys.modules.setdefault("app.agent", types.ModuleType("app.agent"))

    result = main._run_agent_cycle_once()

    fake_mod.run_agent_cycle.assert_called_once()
    assert result is fake_result


def test_agent_cycle_returns_none_when_inner_returns_none():
    fake_mod = types.ModuleType("app.agent.watcher")
    fake_mod.run_agent_cycle = MagicMock(return_value=None)
    sys.modules["app.agent.watcher"] = fake_mod

    assert main._run_agent_cycle_once() is None


# ---------------------------------------------------------------------------
# _run_psi_expansion_cycle_once
# ---------------------------------------------------------------------------

def test_psi_expansion_runs_all_five_steps_and_attests():
    fake_collector = types.ModuleType("app.collectors.psi_collector")
    fake_collector.collect_collateral_exposure = MagicMock()
    fake_collector.sync_collateral_to_backlog = MagicMock(return_value=3)
    fake_collector.discover_protocols = MagicMock(return_value=7)
    fake_collector.enrich_protocol_backlog = MagicMock(return_value=2)
    fake_collector.promote_eligible_protocols = MagicMock(return_value=1)
    sys.modules["app.collectors.psi_collector"] = fake_collector
    sys.modules.setdefault("app.collectors", types.ModuleType("app.collectors"))

    fake_attest = types.ModuleType("app.state_attestation")
    fake_attest.attest_state = MagicMock()
    sys.modules["app.state_attestation"] = fake_attest

    result = main._run_psi_expansion_cycle_once()

    fake_collector.collect_collateral_exposure.assert_called_once()
    fake_collector.sync_collateral_to_backlog.assert_called_once()
    fake_collector.discover_protocols.assert_called_once()
    fake_collector.enrich_protocol_backlog.assert_called_once()
    fake_collector.promote_eligible_protocols.assert_called_once()

    assert result == {
        "synced": 3,
        "discovered": 7,
        "enriched": 2,
        "promoted": 1,
    }

    # Attest must fire unconditionally (the #137 fix that this body
    # encodes: drop the `if discovered or promoted` gate).
    fake_attest.attest_state.assert_called_once()
    args, kwargs = fake_attest.attest_state.call_args
    assert args[0] == "psi_discoveries"
    payload = args[1]
    assert payload == [{"synced": 3, "discovered": 7, "enriched": 2, "promoted": 1}]
    assert kwargs.get("writer_id") == "main.inline.psi_discoveries"


def test_psi_expansion_attests_with_all_zero_counts():
    """Zero-result cycles must still emit an attestation — that's the
    whole point of #137's gate drop. Silence here is a regression."""
    fake_collector = types.ModuleType("app.collectors.psi_collector")
    fake_collector.collect_collateral_exposure = MagicMock()
    fake_collector.sync_collateral_to_backlog = MagicMock(return_value=0)
    fake_collector.discover_protocols = MagicMock(return_value=0)
    fake_collector.enrich_protocol_backlog = MagicMock(return_value=0)
    fake_collector.promote_eligible_protocols = MagicMock(return_value=0)
    sys.modules["app.collectors.psi_collector"] = fake_collector

    fake_attest = types.ModuleType("app.state_attestation")
    fake_attest.attest_state = MagicMock()
    sys.modules["app.state_attestation"] = fake_attest

    main._run_psi_expansion_cycle_once()

    fake_attest.attest_state.assert_called_once()


def test_psi_expansion_swallows_attest_failure():
    """Attest path is best-effort — a failure there must not abort the
    cycle (caller-side last_expansion_at update depends on this)."""
    fake_collector = types.ModuleType("app.collectors.psi_collector")
    fake_collector.collect_collateral_exposure = MagicMock()
    fake_collector.sync_collateral_to_backlog = MagicMock(return_value=0)
    fake_collector.discover_protocols = MagicMock(return_value=0)
    fake_collector.enrich_protocol_backlog = MagicMock(return_value=0)
    fake_collector.promote_eligible_protocols = MagicMock(return_value=0)
    sys.modules["app.collectors.psi_collector"] = fake_collector

    fake_attest = types.ModuleType("app.state_attestation")
    fake_attest.attest_state = MagicMock(side_effect=RuntimeError("DB locked"))
    sys.modules["app.state_attestation"] = fake_attest

    # Must not raise — the trigger's inner try/except catches it.
    result = main._run_psi_expansion_cycle_once()
    assert result["discovered"] == 0


# ---------------------------------------------------------------------------
# Smoke test — run_worker_loop scheduler shape preserved
# ---------------------------------------------------------------------------

def test_phase_triggers_are_module_level_and_signatured():
    """Scenarios harness imports these by qualified name. Pin the
    public shape so accidental renames or signature drift fail loudly."""
    assert inspect.iscoroutinefunction(main._run_cda_cycle_once)
    assert inspect.iscoroutinefunction(main._run_edges_cycle_once)
    assert not inspect.iscoroutinefunction(main._run_agent_cycle_once)
    assert not inspect.iscoroutinefunction(main._run_psi_expansion_cycle_once)

    edges_sig = inspect.signature(main._run_edges_cycle_once)
    assert list(edges_sig.parameters) == ["chain"]
    assert edges_sig.parameters["chain"].annotation is str


def test_run_worker_loop_dispatches_to_phase_triggers():
    """Smoke test: run_worker_loop's source references each trigger
    name. This is the cheapest insurance against someone re-inlining
    a phase body and silently breaking the harness."""
    src = inspect.getsource(main.run_worker_loop)
    assert "_run_cda_cycle_once" in src
    assert "_run_edges_cycle_once" in src
    assert "_run_agent_cycle_once" in src
    assert "_run_psi_expansion_cycle_once" in src
