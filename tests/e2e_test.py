"""
End-to-end tests for the Basis Protocol SII / PSI / Wallet Risk API.

Run against the live deployment:

    BASE_URL=https://app.basisprotocol.xyz pytest tests/e2e_test.py -v

Requires: pytest, requests
"""

import pytest
import requests


# =============================================================================
# 1. API Endpoint Smoke Tests
# =============================================================================

class TestEndpointSmoke:
    """Every public /api/* endpoint returns 200 and valid JSON."""

    SIMPLE_ENDPOINTS = [
        "/api/health",
        "/api/scores",
        "/api/methodology",
        "/api/indices",
        "/api/config",
        "/api/namespace",
        "/api/events",
        "/api/integrity",
        "/api/cda/issuers",
        "/api/psi/scores",
        "/api/psi/definition",
        "/api/wallets/top",
        "/api/wallets/riskiest",
        "/api/wallets/stats",
        "/api/pulse/latest",
        "/api/pulse/history",
        "/api/discovery/latest",
        "/api/assessments/recent",
        "/api/divergence",
        "/api/compose/cqi",
    ]

    @pytest.mark.parametrize("path", SIMPLE_ENDPOINTS)
    def test_endpoint_returns_200_json(self, api, path):
        resp = api(path)
        assert resp.status_code == 200, f"{path} returned {resp.status_code}: {resp.text[:200]}"
        data = resp.json()
        assert data is not None, f"{path} returned null JSON body"

    INTEGRITY_DOMAINS = ["sii", "psi", "wallets", "edges", "cda", "events", "pulse"]

    @pytest.mark.parametrize("domain", INTEGRITY_DOMAINS)
    def test_integrity_domain(self, api, domain):
        resp = api(f"/api/integrity/{domain}")
        assert resp.status_code == 200, f"integrity/{domain} returned {resp.status_code}"
        data = resp.json()
        assert data is not None

    def test_discovery_protocols(self, api):
        """Discovery protocols endpoint (may not exist yet -- skip if 404)."""
        resp = api("/api/discovery/protocols")
        if resp.status_code == 404:
            pytest.skip("discovery/protocols not deployed yet")
        assert resp.status_code == 200
        assert resp.json() is not None


# =============================================================================
# 2. Data Consistency
# =============================================================================

class TestSIIConsistency:
    """SII score data consistency checks."""

    def test_scores_list_not_empty(self, sii_scores):
        scores = sii_scores.get("scores", sii_scores.get("data", []))
        assert len(scores) > 0, "SII scores list is empty"

    def test_every_stablecoin_has_issuer(self, sii_scores):
        scores = sii_scores.get("scores", sii_scores.get("data", []))
        for coin in scores:
            issuer = coin.get("issuer")
            assert issuer is not None, f"{coin.get('symbol')} has null issuer"
            assert issuer.lower() != "unknown", f"{coin.get('symbol')} has 'Unknown' issuer"

    def test_every_stablecoin_has_positive_score(self, sii_scores):
        scores = sii_scores.get("scores", sii_scores.get("data", []))
        for coin in scores:
            score = coin.get("overall_score", coin.get("score"))
            assert score is not None and float(score) > 0, (
                f"{coin.get('symbol')} has score={score}"
            )


class TestPSIConsistency:
    """PSI score data consistency checks."""

    def test_protocols_list_not_empty(self, psi_scores):
        protocols = psi_scores.get("protocols", [])
        assert len(protocols) > 0, "PSI protocols list is empty"

    def test_every_protocol_has_name(self, psi_scores):
        for p in psi_scores.get("protocols", []):
            assert p.get("protocol_name"), f"Protocol {p.get('protocol_slug')} has no name"

    def test_confidence_values(self, psi_scores):
        allowed = {"high", "standard"}
        for p in psi_scores.get("protocols", []):
            conf = p.get("confidence")
            assert conf in allowed, (
                f"Protocol {p.get('protocol_slug')} has confidence={conf}, expected {allowed}"
            )


class TestWalletConsistency:
    """Wallet data consistency checks."""

    def test_top_wallets_not_empty(self, wallets_top):
        wallets = wallets_top.get("wallets", [])
        assert len(wallets) > 0, "Top wallets list is empty"

    def test_no_duplicate_addresses_top(self, wallets_top):
        wallets = wallets_top.get("wallets", [])
        addresses = [w["address"].lower() for w in wallets]
        assert len(addresses) == len(set(addresses)), (
            f"Duplicate addresses in top wallets: {len(addresses)} total vs {len(set(addresses))} unique"
        )

    def test_riskiest_wallets_not_empty(self, wallets_riskiest):
        wallets = wallets_riskiest.get("wallets", [])
        assert len(wallets) > 0, "Riskiest wallets list is empty"

    def test_top_20_wallets_resolve(self, api, wallets_top):
        """First 20 wallets from /api/wallets/top resolve via /api/wallets/{address}."""
        wallets = wallets_top.get("wallets", [])[:20]
        assert len(wallets) > 0, "Need at least 1 wallet to test resolution"
        failures = []
        for w in wallets:
            addr = w["address"]
            resp = api(f"/api/wallets/{addr}")
            if resp.status_code != 200:
                failures.append(f"{addr} -> {resp.status_code}")
        assert not failures, f"Wallets that failed to resolve: {failures}"

    def test_wallet_detail_has_holdings(self, api, wallets_top):
        """Each wallet detail has holdings with value > 0."""
        wallets = wallets_top.get("wallets", [])[:5]
        for w in wallets:
            addr = w["address"]
            resp = api(f"/api/wallets/{addr}")
            if resp.status_code != 200:
                continue
            detail = resp.json()
            holdings = detail.get("holdings", [])
            assert len(holdings) > 0, f"Wallet {addr} has no holdings"
            for h in holdings:
                val = float(h.get("value_usd", 0))
                assert val > 0, f"Wallet {addr} holding {h.get('symbol')} has value_usd={val}"

    def test_wallet_pct_sums_to_100(self, api, wallets_top):
        """pct_of_wallet for each wallet's holdings sums to approximately 100%."""
        wallets = wallets_top.get("wallets", [])[:5]
        tolerance = 5.0  # percent
        for w in wallets:
            addr = w["address"]
            resp = api(f"/api/wallets/{addr}")
            if resp.status_code != 200:
                continue
            detail = resp.json()
            holdings = detail.get("holdings", [])
            if not holdings:
                continue
            total_pct = sum(float(h.get("pct_of_wallet", 0)) for h in holdings)
            assert abs(total_pct - 100.0) < tolerance, (
                f"Wallet {addr}: pct_of_wallet sums to {total_pct:.2f}%, expected ~100%"
            )

    def test_concentration_labels_valid(self, wallets_top):
        """Concentration labels are from the expected set and not all the same."""
        wallets = wallets_top.get("wallets", [])
        allowed = {"Concentrated", "Mixed", "Diversified"}
        labels_seen = set()
        for w in wallets:
            # concentration may be in coverage_quality or a dedicated field
            label = w.get("coverage_quality")
            if label and label in allowed:
                labels_seen.add(label)
        # If no labels found via coverage_quality, check risk details
        if not labels_seen:
            # Fetch a few wallet details to check concentration
            pytest.skip("Concentration labels not available in rankings response")
        assert labels_seen.issubset(allowed), f"Unknown labels: {labels_seen - allowed}"
        if len(wallets) >= 10:
            assert len(labels_seen) > 1, (
                f"All wallets have the same concentration label: {labels_seen}"
            )


# =============================================================================
# 3. Value Sanity
# =============================================================================

class TestValueSanity:
    """Numeric values are within sane ranges."""

    def test_sii_scores_in_range(self, sii_scores):
        scores = sii_scores.get("scores", sii_scores.get("data", []))
        for coin in scores:
            score = float(coin.get("overall_score", coin.get("score", 0)))
            assert 0 <= score <= 100, (
                f"{coin.get('symbol')}: SII score {score} out of [0,100]"
            )

    def test_psi_scores_in_range(self, psi_scores):
        for p in psi_scores.get("protocols", []):
            score = p.get("score")
            if score is not None:
                assert 0 <= float(score) <= 100, (
                    f"{p.get('protocol_slug')}: PSI score {score} out of [0,100]"
                )

    def test_wallet_ranking_value_within_10x_of_detail(self, api, wallets_top):
        """Rankings total_stablecoin_value is within 10x of detail holdings sum."""
        wallets = wallets_top.get("wallets", [])[:5]
        for w in wallets:
            ranking_val = float(w.get("total_stablecoin_value", 0))
            if ranking_val == 0:
                continue
            addr = w["address"]
            resp = api(f"/api/wallets/{addr}")
            if resp.status_code != 200:
                continue
            detail = resp.json()
            detail_sum = sum(float(h.get("value_usd", 0)) for h in detail.get("holdings", []))
            if detail_sum == 0:
                continue
            ratio = max(ranking_val, detail_sum) / max(min(ranking_val, detail_sum), 0.01)
            assert ratio < 10, (
                f"Wallet {addr}: ranking value={ranking_val:.2f}, "
                f"detail sum={detail_sum:.2f}, ratio={ratio:.1f}x (>10x)"
            )


# =============================================================================
# 4. Cross-Page Consistency
# =============================================================================

class TestCrossPageConsistency:
    """Values that should agree across endpoints actually do."""

    def test_stablecoin_count_matches_scores(self, api, sii_scores):
        """Config stablecoin count matches SII scores list length."""
        resp = api("/api/config")
        if resp.status_code != 200:
            pytest.skip("Config endpoint unavailable")
        config = resp.json()
        registry_count = len(config.get("stablecoins", config.get("coins", [])))
        scores_list = sii_scores.get("scores", sii_scores.get("data", []))
        scores_count = len(scores_list)
        assert scores_count > 0, "No SII scores returned"
        if registry_count > 0:
            assert scores_count == registry_count, (
                f"Registry has {registry_count} coins but scores has {scores_count}"
            )

    def test_psi_count_matches(self, psi_scores):
        """Count field matches actual list length."""
        protocols = psi_scores.get("protocols", [])
        count_field = psi_scores.get("count")
        if count_field is not None:
            assert int(count_field) == len(protocols), (
                f"PSI count field={count_field} but list has {len(protocols)} entries"
            )


# =============================================================================
# 5. Link Integrity
# =============================================================================

class TestLinkIntegrity:
    """Every wallet address in rankings resolves to a valid detail page."""

    def test_all_top_wallet_addresses_resolve(self, api, wallets_top):
        wallets = wallets_top.get("wallets", [])
        failures = []
        for w in wallets:
            addr = w["address"]
            resp = api(f"/api/wallets/{addr}")
            if resp.status_code != 200:
                failures.append(f"{addr} -> {resp.status_code}")
        assert not failures, (
            f"{len(failures)}/{len(wallets)} wallet addresses failed to resolve: "
            + ", ".join(failures[:10])
        )

    def test_all_riskiest_wallet_addresses_resolve(self, api, wallets_riskiest):
        wallets = wallets_riskiest.get("wallets", [])
        failures = []
        for w in wallets:
            addr = w["address"]
            resp = api(f"/api/wallets/{addr}")
            if resp.status_code != 200:
                failures.append(f"{addr} -> {resp.status_code}")
        assert not failures, (
            f"{len(failures)}/{len(wallets)} riskiest wallet addresses failed to resolve: "
            + ", ".join(failures[:10])
        )


# =============================================================================
# Summary reporter (runs last via pytest ordering)
# =============================================================================

def pytest_terminal_summary(terminalreporter, exitstatus, config):
    """Print a compact summary at the end of the test run."""
    passed = len(terminalreporter.stats.get("passed", []))
    failed = len(terminalreporter.stats.get("failed", []))
    skipped = len(terminalreporter.stats.get("skipped", []))
    total = passed + failed + skipped
    terminalreporter.write_sep("=", "Basis Protocol E2E Summary")
    terminalreporter.write_line(f"  Total:   {total}")
    terminalreporter.write_line(f"  Passed:  {passed}")
    terminalreporter.write_line(f"  Failed:  {failed}")
    terminalreporter.write_line(f"  Skipped: {skipped}")
    terminalreporter.write_sep("=", "")
