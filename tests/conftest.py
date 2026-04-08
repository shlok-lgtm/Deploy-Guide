"""Shared fixtures for Basis Protocol E2E tests."""

import os
import pytest
import requests


BASE_URL_DEFAULT = "http://localhost:5000"


@pytest.fixture(scope="session")
def base_url():
    """Base URL for the live deployment, set via BASE_URL env var."""
    return os.environ.get("BASE_URL", BASE_URL_DEFAULT).rstrip("/")


@pytest.fixture(scope="session")
def session():
    """Reusable requests session with connection pooling."""
    s = requests.Session()
    s.headers.update({"Accept": "application/json"})
    return s


@pytest.fixture(scope="session")
def api(base_url, session):
    """Helper that returns a callable: api("/api/scores") -> response."""
    def _get(path, **kwargs):
        url = f"{base_url}{path}"
        resp = session.get(url, timeout=30, **kwargs)
        return resp
    return _get


# ---------------------------------------------------------------------------
# Cached data fixtures (fetched once per session for cross-test consistency)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def sii_scores(api):
    """Fetch SII scores once, reuse across tests."""
    resp = api("/api/scores")
    resp.raise_for_status()
    return resp.json()


@pytest.fixture(scope="session")
def psi_scores(api):
    """Fetch PSI scores once, reuse across tests."""
    resp = api("/api/psi/scores")
    resp.raise_for_status()
    return resp.json()


@pytest.fixture(scope="session")
def wallets_top(api):
    """Fetch top wallets once, reuse across tests."""
    resp = api("/api/wallets/top")
    resp.raise_for_status()
    return resp.json()


@pytest.fixture(scope="session")
def wallets_riskiest(api):
    """Fetch riskiest wallets once, reuse across tests."""
    resp = api("/api/wallets/riskiest")
    resp.raise_for_status()
    return resp.json()


@pytest.fixture(scope="session")
def wallets_stats(api):
    """Fetch wallet stats once, reuse across tests."""
    resp = api("/api/wallets/stats")
    resp.raise_for_status()
    return resp.json()
