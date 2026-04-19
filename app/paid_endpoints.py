"""
Paid Endpoints Registry
========================
Single source of truth for all x402 paid endpoints.
Imported by payments.py (middleware config) and server.py (/.well-known/x402).
"""

USDC_BASE = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
BASIS_WALLET = "0x2dF0f62D1861Aa59A4430e3B2b2E7a0D29Cb723b"

PAID_ENDPOINTS = [
    {
        "url": "/api/paid/sii/rankings",
        "method": "GET",
        "price": "$0.005",
        "price_usdc_base_units": "5000",
        "description": "All stablecoin SII rankings with scores, grades, and category breakdowns",
    },
    {
        "url": "/api/paid/sii/{coin}",
        "method": "GET",
        "price": "$0.001",
        "price_usdc_base_units": "1000",
        "description": "Single stablecoin SII score with full component breakdown",
    },
    {
        "url": "/api/paid/psi/scores",
        "method": "GET",
        "price": "$0.005",
        "price_usdc_base_units": "5000",
        "description": "All protocol solvency scores",
    },
    {
        "url": "/api/paid/psi/scores/{slug}",
        "method": "GET",
        "price": "$0.001",
        "price_usdc_base_units": "1000",
        "description": "Single protocol solvency score with component breakdown",
    },
    {
        "url": "/api/paid/cqi",
        "method": "GET",
        "price": "$0.001",
        "price_usdc_base_units": "1000",
        "description": "Composite Quality Index for a stablecoin-protocol pair",
    },
    {
        "url": "/api/paid/rqs/{slug}",
        "method": "GET",
        "price": "$0.001",
        "price_usdc_base_units": "1000",
        "description": "Reserve Quality Score for a protocol's stablecoin treasury holdings",
    },
    {
        "url": "/api/paid/pulse/latest",
        "method": "GET",
        "price": "$0.002",
        "price_usdc_base_units": "2000",
        "description": "Latest daily system pulse with integrity status",
    },
    {
        "url": "/api/paid/discovery/latest",
        "method": "GET",
        "price": "$0.005",
        "price_usdc_base_units": "5000",
        "description": "Latest cross-domain discovery signals",
    },
    {
        "url": "/api/paid/wallets/{address}/profile",
        "method": "GET",
        "price": "$0.005",
        "price_usdc_base_units": "5000",
        "description": "Full wallet risk profile with behavioral signals and reputation",
    },
    {
        "url": "/api/paid/report/{entity_type}/{entity_id}",
        "method": "GET",
        "price": "$0.01",
        "price_usdc_base_units": "10000",
        "description": "Attested risk report with optional regulatory lens",
    },
    {
        "url": "/api/paid/rpi/scores",
        "method": "GET",
        "price": "$0.005",
        "price_usdc_base_units": "5000",
        "description": "All protocol Risk Posture Index scores",
    },
    {
        "url": "/api/paid/rpi/scores/{slug}",
        "method": "GET",
        "price": "$0.001",
        "price_usdc_base_units": "1000",
        "description": "Single protocol RPI score with component breakdown",
    },
]
