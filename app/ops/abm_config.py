"""
ABM Engine Configuration
========================
ICP type definitions and drip sequence templates for the Account-Based Marketing engine.
"""

ABM_ICP_TYPES = {
    "exchange_eu": {
        "label": "EU-Regulated Exchange",
        "lenses": ["MICA67", "SCO60"],
        "pain_points": [
            "MiCA enforcement live — ad-hoc delisting decisions",
            "No standardized stablecoin quality comparison",
            "Auditors asking for methodology documentation",
        ],
        "default_coins": ["USDT", "USDC", "DAI", "FDUSD", "TUSD"],
    },
    "exchange_us": {
        "label": "US Exchange",
        "lenses": ["GENIUS", "OCC"],
        "pain_points": [
            "GENIUS Act registration window opening",
            "No permitted payment stablecoin classification",
        ],
        "default_coins": ["USDT", "USDC", "PYUSD", "GUSD"],
    },
    "exchange_apac": {
        "label": "APAC Exchange",
        "lenses": ["MAS10", "SCO60"],
        "pain_points": [
            "MAS licensing requirements tightening",
            "Banking partner due diligence demands",
        ],
        "default_coins": ["USDT", "USDC", "FDUSD", "XSGD"],
    },
    "lending_protocol": {
        "label": "Lending Protocol",
        "lenses": ["SCO60"],
        "pain_points": [
            "Collateral decisions lack standardized input",
            "Gauntlet/Chaos Labs reports non-comparable",
        ],
        "default_coins": ["USDT", "USDC", "DAI", "FRAX", "LUSD"],
    },
    "dao_treasury": {
        "label": "DAO Treasury",
        "lenses": ["SCO60"],
        "pain_points": [
            "Treasury diversification without benchmarks",
            "Governance proposals lack risk data",
        ],
        "default_coins": ["USDC", "DAI", "FRAX", "LUSD"],
    },
    "insurance_defi": {
        "label": "DeFi Insurance",
        "lenses": ["SCO60"],
        "pain_points": [
            "Coverage pricing lacks standardized risk input",
            "Exploit correlation data fragmented",
        ],
        "default_coins": ["USDT", "USDC", "DAI"],
    },
    "bank_custodian": {
        "label": "Bank / Custodian",
        "lenses": ["SCO60", "OCC"],
        "pain_points": [
            "504x capital efficiency Group 1b vs 2b",
            "No standardized classification methodology",
        ],
        "default_coins": ["USDC", "PYUSD", "GUSD"],
    },
}

ABM_DRIP_TEMPLATES = {
    "exchange_eu": [
        {"day": 0, "ch": "email", "subj": "Your {coin} listings under MiCA Article 67", "gate": False, "desc": "Pre-rendered compliance report. No gate — this is the hook."},
        {"day": 2, "ch": "linkedin", "subj": "Thought leadership share", "gate": False, "desc": "Share 'Disclosure Without Interpretation'."},
        {"day": 5, "ch": "email", "subj": "How {org} stablecoins scored vs peers", "gate": False, "desc": "Comparative SII rankings."},
        {"day": 8, "ch": "email", "subj": "MiCA backtest: 106-day early warning", "gate": True, "desc": "GATE: Email capture for backtest access."},
        {"day": 12, "ch": "email", "subj": "Full compliance report — methodology audit trail", "gate": True, "desc": "GATE: Meeting request for attested report."},
        {"day": 15, "ch": "linkedin", "subj": "DM to compliance lead", "gate": False, "desc": "Reference the report they've seen."},
        {"day": 20, "ch": "email", "subj": "Basel SCO60 impact on your banking partners", "gate": False, "desc": "Bank-side pressure: Group 1b classification data."},
        {"day": 25, "ch": "email", "subj": "Compliance subscription — annual pricing", "gate": True, "desc": "GATE: Call to discuss $50-250K/year subscription."},
    ],
    "exchange_us": [
        {"day": 0, "ch": "email", "subj": "GENIUS Act readiness: {org}'s stablecoin classification", "gate": False, "desc": "Pre-rendered GENIUS Act lens report."},
        {"day": 3, "ch": "linkedin", "subj": "Content share", "gate": False, "desc": "Share Disclosure piece."},
        {"day": 7, "ch": "email", "subj": "Which stablecoins qualify as 'permitted payment'?", "gate": False, "desc": "GENIUS Act classification analysis."},
        {"day": 10, "ch": "email", "subj": "The OCC comment letter Basis submitted", "gate": True, "desc": "GATE: Access OCC comment letter."},
        {"day": 14, "ch": "email", "subj": "Full compliance report with on-chain attestation", "gate": True, "desc": "GATE: Meeting request."},
        {"day": 21, "ch": "email", "subj": "Annual compliance subscription", "gate": True, "desc": "GATE: Pricing discussion."},
    ],
    "exchange_apac": [
        {"day": 0, "ch": "email", "subj": "MAS PS-S10 compliance assessment for {org}", "gate": False, "desc": "Pre-rendered MAS lens report."},
        {"day": 5, "ch": "email", "subj": "Cross-jurisdictional stablecoin risk comparison", "gate": False, "desc": "Multi-lens view: MAS + Basel."},
        {"day": 10, "ch": "email", "subj": "Full compliance report", "gate": True, "desc": "GATE: Meeting for attested report."},
        {"day": 16, "ch": "email", "subj": "Annual subscription", "gate": True, "desc": "GATE: Pricing."},
    ],
    "lending_protocol": [
        {"day": 0, "ch": "email", "subj": "Your collateral stablecoins, scored", "gate": False, "desc": "Protocol risk report for collateral basket."},
        {"day": 3, "ch": "twitter", "subj": "'The Missing Input' thread", "gate": False, "desc": "Price feeds without risk feeds."},
        {"day": 7, "ch": "email", "subj": "Component-level API access for {org}", "gate": False, "desc": "Show peg stability, liquidity depth individually accessible."},
        {"day": 10, "ch": "email", "subj": "Draft governance proposal for {org} integration", "gate": True, "desc": "GATE: Meeting to review governance proposal."},
        {"day": 15, "ch": "forum", "subj": "Public discussion post", "gate": False, "desc": "Post integration concept in governance forum."},
        {"day": 20, "ch": "email", "subj": "Pilot: free integration for 90 days", "gate": True, "desc": "GATE: Call to discuss pilot terms."},
    ],
    "dao_treasury": [
        {"day": 0, "ch": "email", "subj": "Your treasury's stablecoin risk profile", "gate": False, "desc": "Wallet risk report for treasury address."},
        {"day": 4, "ch": "email", "subj": "Concentration risk: what governance doesn't see", "gate": False, "desc": "Show unscored stablecoin exposure."},
        {"day": 8, "ch": "forum", "subj": "Risk transparency proposal", "gate": False, "desc": "Post about Basis Guard for treasury."},
        {"day": 12, "ch": "email", "subj": "Draft governance proposal for treasury risk policy", "gate": True, "desc": "GATE: Meeting to co-draft proposal."},
        {"day": 18, "ch": "email", "subj": "Guard deployment for {org}", "gate": True, "desc": "GATE: Technical call for Safe Guard."},
    ],
    "insurance_defi": [
        {"day": 0, "ch": "email", "subj": "Underwriting data for your covered protocols", "gate": False, "desc": "Underwriting report."},
        {"day": 5, "ch": "email", "subj": "Exploit correlation signals from SII temporal data", "gate": False, "desc": "Historical score degradation patterns."},
        {"day": 10, "ch": "email", "subj": "API integration for real-time underwriting", "gate": True, "desc": "GATE: Technical call."},
        {"day": 15, "ch": "email", "subj": "Coverage pricing model with SII input", "gate": True, "desc": "GATE: Partnership discussion."},
    ],
    "bank_custodian": [
        {"day": 0, "ch": "email", "subj": "Basel SCO60 classification for your custody assets", "gate": False, "desc": "Group 1 vs Group 2 for custodied stablecoins."},
        {"day": 4, "ch": "email", "subj": "504x capital efficiency: the classification that matters", "gate": False, "desc": "Capital treatment difference with their assets."},
        {"day": 8, "ch": "email", "subj": "Full methodology documentation for your auditors", "gate": True, "desc": "GATE: Meeting for compliance documentation."},
        {"day": 14, "ch": "email", "subj": "Enterprise compliance subscription", "gate": True, "desc": "GATE: Pricing discussion."},
    ],
}

ABM_STATE_LABELS = {
    0: "Unaware",
    1: "Seen",
    2: "Engaged",
    3: "Opened Private",
    4: "Asked Question",
    5: "Call Booked",
    6: "Call Done",
    7: "Ask Made",
    8: "Committed",
}
