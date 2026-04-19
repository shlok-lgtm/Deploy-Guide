"""
Report Templates
=================
Each template renders assembled report data into HTML or JSON.
Templates follow the Witness/Proof design language:
Georgia serif, #F3F2ED background, monospace data, dotted borders.
"""

from app.templates.protocol_risk import render as render_protocol_risk
from app.templates.wallet_risk import render as render_wallet_risk
from app.templates.compliance import render as render_compliance
from app.templates.underwriting import render as render_underwriting
from app.templates.sbt_metadata import render as render_sbt_metadata
from app.templates.engagement import render as render_engagement


TEMPLATES = {
    "protocol_risk": render_protocol_risk,
    "wallet_risk": render_wallet_risk,
    "compliance": render_compliance,
    "underwriting": render_underwriting,
    "sbt_metadata": render_sbt_metadata,
    "engagement": render_engagement,
}


def get_template(name: str):
    """Get a template render function by name."""
    return TEMPLATES.get(name)


def list_templates() -> list[str]:
    """List available template names."""
    return list(TEMPLATES.keys())
