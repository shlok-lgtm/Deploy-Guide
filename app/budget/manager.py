"""
API Budget Manager
==================
Coordinates daily Etherscan API call budgets across SII, PSI, and wallet
indexer processes using a single Postgres counter table (ops.api_budget).

Priority cascade: SII → PSI → wallet_refresh → wallet_expansion.
Unused allocation from higher-priority processes rolls into lower ones.
"""

import logging
from app.database import fetch_one, execute

logger = logging.getLogger(__name__)


class ApiBudgetManager:
    """
    Manages daily API call budgets across processes.
    Each process checks available budget before each batch.
    """

    ALLOCATIONS = {
        "sii": 25_000,
        "psi": 10_000,
        "wallet_refresh": 40_000,
        "wallet_expansion": None,  # Gets remainder
    }

    PRIORITY_ORDER = ["sii", "psi", "wallet_refresh", "wallet_expansion"]

    # Column names for each process
    _CALLS_COL = {
        "sii": "sii_calls_used",
        "psi": "psi_calls_used",
        "wallet_refresh": "wallet_refresh_calls_used",
        "wallet_expansion": "wallet_expansion_calls_used",
    }

    def get_or_create_today(self, provider="etherscan") -> dict:
        """
        Returns today's budget row. Creates if not exists.
        Uses INSERT ... ON CONFLICT DO NOTHING + SELECT pattern.
        """
        execute(
            """
            INSERT INTO ops.api_budget (budget_date, provider)
            VALUES (CURRENT_DATE, %s)
            ON CONFLICT (budget_date, provider) DO NOTHING
            """,
            (provider,),
        )
        row = fetch_one(
            """
            SELECT * FROM ops.api_budget
            WHERE budget_date = CURRENT_DATE AND provider = %s
            """,
            (provider,),
        )
        return row

    def available_for(self, process: str, provider="etherscan") -> int:
        """
        Returns how many calls this process can still make today.

        Cascade: unused allocations from higher-priority processes roll down.
        Hard ceiling: never exceed daily_limit across all processes combined.
        """
        if process not in self._CALLS_COL:
            raise ValueError(f"Unknown process: {process}")

        row = self.get_or_create_today(provider)
        daily_limit = row["daily_limit"]

        total_used = (
            row["sii_calls_used"]
            + row["psi_calls_used"]
            + row["wallet_refresh_calls_used"]
            + row["wallet_expansion_calls_used"]
        )

        absolute_remaining = daily_limit - total_used
        if absolute_remaining <= 0:
            return 0

        if process == "sii":
            return min(
                self.ALLOCATIONS["sii"] - row["sii_calls_used"],
                absolute_remaining,
            )

        elif process == "psi":
            sii_surplus = max(0, self.ALLOCATIONS["sii"] - row["sii_calls_used"])
            psi_pool = self.ALLOCATIONS["psi"] + sii_surplus
            return min(psi_pool - row["psi_calls_used"], absolute_remaining)

        elif process == "wallet_refresh":
            sii_surplus = max(0, self.ALLOCATIONS["sii"] - row["sii_calls_used"])
            psi_pool = self.ALLOCATIONS["psi"] + sii_surplus
            psi_surplus = max(0, psi_pool - row["psi_calls_used"])
            refresh_pool = self.ALLOCATIONS["wallet_refresh"] + psi_surplus
            return min(
                refresh_pool - row["wallet_refresh_calls_used"],
                absolute_remaining,
            )

        elif process == "wallet_expansion":
            # Expansion gets whatever is left after all others
            return absolute_remaining

        return 0

    def record_calls(self, process: str, count: int, provider="etherscan"):
        """
        Atomically increment the call counter for this process.
        Uses col = col + N for safe concurrent access.
        """
        if process not in self._CALLS_COL:
            raise ValueError(f"Unknown process: {process}")
        if count <= 0:
            return

        col = self._CALLS_COL[process]
        execute(
            f"""
            UPDATE ops.api_budget
            SET {col} = {col} + %s
            WHERE budget_date = CURRENT_DATE AND provider = %s
            """,
            (count, provider),
        )

    def mark_started(self, process: str, provider="etherscan"):
        """Set {process}_started_at = NOW()."""
        if process not in self._CALLS_COL:
            raise ValueError(f"Unknown process: {process}")

        # Ensure row exists
        self.get_or_create_today(provider)

        col = f"{process}_started_at"
        execute(
            f"""
            UPDATE ops.api_budget
            SET {col} = NOW()
            WHERE budget_date = CURRENT_DATE AND provider = %s
            """,
            (provider,),
        )

    def mark_completed(self, process: str, provider="etherscan"):
        """Set {process}_completed_at = NOW()."""
        if process not in self._CALLS_COL:
            raise ValueError(f"Unknown process: {process}")

        col = f"{process}_completed_at"
        execute(
            f"""
            UPDATE ops.api_budget
            SET {col} = NOW()
            WHERE budget_date = CURRENT_DATE AND provider = %s
            """,
            (provider,),
        )

    def get_status(self, provider="etherscan") -> dict:
        """Return today's budget summary for admin/API visibility."""
        row = self.get_or_create_today(provider)

        total_used = (
            row["sii_calls_used"]
            + row["psi_calls_used"]
            + row["wallet_refresh_calls_used"]
            + row["wallet_expansion_calls_used"]
        )

        return {
            "date": str(row["budget_date"]),
            "provider": row["provider"],
            "daily_limit": row["daily_limit"],
            "total_used": total_used,
            "remaining": row["daily_limit"] - total_used,
            "utilization_pct": round(
                total_used / row["daily_limit"] * 100, 1
            ) if row["daily_limit"] > 0 else 0,
            "breakdown": {
                "sii": {
                    "allocated": self.ALLOCATIONS["sii"],
                    "used": row["sii_calls_used"],
                    "started": str(row["sii_started_at"]) if row["sii_started_at"] else None,
                    "completed": str(row["sii_completed_at"]) if row["sii_completed_at"] else None,
                },
                "psi": {
                    "allocated": self.ALLOCATIONS["psi"],
                    "used": row["psi_calls_used"],
                    "started": str(row["psi_started_at"]) if row["psi_started_at"] else None,
                    "completed": str(row["psi_completed_at"]) if row["psi_completed_at"] else None,
                },
                "wallet_refresh": {
                    "allocated": self.ALLOCATIONS["wallet_refresh"],
                    "used": row["wallet_refresh_calls_used"],
                    "started": str(row["wallet_refresh_started_at"]) if row["wallet_refresh_started_at"] else None,
                    "completed": str(row["wallet_refresh_completed_at"]) if row["wallet_refresh_completed_at"] else None,
                },
                "wallet_expansion": {
                    "allocated": "remainder",
                    "used": row["wallet_expansion_calls_used"],
                    "started": str(row["wallet_expansion_started_at"]) if row["wallet_expansion_started_at"] else None,
                    "completed": str(row["wallet_expansion_completed_at"]) if row["wallet_expansion_completed_at"] else None,
                },
            },
        }
