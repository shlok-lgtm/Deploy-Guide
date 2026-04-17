"""
Capture production SII input vectors from a live basis-hub and write them
to test_vectors/ in the format consumed by test_reproducibility.py.

Usage:
    python verify/capture_vectors.py \\
        --api https://basisprotocol.xyz \\
        --out test_vectors/ \\
        --limit 10

The script pulls the /api/scores endpoint, then for each score fetches
the associated computation attestation (which carries the input hash
and category vector), and writes one vector file per score.

Expected JSON format of the basis-hub attestation endpoint is:
{
  "coin": "usdc",
  "category_scores": { "peg_stability": 98.1, ... },
  "score": 87.4,
  "input_hash": "<sha256 hex>",
  "computation_hash": "0x<sha256 hex>",
  "version": "v1.0.0"
}

If the endpoint shape changes, update this script — not the spec,
and not the reference implementation. Those two must stay in lockstep
with `docs/methodology_*.md`, independent of the hub's API surface.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys
from urllib import request


def fetch(url: str) -> dict:
    with request.urlopen(url, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--api", default="https://basisprotocol.xyz")
    ap.add_argument("--out", type=pathlib.Path, default=pathlib.Path("test_vectors"))
    ap.add_argument("--limit", type=int, default=10)
    args = ap.parse_args()

    args.out.mkdir(parents=True, exist_ok=True)

    scores = fetch(f"{args.api}/api/scores")
    coins = [s["symbol"].lower() for s in scores[: args.limit]]
    print(f"Capturing vectors for {len(coins)} coins: {coins}")

    written = 0
    for coin in coins:
        try:
            att = fetch(f"{args.api}/api/attestation/sii/{coin}")
        except Exception as e:
            print(f"  skip {coin}: {e}", file=sys.stderr)
            continue

        vector = {
            "coin": att.get("coin", coin),
            "version": att.get("version", "v1.0.0"),
            "inputs": att["category_scores"],
            "expected_score": att["score"],
            "expected_input_hash": att["input_hash"],
            "expected_computation_hash": att["computation_hash"],
            "captured_from": args.api,
        }
        dest = args.out / f"sii-{coin}.json"
        dest.write_text(json.dumps(vector, indent=2, sort_keys=True) + "\n")
        print(f"  wrote {dest}")
        written += 1

    print(f"Done. {written}/{len(coins)} vectors captured.")
    if written < 5:
        print("WARNING: fewer than 5 vectors — reproducibility CI will skip.",
              file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
