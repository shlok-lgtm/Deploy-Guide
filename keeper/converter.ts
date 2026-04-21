import { ethers } from "ethers";

// ============================================================
// Score conversion: API 0-100 float → on-chain uint16 0-10000
// ============================================================

export function scoreToUint16(apiScore: number): number {
  return Math.round(apiScore * 100); // 78.45 → 7845
}

export function uint16ToScore(raw: number): number {
  return raw / 100; // 7845 → 78.45
}

// ============================================================
// Grade conversion: API string → bytes2 hex
// ============================================================

export function gradeToBytes2(grade: string): string {
  const byte1 = grade.charCodeAt(0); // 'A' = 0x41
  const byte2 = grade.length > 1 ? grade.charCodeAt(1) : 0; // '+' = 0x2B, '-' = 0x2D
  const combined = (byte1 << 8) | byte2;
  return ethers.zeroPadBytes(ethers.toBeHex(combined), 2);
}

export function bytes2ToGrade(raw: string): string {
  // raw is like "0x412b"
  const hex = raw.replace("0x", "");
  const byte1 = parseInt(hex.slice(0, 2), 16);
  const byte2 = parseInt(hex.slice(2, 4), 16);
  const char1 = String.fromCharCode(byte1);
  const char2 = byte2 !== 0 ? String.fromCharCode(byte2) : "";
  return char1 + char2;
}

// ============================================================
// Token address mapping: API stablecoin ID → Ethereum L1 address
// Source of truth: app/config.py STABLECOIN_REGISTRY
// ============================================================

export const TOKEN_ADDRESSES: Record<string, string> = {
  usdc:  "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
  usdt:  "0xdac17f958d2ee523a2206206994597c13d831ec7",
  dai:   "0x6b175474e89094c44da98b954eedeac495271d0f",
  frax:  "0x853d955acef822db058eb8505911ed77f175b99e",
  pyusd: "0x6c3ea9036406852006290770bedfcaba0e23a0e8",
  fdusd: "0xc5f0f7b66764f6ec8c8dff7ba683102295e16409",
  tusd:  "0x0000000000085d4780b73119b644ae5ecd22b376",
  usdd:  "0x0c10bf8fcb7bf5412187a595ab97a3609160b5c6",
  usde:  "0x4c9edd5852cd905f086c759e8383e09bff1e68b3",
  usd1:  "0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d",
};

export function tokenIdToAddress(id: string): string | undefined {
  return TOKEN_ADDRESSES[id.toLowerCase()];
}

// ============================================================
// Grade→bytes2 lookup table (for reference/validation)
// ============================================================

// Confidence tier codes — the bytes2 "grade" slot now carries a
// methodological confidence tier, not a credit rating.
export const CONFIDENCE_TIER_BYTES2: Record<string, string> = {
  "HI": "0x4849",  // High confidence (>=80% coverage)
  "ST": "0x5354",  // Standard confidence (>=60% coverage)
  "LD": "0x4c44",  // Limited Data (<60% coverage)
  "XX": "0x5858",  // Unknown / fallback
};

export function validateTierCode(code: string): boolean {
  return code in CONFIDENCE_TIER_BYTES2;
}
