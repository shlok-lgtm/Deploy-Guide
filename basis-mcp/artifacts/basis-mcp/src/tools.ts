import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import {
  fetchScores,
  fetchScoreDetail,
  fetchWalletProfile,
  fetchRiskiestWallets,
  fetchBacklog,
  fetchMethodology,
  fetchPsiScores,
  fetchPsiDetail,
  fetchCqi,
  fetchProtocolExposure,
  fetchDriftExploitAnalysis,
} from "./api.js";
import {
  type GradeString,
  type OverallAssessment,
  type StablecoinScore,
  GRADE_ORDER,
  gradeRank,
  isGradeAtLeast,
} from "./config.js";

const API_ERROR_RESPONSE = {
  error: true,
  message: "Basis API unavailable. Please try again shortly.",
  retry_after_seconds: 30,
};

function formatMoney(value: number | undefined | null): string {
  if (value == null) return "unknown";
  if (value >= 1_000_000_000) return `$${(value / 1_000_000_000).toFixed(1)}B`;
  if (value >= 1_000_000) return `$${(value / 1_000_000).toFixed(1)}M`;
  if (value >= 1_000) return `$${(value / 1_000).toFixed(1)}K`;
  return `$${value.toFixed(2)}`;
}

function gradeAtLeast(
  grade: string | undefined | null,
  min: string,
): boolean {
  return isGradeAtLeast(grade, min);
}

function getCategoryScore(
  val: number | { score: number; weight: number } | undefined | null,
): number | undefined {
  if (val == null) return undefined;
  if (typeof val === "number") return val;
  return val.score;
}

export function registerTools(server: McpServer): void {
  const TOOL_ANNOTATIONS = {
    readOnlyHint: true,
    openWorldHint: true,
  };

  server.registerTool(
    "get_stablecoin_scores",
    {
      description:
        "Get current SII scores for all scored stablecoins. Use before any decision involving stablecoins — portfolio assessment, swap routing, or collateral evaluation.",
      inputSchema: z.object({
        min_grade: z
          .enum(["A+", "A", "A-", "B+", "B", "B-", "C+", "C", "C-", "D", "F"])
          .optional()
          .describe(
            "Optional minimum grade filter (e.g. 'B+' returns B+ and above)",
          ),
        sort_by: z
          .enum(["score_desc", "score_asc", "name"])
          .optional()
          .default("score_desc")
          .describe("Sort order for results"),
      }),
      annotations: TOOL_ANNOTATIONS,
    },
    async ({ min_grade, sort_by }) => {
      try {
        const data = await fetchScores();
        let coins: StablecoinScore[] =
          (data.stablecoins ?? data.scores ?? []) as StablecoinScore[];

        if (min_grade) {
          const minRank = GRADE_ORDER[min_grade as GradeString] ?? 0;
          coins = coins.filter(
            (c) => (GRADE_ORDER[c.grade] ?? 0) >= minRank,
          );
        }

        const sortKey = sort_by ?? "score_desc";
        if (sortKey === "score_desc") {
          coins.sort((a, b) => b.score - a.score);
        } else if (sortKey === "score_asc") {
          coins.sort((a, b) => a.score - b.score);
        } else {
          coins.sort((a, b) => a.symbol.localeCompare(b.symbol));
        }

        return {
          content: [
            {
              type: "text" as const,
              text: JSON.stringify(
                {
                  stablecoins: coins,
                  count: coins.length,
                  formula_version: data.formula_version ?? "sii-v1.0.0",
                  timestamp: data.timestamp ?? new Date().toISOString(),
                  methodology_summary:
                    data.methodology_summary ??
                    "SII = 0.30×Peg + 0.25×Liquidity + 0.15×Flows + 0.10×Distribution + 0.20×Structural",
                },
                null,
                2,
              ),
            },
          ],
        };
      } catch {
        return {
          content: [
            { type: "text" as const, text: JSON.stringify(API_ERROR_RESPONSE) },
          ],
        };
      }
    },
  );

  server.registerTool(
    "get_stablecoin_detail",
    {
      description:
        "Full score breakdown for a specific stablecoin including category scores, structural subscores, and methodology version. Use to deep-dive into why an asset scored the way it did.",
      inputSchema: z.object({
        coin: z
          .string()
          .describe("Stablecoin identifier (e.g. 'usdc', 'usdt', 'dai')"),
      }),
      annotations: TOOL_ANNOTATIONS,
    },
    async ({ coin }) => {
      try {
        const data = await fetchScoreDetail(coin);

        if (data.__status === 404) {
          return {
            content: [
              {
                type: "text" as const,
                text: JSON.stringify({
                  error: false,
                  is_scored: false,
                  symbol: coin.toUpperCase(),
                  message:
                    "This stablecoin is not yet scored by SII. It may appear in the scoring backlog.",
                }),
              },
            ],
          };
        }

        const cats = data.categories ?? {};
        const catScores: Record<string, number> = {};
        for (const [k, v] of Object.entries(cats)) {
          const s = getCategoryScore(v as Parameters<typeof getCategoryScore>[0]);
          if (s != null) catScores[k] = s;
        }

        let weakest: string | undefined;
        let strongest: string | undefined;
        if (Object.keys(catScores).length > 0) {
          weakest = Object.entries(catScores).sort(
            ([, a], [, b]) => a - b,
          )[0]?.[0];
          strongest = Object.entries(catScores).sort(
            ([, a], [, b]) => b - a,
          )[0]?.[0];
        }

        return {
          content: [
            {
              type: "text" as const,
              text: JSON.stringify(
                {
                  ...data,
                  weakest_category: data.weakest_category ?? weakest,
                  strongest_category: data.strongest_category ?? strongest,
                },
                null,
                2,
              ),
            },
          ],
        };
      } catch {
        return {
          content: [
            { type: "text" as const, text: JSON.stringify(API_ERROR_RESPONSE) },
          ],
        };
      }
    },
  );

  server.registerTool(
    "get_wallet_risk",
    {
      description:
        "Get risk profile for a specific Ethereum wallet — composite risk score, concentration risk, coverage quality, dominant holdings. Use for counterparty due diligence before a transaction.",
      inputSchema: z.object({
        address: z
          .string()
          .regex(/^0x/i, "Address must start with 0x")
          .describe("Ethereum wallet address (0x-prefixed, 42 characters)"),
      }),
      annotations: TOOL_ANNOTATIONS,
    },
    async ({ address }) => {
      try {
        const data = await fetchWalletProfile(address);

        if (data.__status === 404 || data.found_in_index === false) {
          return {
            content: [
              {
                type: "text" as const,
                text: JSON.stringify({
                  error: false,
                  found_in_index: false,
                  address,
                  message:
                    "Wallet not yet indexed by Basis. Only wallets with stablecoin holdings are indexed.",
                }),
              },
            ],
          };
        }

        const totalVal = formatMoney(data.total_stablecoin_value);
        const dominantPct =
          data.concentration?.dominant_asset_pct?.toFixed(1) ?? "?";
        const dominantAsset =
          data.concentration?.dominant_asset ?? "unknown";
        const unscoredPct =
          data.coverage?.unscored_pct?.toFixed(1) ?? "0";

        const result = {
          ...data,
          found_in_index: true,
          concentration: {
            ...data.concentration,
            interpretation:
              data.concentration?.interpretation ??
              `${dominantPct}% concentrated in ${dominantAsset}`,
          },
          coverage: {
            ...data.coverage,
            interpretation:
              data.coverage?.interpretation ??
              `${(100 - parseFloat(unscoredPct)).toFixed(1)}% of stablecoin value has SII coverage`,
          },
          risk_interpretation:
            `This wallet holds ${totalVal} in stablecoins with a ${data.risk_grade ?? "?"} risk grade. ` +
            `Primary exposure is ${dominantAsset} (${dominantPct}%). ` +
            `Coverage is ${data.coverage?.quality ?? "unknown"} with ${(100 - parseFloat(unscoredPct)).toFixed(1)}% of value scored by SII.`,
        };

        return {
          content: [
            {
              type: "text" as const,
              text: JSON.stringify(result, null, 2),
            },
          ],
        };
      } catch {
        return {
          content: [
            { type: "text" as const, text: JSON.stringify(API_ERROR_RESPONSE) },
          ],
        };
      }
    },
  );

  server.registerTool(
    "get_wallet_holdings",
    {
      description:
        "Detailed holdings breakdown for an Ethereum wallet with per-asset SII scores. Use to understand exactly what a wallet holds and identify unscored exposure.",
      inputSchema: z.object({
        address: z
          .string()
          .describe("Ethereum wallet address (0x-prefixed)"),
      }),
      annotations: TOOL_ANNOTATIONS,
    },
    async ({ address }) => {
      try {
        const data = await fetchWalletProfile(address);

        if (data.__status === 404 || data.found_in_index === false) {
          return {
            content: [
              {
                type: "text" as const,
                text: JSON.stringify({
                  error: false,
                  found_in_index: false,
                  address,
                  message:
                    "Wallet not yet indexed by Basis. Only wallets with stablecoin holdings are indexed.",
                }),
              },
            ],
          };
        }

        const holdings = (data.holdings ?? []).map((h) => {
          const pct = h.pct_of_wallet?.toFixed(1) ?? "?";
          let risk_contribution: string;
          if (h.is_scored && h.sii_grade) {
            risk_contribution = `${h.sii_grade} grade. ${pct}% of portfolio.`;
          } else {
            risk_contribution = `UNSCORED — no SII coverage. ${pct}% exposure without risk assessment.`;
          }
          return { ...h, risk_contribution };
        });

        const scoredValue = holdings
          .filter((h) => h.is_scored)
          .reduce((sum, h) => sum + (h.value_usd ?? 0), 0);
        const unscoredValue = holdings
          .filter((h) => !h.is_scored)
          .reduce((sum, h) => sum + (h.value_usd ?? 0), 0);

        return {
          content: [
            {
              type: "text" as const,
              text: JSON.stringify(
                {
                  address,
                  total_stablecoin_value: data.total_stablecoin_value,
                  holdings,
                  scored_value: scoredValue,
                  unscored_value: unscoredValue,
                  indexed_at: data.last_indexed_at,
                },
                null,
                2,
              ),
            },
          ],
        };
      } catch {
        return {
          content: [
            { type: "text" as const, text: JSON.stringify(API_ERROR_RESPONSE) },
          ],
        };
      }
    },
  );

  server.registerTool(
    "get_riskiest_wallets",
    {
      description:
        "Wallets with the most capital at risk — lowest risk scores weighted by total value. Use for systemic risk monitoring and identifying wallets most impacted by a stablecoin failure.",
      inputSchema: z.object({
        limit: z
          .number()
          .int()
          .min(1)
          .max(100)
          .optional()
          .default(20)
          .describe("Number of wallets to return (1–100, default 20)"),
      }),
      annotations: TOOL_ANNOTATIONS,
    },
    async ({ limit }) => {
      try {
        const data = await fetchRiskiestWallets(limit ?? 20);
        const wallets = (data.wallets ?? []).map((w) => {
          const val = formatMoney(w.total_stablecoin_value);
          const unscoredVal = formatMoney(
            ((w.unscored_pct ?? 0) / 100) * (w.total_stablecoin_value ?? 0),
          );
          const interpretation =
            w.capital_at_risk_interpretation ??
            `${val} in stablecoins with ${w.risk_grade} grade. ` +
              `${w.dominant_asset_pct?.toFixed(1) ?? "?"}% concentrated in ${w.dominant_asset ?? "unknown"}. ` +
              `${unscoredVal} in unscored assets.`;
          return { ...w, capital_at_risk_interpretation: interpretation };
        });

        const totalAtRisk =
          data.total_at_risk_capital ??
          wallets.reduce((s, w) => s + (w.total_stablecoin_value ?? 0), 0);

        return {
          content: [
            {
              type: "text" as const,
              text: JSON.stringify(
                {
                  wallets,
                  count: wallets.length,
                  total_at_risk_capital: totalAtRisk,
                  timestamp: data.timestamp ?? new Date().toISOString(),
                },
                null,
                2,
              ),
            },
          ],
        };
      } catch {
        return {
          content: [
            { type: "text" as const, text: JSON.stringify(API_ERROR_RESPONSE) },
          ],
        };
      }
    },
  );

  server.registerTool(
    "get_scoring_backlog",
    {
      description:
        "Unscored stablecoin assets ranked by total capital exposure across all indexed wallets. Shows which unscored assets represent the most risk.",
      inputSchema: z.object({
        limit: z
          .number()
          .int()
          .min(1)
          .max(100)
          .optional()
          .default(20)
          .describe("Number of backlog items to return (1–100, default 20)"),
      }),
      annotations: TOOL_ANNOTATIONS,
    },
    async ({ limit }) => {
      try {
        const data = await fetchBacklog(limit ?? 20);
        const items = (data.backlog ?? []).map((item) => {
          const totalVal = formatMoney(item.total_value_held);
          const maxVal = formatMoney(item.max_single_holding);
          const interpretation =
            item.coverage_gap_interpretation ??
            `${totalVal} across ${item.wallets_holding ?? 0} wallets has no SII coverage. Largest single exposure: ${maxVal}.`;
          return { ...item, coverage_gap_interpretation: interpretation };
        });

        const totalUnscored =
          data.total_unscored_value ??
          items.reduce((s, i) => s + (i.total_value_held ?? 0), 0);

        return {
          content: [
            {
              type: "text" as const,
              text: JSON.stringify(
                {
                  backlog: items,
                  count: items.length,
                  total_unscored_value: totalUnscored,
                  timestamp: data.timestamp ?? new Date().toISOString(),
                },
                null,
                2,
              ),
            },
          ],
        };
      } catch {
        return {
          content: [
            { type: "text" as const, text: JSON.stringify(API_ERROR_RESPONSE) },
          ],
        };
      }
    },
  );

  server.registerTool(
    "check_transaction_risk",
    {
      description:
        "Composite risk assessment for a proposed stablecoin transaction — evaluates the asset, sender wallet, and receiver wallet. The core agent decision gate before executing any stablecoin transfer, swap, or deposit.",
      inputSchema: z.object({
        from_address: z
          .string()
          .describe("Sender's Ethereum wallet address"),
        to_address: z
          .string()
          .describe("Receiver's Ethereum wallet address"),
        asset_symbol: z
          .string()
          .describe(
            "Symbol of the stablecoin being transferred (e.g. 'usdc', 'dai')",
          ),
      }),
      annotations: TOOL_ANNOTATIONS,
    },
    async ({ from_address, to_address, asset_symbol }) => {
      try {
        const [assetResult, senderResult, receiverResult] = await Promise.all([
          fetchScoreDetail(asset_symbol),
          fetchWalletProfile(from_address),
          fetchWalletProfile(to_address),
        ]);

        const assetNotFound = assetResult.__status === 404;
        const senderNotFound =
          senderResult.__status === 404 ||
          senderResult.found_in_index === false;
        const receiverNotFound =
          receiverResult.__status === 404 ||
          receiverResult.found_in_index === false;

        const assetGrade = assetResult.grade;
        const senderGrade = senderResult.risk_grade;
        const receiverGrade = receiverResult.risk_grade;
        const senderCoverage = senderResult.coverage?.quality;
        const receiverCoverage = receiverResult.coverage?.quality;

        let overall_assessment: OverallAssessment;
        const risk_factors: string[] = [];

        if (assetNotFound) {
          overall_assessment = "UNKNOWN";
          risk_factors.push(
            `Asset ${asset_symbol.toUpperCase()} is not scored by SII — risk cannot be assessed.`,
          );
        } else if (
          gradeAtLeast(assetGrade, "C+") === false ||
          (!senderNotFound && !gradeAtLeast(senderGrade, "C")) ||
          (!receiverNotFound && !gradeAtLeast(receiverGrade, "C")) ||
          senderNotFound ||
          receiverNotFound
        ) {
          overall_assessment = "HIGH_RISK";
        } else if (
          gradeAtLeast(assetGrade, "A-") &&
          gradeAtLeast(senderGrade, "B") &&
          gradeAtLeast(receiverGrade, "B") &&
          (senderCoverage === "full" || senderCoverage === "high") &&
          (receiverCoverage === "full" || receiverCoverage === "high")
        ) {
          overall_assessment = "LOW_RISK";
        } else {
          overall_assessment = "MEDIUM_RISK";
        }

        if (!assetNotFound) {
          if (gradeAtLeast(assetGrade, "B+")) {
            risk_factors.push(
              `Asset ${assetResult.symbol ?? asset_symbol.toUpperCase()} has strong SII score (${assetResult.score?.toFixed(1) ?? "?"}, ${assetGrade}).`,
            );
          } else if (gradeAtLeast(assetGrade, "C+")) {
            risk_factors.push(
              `Asset ${assetResult.symbol ?? asset_symbol.toUpperCase()} has moderate SII score (${assetResult.score?.toFixed(1) ?? "?"}, ${assetGrade}).`,
            );
          } else {
            risk_factors.push(
              `Asset ${assetResult.symbol ?? asset_symbol.toUpperCase()} has weak SII score (${assetResult.score?.toFixed(1) ?? "?"}, ${assetGrade}).`,
            );
          }
        }

        if (senderNotFound) {
          risk_factors.push(
            "Sender wallet not found in index — no stablecoin exposure data available.",
          );
        } else {
          const conc = senderResult.concentration;
          if (conc?.grade && !gradeAtLeast(conc.grade, "B")) {
            risk_factors.push(
              `Sender wallet has high concentration risk (${conc.dominant_asset_pct?.toFixed(1) ?? "?"}% in ${conc.dominant_asset ?? "unknown"}).`,
            );
          } else {
            risk_factors.push(
              `Sender wallet risk grade: ${senderGrade ?? "?"}.`,
            );
          }
        }

        if (receiverNotFound) {
          risk_factors.push(
            "Receiver wallet not found in index — no stablecoin exposure data available.",
          );
        } else {
          const unscPct = receiverResult.coverage?.unscored_pct ?? 0;
          if (unscPct > 10) {
            risk_factors.push(
              `Receiver wallet has ${unscPct.toFixed(1)}% unscored stablecoin exposure (${formatMoney(((unscPct / 100) * (receiverResult.total_stablecoin_value ?? 0)))}).`,
            );
          } else {
            risk_factors.push(
              `Receiver wallet risk grade: ${receiverGrade ?? "?"}.`,
            );
          }
        }

        const recommendationMap: Record<OverallAssessment, string> = {
          LOW_RISK:
            "Asset is well-scored and both wallets have strong risk profiles. Proceed with confidence.",
          MEDIUM_RISK:
            "Proceed with awareness — review the risk factors below before executing.",
          HIGH_RISK:
            "Do not proceed without further review. One or more critical risk factors identified.",
          UNKNOWN:
            "Cannot assess — asset is not yet scored by SII. Check the scoring backlog.",
        };

        return {
          content: [
            {
              type: "text" as const,
              text: JSON.stringify(
                {
                  transaction_risk: {
                    overall_assessment,
                    recommendation: recommendationMap[overall_assessment],
                    risk_factors,
                  },
                  asset: assetNotFound
                    ? {
                        symbol: asset_symbol.toUpperCase(),
                        is_scored: false,
                        message: "Not scored by SII",
                      }
                    : {
                        symbol: assetResult.symbol,
                        sii_score: assetResult.score,
                        sii_grade: assetGrade,
                        is_scored: true,
                        weakest_category: assetResult.weakest_category,
                      },
                  sender: senderNotFound
                    ? { address: from_address, found_in_index: false }
                    : {
                        address: from_address,
                        risk_score: senderResult.risk_score,
                        risk_grade: senderGrade,
                        total_stablecoin_value:
                          senderResult.total_stablecoin_value,
                        coverage_quality: senderCoverage,
                        found_in_index: true,
                      },
                  receiver: receiverNotFound
                    ? { address: to_address, found_in_index: false }
                    : {
                        address: to_address,
                        risk_score: receiverResult.risk_score,
                        risk_grade: receiverGrade,
                        total_stablecoin_value:
                          receiverResult.total_stablecoin_value,
                        coverage_quality: receiverCoverage,
                        unscored_pct: receiverResult.coverage?.unscored_pct,
                        found_in_index: true,
                      },
                  timestamp: new Date().toISOString(),
                },
                null,
                2,
              ),
            },
          ],
        };
      } catch {
        return {
          content: [
            { type: "text" as const, text: JSON.stringify(API_ERROR_RESPONSE) },
          ],
        };
      }
    },
  );

  server.registerTool(
    "get_methodology",
    {
      description:
        "Returns the current SII formula, category weights, structural subweights, grade scale, data sources, and version information. Use for explaining scoring decisions and audit transparency.",
      inputSchema: z.object({}),
      annotations: TOOL_ANNOTATIONS,
    },
    async () => {
      try {
        const data = await fetchMethodology();

        const result = {
          ...data,
          verification: {
            methodology_is_public: true,
            scores_are_deterministic: true,
            no_customer_specific_adjustments: true,
            methodology_locked_since: "2026-01-15",
            ...(data.verification ?? {}),
          },
          wallet_scoring: {
            version: "wallet-v1.0.0",
            method:
              "Value-weighted average of SII scores across scored holdings",
            concentration: "Herfindahl-Hirschman Index normalized to 0–100",
            ...(data.wallet_scoring ?? {}),
          },
        };

        return {
          content: [
            {
              type: "text" as const,
              text: JSON.stringify(result, null, 2),
            },
          ],
        };
      } catch {
        return {
          content: [
            { type: "text" as const, text: JSON.stringify(API_ERROR_RESPONSE) },
          ],
        };
      }
    },
  );

  // ===========================================================================
  // PSI (Protocol Solvency Index) Tools
  // ===========================================================================

  server.registerTool(
    "get_protocol_score",
    {
      description:
        "Get the PSI (Protocol Solvency Index) score for a DeFi protocol. Returns score (0-100), grade, and category breakdown across balance sheet, revenue, liquidity, security, governance, and token health. Covers Ethereum and Solana protocols.",
      inputSchema: z.object({
        slug: z
          .string()
          .describe(
            "Protocol slug (e.g. 'drift', 'aave', 'jupiter-perpetual-exchange', 'raydium', 'compound-finance')",
          ),
      }),
      annotations: TOOL_ANNOTATIONS,
    },
    async ({ slug }) => {
      try {
        const data = await fetchPsiDetail(slug);

        if (data.__status === 404) {
          return {
            content: [
              {
                type: "text" as const,
                text: JSON.stringify({
                  error: false,
                  is_scored: false,
                  protocol_slug: slug,
                  message:
                    "This protocol is not yet scored by PSI. Currently scoring 13 protocols across Ethereum and Solana.",
                }),
              },
            ],
          };
        }

        const cats = data.category_scores ?? {};
        let weakest: string | undefined;
        let strongest: string | undefined;
        if (Object.keys(cats).length > 0) {
          weakest = Object.entries(cats).sort(
            ([, a], [, b]) => a - b,
          )[0]?.[0];
          strongest = Object.entries(cats).sort(
            ([, a], [, b]) => b - a,
          )[0]?.[0];
        }

        return {
          content: [
            {
              type: "text" as const,
              text: JSON.stringify(
                {
                  protocol_slug: data.protocol_slug,
                  protocol_name: data.protocol_name,
                  psi_score: data.score,
                  psi_grade: data.grade,
                  category_scores: cats,
                  weakest_category: weakest,
                  strongest_category: strongest,
                  formula_version: data.formula_version,
                  computed_at: data.computed_at,
                  methodology_summary:
                    "PSI = 0.25×Balance Sheet + 0.20×Revenue + 0.20×Liquidity + 0.15×Security + 0.10×Governance + 0.10×Token Health",
                },
                null,
                2,
              ),
            },
          ],
        };
      } catch {
        return {
          content: [
            { type: "text" as const, text: JSON.stringify(API_ERROR_RESPONSE) },
          ],
        };
      }
    },
  );

  server.registerTool(
    "get_protocol_rankings",
    {
      description:
        "Get ranked list of all scored DeFi protocols by PSI score. Currently scores 13 protocols across Ethereum and Solana including Aave, Compound, Drift, Jupiter, and Raydium.",
      inputSchema: z.object({
        min_grade: z
          .enum(["A+", "A", "A-", "B+", "B", "B-", "C+", "C", "C-", "D", "F"])
          .optional()
          .describe(
            "Optional minimum grade filter (e.g. 'B' returns B and above)",
          ),
      }),
      annotations: TOOL_ANNOTATIONS,
    },
    async ({ min_grade }) => {
      try {
        const data = await fetchPsiScores();
        let protocols = (data.protocols ?? []).map((p) => ({
          protocol_slug: p.protocol_slug,
          protocol_name: p.protocol_name,
          psi_score: p.score,
          psi_grade: p.grade,
          category_scores: p.category_scores,
          computed_at: p.computed_at,
        }));

        if (min_grade) {
          const minRank = GRADE_ORDER[min_grade as GradeString] ?? 0;
          protocols = protocols.filter(
            (p) => (GRADE_ORDER[p.psi_grade ?? ""] ?? 0) >= minRank,
          );
        }

        protocols.sort(
          (a, b) => (b.psi_score ?? 0) - (a.psi_score ?? 0),
        );

        return {
          content: [
            {
              type: "text" as const,
              text: JSON.stringify(
                {
                  protocols,
                  count: protocols.length,
                  index: "psi",
                  version: data.version ?? "v0.1.0",
                  timestamp: new Date().toISOString(),
                },
                null,
                2,
              ),
            },
          ],
        };
      } catch {
        return {
          content: [
            { type: "text" as const, text: JSON.stringify(API_ERROR_RESPONSE) },
          ],
        };
      }
    },
  );

  server.registerTool(
    "get_cqi",
    {
      description:
        "Get the Composite Quality Index (CQI) for a stablecoin-protocol pair. CQI = sqrt(SII x PSI) — measures the combined risk of holding a specific stablecoin within a specific protocol. Example: CQI(USDC, Drift) tells you the risk of USDC deposited in Drift vaults.",
      inputSchema: z.object({
        asset: z
          .string()
          .describe("Stablecoin symbol (e.g. 'usdc', 'usdt', 'dai')"),
        protocol: z
          .string()
          .describe(
            "Protocol slug (e.g. 'drift', 'aave', 'compound-finance')",
          ),
      }),
      annotations: TOOL_ANNOTATIONS,
    },
    async ({ asset, protocol }) => {
      try {
        const data = await fetchCqi(asset, protocol);

        if (data.__status === 404 || data.error) {
          return {
            content: [
              {
                type: "text" as const,
                text: JSON.stringify({
                  error: false,
                  asset: asset.toUpperCase(),
                  protocol,
                  message:
                    data.error ??
                    "CQI not available for this pair. Ensure both SII and PSI scores exist.",
                }),
              },
            ],
          };
        }

        const sii = data.inputs?.sii?.score;
        const psi = data.inputs?.psi?.score;
        const interpretation =
          data.cqi_score != null && sii != null && psi != null
            ? `${asset.toUpperCase()} in ${data.protocol ?? protocol}: CQI ${data.cqi_score.toFixed(1)} (${data.cqi_grade}). ` +
              `SII ${sii.toFixed(1)} (stablecoin quality) × PSI ${psi.toFixed(1)} (protocol solvency) = ${data.cqi_score.toFixed(1)} composite risk.`
            : undefined;

        return {
          content: [
            {
              type: "text" as const,
              text: JSON.stringify(
                {
                  asset: data.asset ?? asset.toUpperCase(),
                  protocol: data.protocol ?? protocol,
                  protocol_slug: data.protocol_slug ?? protocol,
                  cqi_score: data.cqi_score,
                  cqi_grade: data.cqi_grade,
                  inputs: data.inputs,
                  method: data.method ?? "geometric_mean",
                  interpretation,
                  formula_version:
                    data.formula_version ?? "composition-v1.0.0",
                },
                null,
                2,
              ),
            },
          ],
        };
      } catch {
        return {
          content: [
            { type: "text" as const, text: JSON.stringify(API_ERROR_RESPONSE) },
          ],
        };
      }
    },
  );

  server.registerTool(
    "get_protocol_exposure",
    {
      description:
        "Get a protocol's stablecoin exposure — which stablecoins it holds in treasury and accepts as collateral, cross-referenced against SII scores. Shows unscored stablecoins the protocol accepts. Essential for understanding protocol-level stablecoin risk.",
      inputSchema: z.object({
        slug: z
          .string()
          .describe("Protocol slug (e.g. 'drift', 'aave', 'morpho')"),
      }),
      annotations: TOOL_ANNOTATIONS,
    },
    async ({ slug }) => {
      try {
        const data = await fetchProtocolExposure(slug);

        if (data.__status === 404) {
          return {
            content: [
              {
                type: "text" as const,
                text: JSON.stringify({
                  error: false,
                  protocol_slug: slug,
                  message:
                    "Protocol not found or no exposure data available.",
                }),
              },
            ],
          };
        }

        const treasury = data.treasury_stablecoin_exposure;
        const collateral = data.collateral_stablecoin_exposure;

        const treasuryVal = formatMoney(treasury?.total_usd);
        const collateralVal = formatMoney(collateral?.total_tvl_usd);
        const siiPct = collateral?.sii_scored_pct;

        const interpretation =
          `${data.name ?? slug} holds ${treasuryVal} in treasury stablecoins ` +
          `and accepts ${collateralVal} in stablecoin collateral. ` +
          `${siiPct != null ? `${siiPct.toFixed(1)}% of collateral is SII-scored.` : ""}`;

        return {
          content: [
            {
              type: "text" as const,
              text: JSON.stringify(
                {
                  protocol_slug: data.slug ?? slug,
                  protocol_name: data.name,
                  psi_score: data.psi_score,
                  treasury: treasury,
                  collateral: collateral,
                  interpretation,
                },
                null,
                2,
              ),
            },
          ],
        };
      } catch {
        return {
          content: [
            { type: "text" as const, text: JSON.stringify(API_ERROR_RESPONSE) },
          ],
        };
      }
    },
  );

  server.registerTool(
    "get_drift_exploit_analysis",
    {
      description:
        "Structured analysis of the Drift Protocol exploit (April 1, 2026, ~$270M drained). Returns PSI score, CQI composition with USDC, stablecoin exposure, market impact, and narrative. Demonstrates how CQI captures protocol-level risk that SII alone misses.",
      inputSchema: z.object({}),
      annotations: TOOL_ANNOTATIONS,
    },
    async () => {
      try {
        const data = await fetchDriftExploitAnalysis();

        if (data.__status === 404) {
          return {
            content: [
              {
                type: "text" as const,
                text: JSON.stringify({
                  error: false,
                  message:
                    "Drift exploit analysis not yet available. The endpoint requires PSI scoring to be deployed.",
                }),
              },
            ],
          };
        }

        return {
          content: [
            {
              type: "text" as const,
              text: JSON.stringify(data, null, 2),
            },
          ],
        };
      } catch {
        return {
          content: [
            { type: "text" as const, text: JSON.stringify(API_ERROR_RESPONSE) },
          ],
        };
      }
    },
  );
}
