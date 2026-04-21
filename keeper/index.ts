/**
 * Basis Keeper — main loop
 *
 * Flow: poll API → diff on-chain → publish deltas → log + alert
 *
 * Run:
 *   npx tsx keeper/index.ts
 *   npx tsx keeper/index.ts --dry-run
 */

import { ethers } from "ethers";
import { loadConfig } from "./config.js";
import { logger } from "./logger.js";
import { sendAlert, checkStaleness } from "./alerter.js";
import { TOKEN_ADDRESSES } from "./converter.js";
import { fetchOnChainScores, computeUpdates, type ApiScore } from "./differ.js";
import { publishUpdates, publishPsiScores, publishReportHashes, publishStateRoot, sleep, type ReportHashUpdate, type PsiScoreUpdate } from "./publisher.js";

// Hub API response envelope
interface HubScoresResponse {
  stablecoins: ApiScore[];
  count: number;
  formula_version: string;
  timestamp: string;
}

const FORMULA_VERSION = 100; // v1.0.0

async function fetchApiScores(apiUrl: string, endpoint: string): Promise<ApiScore[]> {
  const url = `${apiUrl}${endpoint}`;
  logger.info("Fetching scores from API", { url });

  const res = await fetch(url, {
    headers: { "Accept": "application/json" },
    signal: AbortSignal.timeout(30_000),
  });

  if (!res.ok) {
    throw new Error(`API returned HTTP ${res.status}: ${await res.text()}`);
  }

  const raw = await res.json() as ApiScore[] | HubScoresResponse;

  // Hub returns a wrapped envelope; flat array is also supported for local dev
  let scores: ApiScore[];
  if (Array.isArray(raw)) {
    scores = raw;
  } else if (raw && typeof raw === "object" && Array.isArray((raw as HubScoresResponse).stablecoins)) {
    scores = (raw as HubScoresResponse).stablecoins;
  } else {
    throw new Error(`Unexpected API response shape: ${JSON.stringify(raw).slice(0, 200)}`);
  }

  logger.info("Fetched API scores", { count: scores.length });
  return scores;
}

// PSI API response envelope
interface HubPsiResponse {
  protocols: Array<{
    protocol_slug: string;
    score: number;
    grade: string;
    computed_at?: string;
  }>;
  count: number;
}

const PSI_VERSION = 100; // psi-v0.1.0

function gradeToBytes2(grade: string): string {
  const g = (grade || "--").slice(0, 2).padEnd(2, " ");
  return (
    "0x" +
    g.charCodeAt(0).toString(16).padStart(2, "0") +
    g.charCodeAt(1).toString(16).padStart(2, "0")
  );
}

async function fetchPsiScores(apiUrl: string): Promise<PsiScoreUpdate[]> {
  const url = `${apiUrl}/api/psi/scores`;
  logger.info("Fetching PSI scores from API", { url });

  const res = await fetch(url, {
    headers: { "Accept": "application/json" },
    signal: AbortSignal.timeout(30_000),
  });

  if (!res.ok) {
    throw new Error(`PSI API returned HTTP ${res.status}: ${await res.text()}`);
  }

  const raw = (await res.json()) as HubPsiResponse;
  const protocols = raw.protocols || [];
  const cycleTimestamp = Math.floor(Date.now() / 1000);

  const updates: PsiScoreUpdate[] = protocols
    .filter((p) => p.score != null && p.grade)
    .map((p) => ({
      slug: p.protocol_slug,
      score: Math.round(p.score * 100),
      grade: gradeToBytes2(p.grade),
      timestamp: cycleTimestamp,
      version: PSI_VERSION,
    }));

  logger.info("Fetched PSI scores", { count: updates.length });
  return updates;
}

/**
 * Build a token address map from the API response.
 * Uses id.toLowerCase() → token_contract (Ethereum L1 address).
 * Falls back to the static TOKEN_ADDRESSES if any entry is missing the token_contract field.
 */
function buildTokenAddressMap(apiScores: ApiScore[]): Record<string, string> {
  const dynamic: Record<string, string> = {};
  let missingCount = 0;

  for (const s of apiScores) {
    if (s.token_contract) {
      dynamic[s.id.toLowerCase()] = s.token_contract.toLowerCase();
    } else {
      missingCount++;
    }
  }

  if (missingCount > 0) {
    logger.warn(
      "Some API entries are missing the token_contract field — falling back to static TOKEN_ADDRESSES",
      { missingCount, totalEntries: apiScores.length }
    );
    return TOKEN_ADDRESSES;
  }

  const newIds = Object.keys(dynamic).filter((id) => !(id in TOKEN_ADDRESSES));
  if (newIds.length > 0) {
    logger.info("Dynamic token map includes new tokens not in static map", { newIds });
  }

  return dynamic;
}

async function runCycle(
  config: ReturnType<typeof loadConfig>,
  walletBase: ethers.Wallet,
  walletArb: ethers.Wallet,
  providerBase: ethers.JsonRpcProvider,
  providerArb: ethers.JsonRpcProvider
): Promise<void> {
  const cycleStart = Date.now();
  const cycleTimestamp = Math.floor(cycleStart / 1000);

  logger.info("=== Keeper cycle start ===", { cycleTimestamp });

  // 1. Poll API — on failure, warn and fall back to static TOKEN_ADDRESSES so the
  //    cycle can still push staleness-detected updates for known tokens.
  let apiScores: ApiScore[] = [];
  let tokenAddresses: Record<string, string> = TOKEN_ADDRESSES;

  try {
    apiScores = await fetchApiScores(config.apiUrl, config.apiScoresEndpoint);

    // Build dynamic token address map from the API response.
    // Falls back to static TOKEN_ADDRESSES if any entry lacks the contract field.
    tokenAddresses = buildTokenAddressMap(apiScores);
  } catch (err) {
    logger.warn(
      "Failed to fetch API scores — skipping score updates, using static TOKEN_ADDRESSES for staleness check",
      { error: err instanceof Error ? err.message : String(err) }
    );
    await sendAlert(`Basis keeper: API fetch failed — ${err instanceof Error ? err.message : String(err)}`);
  }

  // 2. Diff on-chain state for both chains in parallel
  const [onChainBaseResult, onChainArbResult] = await Promise.allSettled([
    fetchOnChainScores(providerBase, config.chains.base.oracleAddress),
    fetchOnChainScores(providerArb, config.chains.arbitrum.oracleAddress),
  ]);

  const onChainBase = onChainBaseResult.status === "fulfilled"
    ? onChainBaseResult.value
    : (() => { logger.error("Failed to fetch on-chain scores from Base", { error: onChainBaseResult.reason?.stack ?? String(onChainBaseResult.reason) }); return new Map<string, import("./differ.js").OnChainScore>(); })();
  const onChainArb = onChainArbResult.status === "fulfilled"
    ? onChainArbResult.value
    : (() => { logger.error("Failed to fetch on-chain scores from Arbitrum", { error: onChainArbResult.reason?.stack ?? String(onChainArbResult.reason) }); return new Map<string, import("./differ.js").OnChainScore>(); })();

  const updatesBase = computeUpdates(
    apiScores,
    onChainBase,
    tokenAddresses,
    config.scoreChangeThreshold,
    FORMULA_VERSION,
    cycleTimestamp
  );

  const updatesArb = computeUpdates(
    apiScores,
    onChainArb,
    tokenAddresses,
    config.scoreChangeThreshold,
    FORMULA_VERSION,
    cycleTimestamp
  );

  logger.info("Diff complete", {
    baseUpdates: updatesBase.length,
    arbUpdates: updatesArb.length,
  });

  // 3. Publish deltas to both chains in parallel
  const [resultBase, resultArb] = await Promise.all([
    publishUpdates(
      updatesBase,
      providerBase,
      walletBase,
      config.chains.base.oracleAddress,
      "base",
      config
    ),
    publishUpdates(
      updatesArb,
      providerArb,
      walletArb,
      config.chains.arbitrum.oracleAddress,
      "arbitrum",
      config
    ),
  ]);

  // 3b. Publish PSI scores to both chains in parallel
  try {
    const psiUpdates = await fetchPsiScores(config.apiUrl);
    if (psiUpdates.length > 0) {
      const [psiBase, psiArb] = await Promise.all([
        publishPsiScores(
          psiUpdates, providerBase, walletBase,
          config.chains.base.oracleAddress, "base", config
        ),
        publishPsiScores(
          psiUpdates, providerArb, walletArb,
          config.chains.arbitrum.oracleAddress, "arbitrum", config
        ),
      ]);
      const psiResults = [psiBase, psiArb].filter(Boolean);
      logger.info("PSI scores published", {
        count: psiUpdates.length,
        chains: psiResults.map((r) => r!.chain),
      });
    }
  } catch (err) {
    logger.warn("PSI score publishing failed (non-blocking)", {
      error: err instanceof Error ? err.message : String(err),
    });
  }

  // 4. Log results
  const results = [resultBase, resultArb].filter(Boolean);
  if (results.length > 0) {
    logger.info("Cycle publish summary", {
      results: results.map((r) => ({
        chain: r!.chain,
        txHash: r!.txHash,
        updatesCount: r!.updatesCount,
      })),
    });
  } else {
    logger.info("No on-chain updates needed this cycle");
  }

  // 5. Staleness check (if configured)
  if (config.alertOnStaleness) {
    const maxAge = config.pollIntervalSeconds * 2;
    const knownTokens = Object.values(tokenAddresses);

    const oracleBaseAbi = [
      "function isStale(address token, uint256 maxAge) external view returns (bool)",
    ];
    const oracleBase = new ethers.Contract(
      config.chains.base.oracleAddress,
      oracleBaseAbi,
      providerBase
    );

    await checkStaleness(oracleBase as Parameters<typeof checkStaleness>[0], knownTokens, maxAge);
  }

  // 6. Publish report hashes for scored entities
  try {
    const reportUpdates = await fetchReportHashes(config.apiUrl);
    if (reportUpdates.length > 0) {
      const [pubBase, pubArb] = await Promise.all([
        publishReportHashes(reportUpdates, providerBase, walletBase, config.chains.base.oracleAddress, "base", config),
        publishReportHashes(reportUpdates, providerArb, walletArb, config.chains.arbitrum.oracleAddress, "arbitrum", config),
      ]);
      logger.info("Report hashes published", { base: pubBase, arbitrum: pubArb });
    }
  } catch (err) {
    logger.warn("Report hash publishing failed", { error: err instanceof Error ? err.message : String(err) });
  }

  // 7. Publish daily state root (once per day)
  try {
    const stateRootHash = await fetchStateRootHash(config.apiUrl);
    if (stateRootHash) {
      await Promise.all([
        publishStateRoot(stateRootHash, providerBase, walletBase, config.chains.base.oracleAddress, "base", config),
        publishStateRoot(stateRootHash, providerArb, walletArb, config.chains.arbitrum.oracleAddress, "arbitrum", config),
      ]);
    }
  } catch (err) {
    logger.warn("State root publishing failed", { error: err instanceof Error ? err.message : String(err) });
  }

  const cycleDuration = Date.now() - cycleStart;
  logger.info("=== Keeper cycle complete ===", { durationMs: cycleDuration });
}

async function fetchReportHashes(apiUrl: string): Promise<ReportHashUpdate[]> {
  try {
    // Get recent report attestations from hub
    const res = await fetch(`${apiUrl}/api/reports/lenses`, {
      signal: AbortSignal.timeout(15_000),
    });
    if (!res.ok) return [];

    // Fetch scores to get entity IDs
    const scoresRes = await fetch(`${apiUrl}/api/scores`, {
      signal: AbortSignal.timeout(15_000),
    });
    if (!scoresRes.ok) return [];
    const scoresData = await scoresRes.json() as any;
    const stablecoins = Array.isArray(scoresData) ? scoresData : scoresData.stablecoins || [];

    const updates: ReportHashUpdate[] = [];

    // For each stablecoin, check if it has a report attestation
    for (const coin of stablecoins.slice(0, 20)) {
      try {
        const rRes = await fetch(
          `${apiUrl}/api/reports/stablecoin/${coin.id || coin.symbol}?format=json&template=sbt_metadata`,
          { signal: AbortSignal.timeout(10_000) }
        );
        if (!rRes.ok) continue;
        const rData = await rRes.json() as any;
        const reportHash = rData.report_hash || rRes.headers.get("x-report-hash");
        if (!reportHash) continue;

        const entityId = ethers.keccak256(ethers.toUtf8Bytes(coin.id || coin.symbol));
        updates.push({
          entityId,
          reportHash: ethers.zeroPadValue(ethers.toBeHex(BigInt("0x" + reportHash.replace("0x", ""))), 32),
          lensId: "0x00000000",
        });
      } catch {
        // Skip individual failures
      }
    }

    logger.info("Fetched report hashes from hub", { count: updates.length });
    return updates;
  } catch (err) {
    logger.warn("Failed to fetch report hashes", { error: err instanceof Error ? err.message : String(err) });
    return [];
  }
}

async function fetchStateRootHash(apiUrl: string): Promise<string | null> {
  try {
    const res = await fetch(`${apiUrl}/api/state-root/latest`, {
      signal: AbortSignal.timeout(15_000),
    });
    if (!res.ok) return null;
    const data = await res.json() as any;
    const stateRoot = data.state_root;
    if (!stateRoot || !stateRoot.attestation_domains) return null;

    // Hash the state root object to get the on-chain commitment
    const canonical = JSON.stringify(stateRoot, Object.keys(stateRoot).sort());
    const hash = ethers.keccak256(ethers.toUtf8Bytes(canonical));
    logger.info("Fetched state root hash", { domains: stateRoot.domain_count, hash: hash.slice(0, 18) });
    return hash;
  } catch (err) {
    logger.warn("Failed to fetch state root", { error: err instanceof Error ? err.message : String(err) });
    return null;
  }
}

async function main(): Promise<void> {
  const config = loadConfig();

  logger.info("Basis Keeper starting", {
    apiUrl: config.apiUrl,
    baseOracle: config.chains.base.oracleAddress,
    arbOracle: config.chains.arbitrum.oracleAddress,
    pollIntervalSeconds: config.pollIntervalSeconds,
    threshold: config.scoreChangeThreshold,
    dryRun: config.dryRun,
  });

  const providerBase = new ethers.JsonRpcProvider(
    config.chains.base.rpcUrl,
    config.chains.base.chainId
  );
  const providerArb = new ethers.JsonRpcProvider(
    config.chains.arbitrum.rpcUrl,
    config.chains.arbitrum.chainId
  );

  const walletBase = new ethers.Wallet(config.keeperPrivateKey, providerBase);
  const walletArb  = new ethers.Wallet(config.keeperPrivateKey, providerArb);

  logger.info("Keeper wallet address", { address: walletBase.address });

  // Startup notification — catches restart loops
  await sendAlert(
    `Keeper started. Interval: ${config.pollIntervalSeconds}s. Wallet: ${walletBase.address}`
  );

  const intervalMs = config.pollIntervalSeconds * 1000;

  // Guard against restart-triggered duplicate cycles.
  // Query the on-chain stateRootTimestamp to see when the last full cycle completed.
  // If less than pollIntervalSeconds has elapsed, sleep the remainder.
  try {
    const guardOracle = new ethers.Contract(
      config.chains.base.oracleAddress,
      ["function stateRootTimestamp() external view returns (uint48)"],
      providerBase
    );
    const lastTs = Number(await guardOracle.stateRootTimestamp());
    if (lastTs > 0) {
      const now = Math.floor(Date.now() / 1000);
      const elapsed = now - lastTs;
      if (elapsed < config.pollIntervalSeconds) {
        const waitSec = config.pollIntervalSeconds - elapsed;
        logger.info("Recent cycle detected on-chain, deferring first run", {
          lastStateRootAge: elapsed,
          waitSeconds: waitSec,
        });
        await sleep(waitSec * 1000);
      }
    }
  } catch (err) {
    logger.warn("Startup guard check failed, proceeding immediately", {
      error: err instanceof Error ? err.message : String(err),
    });
  }

  // Check wallet balance and alert if low
  async function checkWalletBalance(): Promise<void> {
    try {
      const [balBase, balArb] = await Promise.all([
        providerBase.getBalance(walletBase.address),
        providerArb.getBalance(walletArb.address),
      ]);
      const ethBase = Number(ethers.formatEther(balBase));
      const ethArb = Number(ethers.formatEther(balArb));
      logger.info("Wallet balances", { base: ethBase.toFixed(6), arbitrum: ethArb.toFixed(6) });
      if (ethBase < 0.005) {
        await sendAlert(`Keeper wallet balance LOW: ${ethBase.toFixed(6)} ETH on Base`);
      }
      if (ethArb < 0.005) {
        await sendAlert(`Keeper wallet balance LOW: ${ethArb.toFixed(6)} ETH on Arbitrum`);
      }
    } catch (err) {
      logger.warn("Wallet balance check failed", {
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  // Check balance at startup
  await checkWalletBalance();

  // Log keeper cycle to the hub API for observability
  async function logCycleStart(trigger: string): Promise<number | null> {
    try {
      const res = await fetch(`${config.apiUrl}/api/ops/keeper-cycle/start`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ trigger_reason: trigger }),
        signal: AbortSignal.timeout(5_000),
      });
      if (res.ok) {
        const data = await res.json() as { id: number };
        return data.id;
      }
    } catch {
      // Non-blocking — cycle logging is best-effort
    }
    return null;
  }

  async function logCycleComplete(cycleId: number, durationMs: number, errors: string[]): Promise<void> {
    try {
      await fetch(`${config.apiUrl}/api/ops/keeper-cycle/${cycleId}/complete`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ duration_ms: durationMs, errors: errors.length > 0 ? errors : null }),
        signal: AbortSignal.timeout(5_000),
      });
    } catch {
      // Non-blocking
    }
  }

  // Run first cycle immediately (unless deferred above), then on schedule
  while (true) {
    const cycleStartMs = Date.now();
    const cycleId = await logCycleStart("scheduled");
    const cycleErrors: string[] = [];

    try {
      await runCycle(config, walletBase, walletArb, providerBase, providerArb);
    } catch (err) {
      const msg = `Unhandled error in keeper cycle`;
      const errStr = err instanceof Error ? (err.stack ?? err.message) : String(err);
      logger.error(msg, { error: errStr });
      cycleErrors.push(errStr);
      await sendAlert(msg, err);
    }

    if (cycleId != null) {
      await logCycleComplete(cycleId, Date.now() - cycleStartMs, cycleErrors);
    }

    // Check balance after each cycle
    await checkWalletBalance();

    logger.info(`Sleeping ${config.pollIntervalSeconds}s until next cycle`);
    await sleep(intervalMs);
  }
}

main().catch(async (err) => {
  logger.error("Fatal keeper error", { error: err instanceof Error ? (err.stack ?? err.message) : String(err) });
  await sendAlert("Keeper crashed — fatal error", err);
  process.exit(1);
});
