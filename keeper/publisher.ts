import { ethers } from "ethers";
import { logger } from "./logger.js";
import { sendAlert } from "./alerter.js";
import type { ScoreUpdate } from "./differ.js";
import type { KeeperConfig } from "./config.js";

const ORACLE_ABI = [
  "function batchUpdateScores(address[] calldata tokens, uint16[] calldata scores, bytes2[] calldata grades, uint48[] calldata timestamps, uint16[] calldata versions) external",
  "function batchUpdatePsiScores(string[] calldata slugs, uint16[] calldata scores, bytes2[] calldata grades, uint48[] calldata timestamps, uint16[] calldata versions) external",
  "function isStale(address token, uint256 maxAge) external view returns (bool)",
  "function publishReportHash(bytes32 entityId, bytes32 reportHash, bytes4 lensId) external",
  "function publishStateRoot(bytes32 stateRoot) external",
  "function reportTimestamps(bytes32 entityId) external view returns (uint48)",
  "function stateRootTimestamp() external view returns (uint48)",
];

const SBT_ABI = [
  "function mintRating(address recipient, bytes32 entityId, uint8 entityType, uint16 score, bytes2 grade, uint8 confidence, bytes32 reportHash, uint16 methodVersion) external returns (uint256)",
  "function updateRating(uint256 tokenId, uint16 score, bytes2 grade, uint8 confidence, bytes32 reportHash, uint16 methodVersion) external",
  "function entityToToken(bytes32 entityId) external view returns (uint256)",
];

// ============================================================
// Nonce manager — handles concurrent submissions across chains
// ============================================================

class NonceManager {
  private nonces: Map<string, number> = new Map();

  async getCurrentNonce(
    provider: ethers.JsonRpcProvider,
    address: string,
    chainKey: string
  ): Promise<number> {
    const cached = this.nonces.get(chainKey);
    const onChain = await provider.getTransactionCount(address, "pending");
    const nonce = Math.max(cached ?? 0, onChain);
    this.nonces.set(chainKey, nonce + 1);
    return nonce;
  }

  reset(chainKey: string): void {
    this.nonces.delete(chainKey);
  }
}

export const nonceManager = new NonceManager();

// ============================================================
// Retry with exponential backoff + jitter
// ============================================================

export async function withRetry<T>(
  fn: () => Promise<T>,
  config: { maxRetries: number; baseDelay: number; maxDelay: number },
  context: string
): Promise<T> {
  for (let attempt = 0; attempt <= config.maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      if (attempt === config.maxRetries) {
        await sendAlert(`FAILED after ${config.maxRetries} retries: ${context}`, error);
        throw error;
      }
      const delay = Math.min(
        config.baseDelay * Math.pow(2, attempt) + Math.random() * 1000,
        config.maxDelay
      );
      logger.warn(`Retry ${attempt + 1}/${config.maxRetries} in ${Math.round(delay)}ms: ${context}`);
      await sleep(delay);
    }
  }
  throw new Error("unreachable");
}

// ============================================================
// Publisher
// ============================================================

export interface PublishResult {
  chain: string;
  txHash: string;
  updatesCount: number;
  gasUsed?: bigint;
}

export async function publishUpdates(
  updates: ScoreUpdate[],
  provider: ethers.JsonRpcProvider,
  wallet: ethers.Wallet,
  oracleAddress: string,
  chainKey: string,
  config: KeeperConfig
): Promise<PublishResult | null> {
  if (updates.length === 0) {
    logger.info("No updates to publish", { chain: chainKey });
    return null;
  }

  if (config.dryRun) {
    logger.info("DRY RUN — would publish updates", {
      chain: chainKey,
      count: updates.length,
      tokens: updates.map((u) => u.token),
    });
    return null;
  }

  const feeData = await provider.getFeeData();
  const gasPriceGwei = feeData.gasPrice
    ? Number(ethers.formatUnits(feeData.gasPrice, "gwei"))
    : 0;

  if (gasPriceGwei > config.maxGasPriceGwei) {
    const msg = `Gas price ${gasPriceGwei.toFixed(3)} gwei exceeds cap ${config.maxGasPriceGwei} gwei on ${chainKey}`;
    logger.warn(msg);
    await sendAlert(msg);
    return null;
  }

  const oracle = new ethers.Contract(oracleAddress, ORACLE_ABI, wallet);

  const tokens     = updates.map((u) => u.token);
  const scores     = updates.map((u) => u.score);
  const grades     = updates.map((u) => u.grade);
  const timestamps = updates.map((u) => u.timestamp);
  const versions   = updates.map((u) => u.version);

  const nonce = await nonceManager.getCurrentNonce(provider, wallet.address, chainKey);

  const txHash = await withRetry(
    async () => {
      const tx = await (oracle.batchUpdateScores as ethers.ContractMethod)(
        tokens, scores, grades, timestamps, versions,
        {
          nonce,
          gasLimit: BigInt(config.gasLimitPerUpdate),
        }
      );

      logger.info("Transaction submitted", {
        chain: chainKey,
        txHash: tx.hash,
        nonce,
        updatesCount: updates.length,
      });

      const receipt = await tx.wait(1);

      logger.info("Transaction confirmed", {
        chain: chainKey,
        txHash: tx.hash,
        blockNumber: receipt?.blockNumber,
        gasUsed: receipt?.gasUsed?.toString(),
      });

      return tx.hash as string;
    },
    {
      maxRetries: config.maxRetries,
      baseDelay: config.baseRetryDelayMs,
      maxDelay: config.maxRetryDelayMs,
    },
    `batchUpdateScores on ${chainKey}`
  );

  return {
    chain: chainKey,
    txHash,
    updatesCount: updates.length,
  };
}

// ============================================================
// PSI score publishing
// ============================================================

export interface PsiScoreUpdate {
  slug: string;
  score: number;       // uint16 (float * 100)
  grade: string;       // bytes2 hex
  timestamp: number;   // uint48 unix seconds
  version: number;     // uint16
}

export async function publishPsiScores(
  updates: PsiScoreUpdate[],
  provider: ethers.JsonRpcProvider,
  wallet: ethers.Wallet,
  oracleAddress: string,
  chainKey: string,
  config: KeeperConfig
): Promise<PublishResult | null> {
  if (updates.length === 0) {
    logger.info("No PSI updates to publish", { chain: chainKey });
    return null;
  }

  if (config.dryRun) {
    logger.info("DRY RUN — would publish PSI scores", {
      chain: chainKey,
      count: updates.length,
      slugs: updates.map((u) => u.slug),
    });
    return null;
  }

  const feeData = await provider.getFeeData();
  const gasPriceGwei = feeData.gasPrice
    ? Number(ethers.formatUnits(feeData.gasPrice, "gwei"))
    : 0;

  if (gasPriceGwei > config.maxGasPriceGwei) {
    const msg = `Gas price ${gasPriceGwei.toFixed(3)} gwei exceeds cap ${config.maxGasPriceGwei} gwei on ${chainKey}`;
    logger.warn(msg);
    await sendAlert(msg);
    return null;
  }

  const oracle = new ethers.Contract(oracleAddress, ORACLE_ABI, wallet);

  const slugs      = updates.map((u) => u.slug);
  const scores     = updates.map((u) => u.score);
  const grades     = updates.map((u) => u.grade);
  const timestamps = updates.map((u) => u.timestamp);
  const versions   = updates.map((u) => u.version);

  const nonce = await nonceManager.getCurrentNonce(provider, wallet.address, chainKey);

  const txHash = await withRetry(
    async () => {
      const tx = await (oracle.batchUpdatePsiScores as ethers.ContractMethod)(
        slugs, scores, grades, timestamps, versions,
        {
          nonce,
          gasLimit: 200_000n,
        }
      );

      logger.info("PSI transaction submitted", {
        chain: chainKey,
        txHash: tx.hash,
        nonce,
        updatesCount: updates.length,
      });

      const receipt = await tx.wait(1);

      logger.info("PSI transaction confirmed", {
        chain: chainKey,
        txHash: tx.hash,
        blockNumber: receipt?.blockNumber,
        gasUsed: receipt?.gasUsed?.toString(),
      });

      return tx.hash as string;
    },
    {
      maxRetries: config.maxRetries,
      baseDelay: config.baseRetryDelayMs,
      maxDelay: config.maxRetryDelayMs,
    },
    `batchUpdatePsiScores on ${chainKey}`
  );

  return {
    chain: chainKey,
    txHash,
    updatesCount: updates.length,
  };
}

// ============================================================
// Report hash publishing
// ============================================================

export interface ReportHashUpdate {
  entityId: string;   // hex bytes32
  reportHash: string; // hex bytes32
  lensId: string;     // hex bytes4
}

export async function publishReportHashes(
  updates: ReportHashUpdate[],
  provider: ethers.JsonRpcProvider,
  wallet: ethers.Wallet,
  oracleAddress: string,
  chainKey: string,
  config: KeeperConfig
): Promise<number> {
  if (updates.length === 0 || config.dryRun) {
    if (config.dryRun && updates.length > 0) {
      logger.info("DRY RUN — would publish report hashes", { chain: chainKey, count: updates.length });
    }
    return 0;
  }

  const oracle = new ethers.Contract(oracleAddress, ORACLE_ABI, wallet);
  let published = 0;

  for (const u of updates) {
    try {
      const nonce = await nonceManager.getCurrentNonce(provider, wallet.address, chainKey);
      const tx = await (oracle.publishReportHash as ethers.ContractMethod)(
        u.entityId, u.reportHash, u.lensId,
        { nonce, gasLimit: 100_000n }
      );
      await tx.wait(1);
      published++;
      logger.info("Report hash published", { chain: chainKey, entityId: u.entityId.slice(0, 18), txHash: tx.hash });
    } catch (err) {
      logger.warn("Failed to publish report hash", {
        chain: chainKey,
        entityId: u.entityId.slice(0, 18),
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  return published;
}

// ============================================================
// State root publishing
// ============================================================

export async function publishStateRoot(
  stateRootHash: string,
  provider: ethers.JsonRpcProvider,
  wallet: ethers.Wallet,
  oracleAddress: string,
  chainKey: string,
  config: KeeperConfig
): Promise<boolean> {
  if (config.dryRun) {
    logger.info("DRY RUN — would publish state root", { chain: chainKey, hash: stateRootHash.slice(0, 18) });
    return false;
  }

  try {
    const oracle = new ethers.Contract(oracleAddress, ORACLE_ABI, wallet);

    // Check if today's state root is already published
    const existingTs = await oracle.stateRootTimestamp();
    const now = Math.floor(Date.now() / 1000);
    const oneDayAgo = now - 86400;

    if (Number(existingTs) > oneDayAgo) {
      logger.info("State root already published today, skipping", { chain: chainKey });
      return false;
    }

    const nonce = await nonceManager.getCurrentNonce(provider, wallet.address, chainKey);
    const tx = await (oracle.publishStateRoot as ethers.ContractMethod)(
      stateRootHash,
      { nonce, gasLimit: 80_000n }
    );
    await tx.wait(1);
    logger.info("State root published", { chain: chainKey, hash: stateRootHash.slice(0, 18), txHash: tx.hash });
    return true;
  } catch (err) {
    logger.warn("Failed to publish state root", {
      chain: chainKey,
      error: err instanceof Error ? err.message : String(err),
    });
    return false;
  }
}

// ============================================================
// Helpers
// ============================================================

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
