import { ethers } from "ethers";
import { scoreToUint16, gradeToBytes2, bytes2ToGrade } from "./converter.js";
import { logger } from "./logger.js";

export interface ApiScore {
  id: string;
  name: string;
  symbol: string;
  issuer?: string;
  token_contract: string;  // Ethereum L1 contract address (canonical identifier)
  score: number;
  grade: string;
  computed_at?: string;    // ISO 8601 timestamp from hub API
}

export interface OnChainScore {
  token: string;
  score: number;       // uint16 0-10000
  grade: string;       // bytes2 hex
  timestamp: number;   // uint48 unix seconds
  version: number;     // uint16
}

export interface ScoreUpdate {
  token: string;
  score: number;       // uint16
  grade: string;       // bytes2 hex
  timestamp: number;   // uint48
  version: number;     // uint16
}

const ORACLE_ABI = [
  "function getAllScores() external view returns (address[] tokens, tuple(uint16 score, bytes2 grade, uint48 timestamp, uint16 version)[] scores)",
];

export async function fetchOnChainScores(
  provider: ethers.JsonRpcProvider,
  oracleAddress: string
): Promise<Map<string, OnChainScore>> {
  const oracle = new ethers.Contract(oracleAddress, ORACLE_ABI, provider);

  const [tokens, rawScores] = await oracle.getAllScores() as [string[], { score: bigint; grade: string; timestamp: bigint; version: bigint }[]];

  const map = new Map<string, OnChainScore>();
  for (let i = 0; i < tokens.length; i++) {
    const token = tokens[i].toLowerCase();
    const raw = rawScores[i];
    map.set(token, {
      token,
      score: Number(raw.score),
      grade: raw.grade.toLowerCase(),
      timestamp: Number(raw.timestamp),
      version: Number(raw.version),
    });
  }
  return map;
}

export function computeUpdates(
  apiScores: ApiScore[],
  onChainScores: Map<string, OnChainScore>,
  tokenAddresses: Record<string, string>,
  threshold: number,
  version: number,
  cycleTimestamp: number
): ScoreUpdate[] {
  const updates: ScoreUpdate[] = [];
  const HEARTBEAT_MAX_AGE_SECONDS = 24 * 60 * 60; // 24 hours

  for (const apiScore of apiScores) {
    const tokenAddress = tokenAddresses[apiScore.id.toLowerCase()];

    if (!tokenAddress) {
      logger.warn("Unknown stablecoin ID — add to TOKEN_ADDRESSES to support it", {
        id: apiScore.id,
      });
      continue;
    }

    const onChain = onChainScores.get(tokenAddress.toLowerCase());
    const newScore = scoreToUint16(apiScore.score);
    const newGrade = gradeToBytes2(apiScore.grade).toLowerCase();

    // Always include if no on-chain score yet (auto-promotion)
    if (!onChain) {
      logger.info("New token — auto-promoting to on-chain", {
        id: apiScore.id,
        token: tokenAddress,
        score: newScore,
        grade: apiScore.grade,
      });
      updates.push({
        token: tokenAddress,
        score: newScore,
        grade: newGrade,
        timestamp: cycleTimestamp,
        version,
      });
      continue;
    }

    // Check score delta threshold
    const scoreDelta = Math.abs(newScore - onChain.score);
    const gradeChanged = newGrade !== onChain.grade;

    // 24-hour heartbeat: republish if on-chain timestamp is older than 24h
    // regardless of score delta. Proves liveness to integrators.
    const onChainAge = cycleTimestamp - onChain.timestamp;
    const heartbeatDue = onChainAge >= HEARTBEAT_MAX_AGE_SECONDS;

    if (scoreDelta < threshold && !gradeChanged && !heartbeatDue) {
      logger.debug("Score unchanged — skipping", {
        id: apiScore.id,
        token: tokenAddress,
        onChainScore: onChain.score,
        newScore,
        delta: scoreDelta,
        ageHours: Math.round(onChainAge / 3600),
      });
      continue;
    }

    const reason = heartbeatDue && scoreDelta < threshold ? "heartbeat" : "delta";
    logger.info(`Score update — ${reason}`, {
      id: apiScore.id,
      token: tokenAddress,
      oldScore: onChain.score,
      newScore,
      delta: scoreDelta,
      gradeChanged,
      ageHours: Math.round(onChainAge / 3600),
    });

    updates.push({
      token: tokenAddress,
      score: newScore,
      grade: newGrade,
      timestamp: cycleTimestamp,
      version,
    });
  }

  return updates;
}
