import { config as dotenvConfig } from "dotenv";
dotenvConfig();

export interface KeeperConfig {
  apiUrl: string;
  apiScoresEndpoint: string;
  chains: {
    base: {
      rpcUrl: string;
      oracleAddress: string;
      chainId: 8453;
    };
    arbitrum: {
      rpcUrl: string;
      oracleAddress: string;
      chainId: 42161;
    };
  };
  pollIntervalSeconds: number;
  scoreChangeThreshold: number;
  keeperPrivateKey: string;
  maxRetries: number;
  baseRetryDelayMs: number;
  maxRetryDelayMs: number;
  webhookUrl?: string;
  alertOnStaleness: boolean;
  maxGasPriceGwei: number;
  gasLimitPerUpdate: number;
  dryRun: boolean;
  sbtAddress?: string;
}

function requireEnv(key: string): string {
  const val = process.env[key];
  if (!val) throw new Error(`Missing required env var: ${key}`);
  return val;
}

export function loadConfig(): KeeperConfig {
  return {
    apiUrl: process.env["BASIS_API_URL"] ?? "https://basisprotocol.xyz",
    apiScoresEndpoint: "/api/scores",

    chains: {
      base: {
        rpcUrl: requireEnv("BASE_RPC_URL"),
        oracleAddress: requireEnv("BASE_ORACLE_ADDRESS"),
        chainId: 8453,
      },
      arbitrum: {
        rpcUrl: requireEnv("ARBITRUM_RPC_URL"),
        oracleAddress: requireEnv("ARBITRUM_ORACLE_ADDRESS"),
        chainId: 42161,
      },
    },

    pollIntervalSeconds: Number(process.env["POLL_INTERVAL_SECONDS"] ?? "3600"),
    scoreChangeThreshold: Number(process.env["SCORE_CHANGE_THRESHOLD"] ?? "10"),

    keeperPrivateKey: requireEnv("KEEPER_PRIVATE_KEY"),

    maxRetries: Number(process.env["MAX_RETRIES"] ?? "3"),
    baseRetryDelayMs: Number(process.env["BASE_RETRY_DELAY_MS"] ?? "1000"),
    maxRetryDelayMs: Number(process.env["MAX_RETRY_DELAY_MS"] ?? "30000"),

    webhookUrl: process.env["SLACK_WEBHOOK_URL"] ?? process.env["DISCORD_WEBHOOK_URL"],
    alertOnStaleness: process.env["ALERT_ON_STALENESS"] !== "false",

    maxGasPriceGwei: Number(process.env["MAX_GAS_PRICE_GWEI"] ?? "1.0"),
    gasLimitPerUpdate: Number(process.env["GAS_LIMIT_PER_UPDATE"] ?? "150000"),

    dryRun: process.env["DRY_RUN"] === "true",

    sbtAddress: process.env["BASE_SBT_ADDRESS"],
  };
}
