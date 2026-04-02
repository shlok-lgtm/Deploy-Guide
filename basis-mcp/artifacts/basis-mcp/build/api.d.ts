import { type ScoresResponse, type StablecoinDetail, type WalletProfile, type RiskiestWalletsResponse, type BacklogResponse, type MethodologyResponse } from "./config.js";
export declare function fetchScores(): Promise<ScoresResponse>;
export declare function fetchScoreDetail(coin: string): Promise<StablecoinDetail & {
    __status?: number;
}>;
export declare function fetchWalletProfile(address: string): Promise<WalletProfile & {
    __status?: number;
}>;
export declare function fetchRiskiestWallets(limit: number): Promise<RiskiestWalletsResponse>;
export declare function fetchBacklog(limit: number): Promise<BacklogResponse>;
export declare function fetchMethodology(): Promise<MethodologyResponse>;
//# sourceMappingURL=api.d.ts.map