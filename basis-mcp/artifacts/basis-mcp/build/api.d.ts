import { type ScoresResponse, type StablecoinDetail, type WalletProfile, type RiskiestWalletsResponse, type BacklogResponse, type MethodologyResponse, type PsiScoresResponse, type PsiDetailResponse, type CqiResponse, type FullExposureResponse, type DriftExploitAnalysis } from "./config.js";
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
export declare function fetchPsiScores(): Promise<PsiScoresResponse>;
export declare function fetchPsiDetail(slug: string): Promise<PsiDetailResponse>;
export declare function fetchCqi(asset: string, protocol: string): Promise<CqiResponse>;
export declare function fetchProtocolExposure(slug: string): Promise<FullExposureResponse>;
export declare function fetchDriftExploitAnalysis(): Promise<DriftExploitAnalysis>;
//# sourceMappingURL=api.d.ts.map