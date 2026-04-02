export declare const BASE_URL = "https://basisprotocol.xyz";
export declare const GRADE_ORDER: Record<string, number>;
export declare const API_TIMEOUT_MS = 10000;
export type GradeString = "A+" | "A" | "A-" | "B+" | "B" | "B-" | "C+" | "C" | "C-" | "D" | "F";
export type SortBy = "score_desc" | "score_asc" | "name";
export interface StablecoinScore {
    symbol: string;
    name: string;
    issuer?: string;
    score: number;
    grade: string;
    price?: number;
    market_cap?: number;
    daily_change?: number;
    categories?: {
        peg?: number;
        liquidity?: number;
        flows?: number;
        distribution?: number;
        structural?: number;
    };
}
export interface ScoresResponse {
    stablecoins?: StablecoinScore[];
    scores?: StablecoinScore[];
    count?: number;
    formula_version?: string;
    timestamp?: string;
    methodology_summary?: string;
}
export interface CategoryDetail {
    score: number;
    weight: number;
}
export interface StablecoinDetail {
    symbol: string;
    name: string;
    issuer?: string;
    score: number;
    grade: string;
    price?: number;
    market_cap?: number;
    categories?: {
        peg?: number | CategoryDetail;
        liquidity?: number | CategoryDetail;
        flows?: number | CategoryDetail;
        distribution?: number | CategoryDetail;
        structural?: number | CategoryDetail;
    };
    structural_breakdown?: Record<string, {
        score: number;
        weight: number;
    }>;
    weakest_category?: string;
    strongest_category?: string;
    component_count?: number;
    formula_version?: string;
    daily_change?: number;
    weekly_change?: number;
    computed_at?: string;
    is_scored?: boolean;
    error?: boolean;
    message?: string;
}
export interface WalletHolding {
    symbol: string;
    token_address?: string;
    balance?: number;
    value_usd?: number;
    pct_of_wallet?: number;
    is_scored: boolean;
    sii_score?: number | null;
    sii_grade?: string | null;
    risk_contribution?: string;
}
export interface WalletProfile {
    address: string;
    risk_score?: number;
    risk_grade?: string;
    total_stablecoin_value?: number;
    size_tier?: string;
    label?: string;
    is_contract?: boolean;
    concentration?: {
        hhi?: number;
        grade?: string;
        dominant_asset?: string;
        dominant_asset_pct?: number;
        interpretation?: string;
    };
    coverage?: {
        quality?: string;
        scored_holdings?: number;
        unscored_holdings?: number;
        unscored_pct?: number;
        interpretation?: string;
    };
    holdings?: WalletHolding[];
    formula_version?: string;
    last_indexed_at?: string;
    found_in_index?: boolean;
    error?: boolean;
    message?: string;
}
export interface RiskiestWallet {
    address: string;
    risk_score: number;
    risk_grade: string;
    total_stablecoin_value: number;
    size_tier?: string;
    dominant_asset?: string;
    dominant_asset_pct?: number;
    coverage_quality?: string;
    unscored_pct?: number;
    capital_at_risk_interpretation?: string;
}
export interface RiskiestWalletsResponse {
    wallets?: RiskiestWallet[];
    count?: number;
    total_at_risk_capital?: number;
    timestamp?: string;
}
export interface BacklogItem {
    symbol: string;
    name?: string;
    token_address?: string;
    total_value_held?: number;
    wallets_holding?: number;
    avg_holding_value?: number;
    max_single_holding?: number;
    scoring_status?: string;
    scoring_priority?: number;
    coverage_gap_interpretation?: string;
}
export interface BacklogResponse {
    backlog?: BacklogItem[];
    count?: number;
    total_unscored_value?: number;
    timestamp?: string;
}
export interface MethodologyResponse {
    version?: string;
    formula?: string;
    structural_formula?: string;
    weights?: {
        top_level?: Record<string, number>;
        structural?: Record<string, number>;
    };
    grade_scale?: Record<string, string>;
    total_components?: number;
    automated_components?: number;
    data_sources?: string[];
    wallet_scoring?: {
        version?: string;
        method?: string;
        concentration?: string;
    };
    verification?: {
        methodology_is_public?: boolean;
        scores_are_deterministic?: boolean;
        no_customer_specific_adjustments?: boolean;
        methodology_locked_since?: string;
    };
}
export type OverallAssessment = "LOW_RISK" | "MEDIUM_RISK" | "HIGH_RISK" | "UNKNOWN";
export declare function gradeRank(grade: string | undefined | null): number;
export declare function isGradeAtLeast(grade: string | undefined | null, minGrade: string): boolean;
//# sourceMappingURL=config.d.ts.map