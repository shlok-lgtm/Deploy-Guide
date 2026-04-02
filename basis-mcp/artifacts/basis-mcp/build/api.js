import { BASE_URL, API_TIMEOUT_MS, } from "./config.js";
async function apiFetch(path) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), API_TIMEOUT_MS);
    try {
        const res = await fetch(`${BASE_URL}${path}`, {
            signal: controller.signal,
            headers: { Accept: "application/json" },
        });
        if (res.status === 404) {
            return { __status: 404 };
        }
        if (!res.ok) {
            throw new Error(`HTTP ${res.status}: ${res.statusText}`);
        }
        return (await res.json());
    }
    catch (err) {
        if (err.name === "AbortError") {
            throw new Error("Request timed out after 10s");
        }
        throw err;
    }
    finally {
        clearTimeout(timer);
    }
}
export async function fetchScores() {
    return apiFetch("/api/scores");
}
export async function fetchScoreDetail(coin) {
    return apiFetch(`/api/scores/${encodeURIComponent(coin.toLowerCase())}`);
}
export async function fetchWalletProfile(address) {
    return apiFetch(`/api/wallets/${encodeURIComponent(address)}`);
}
export async function fetchRiskiestWallets(limit) {
    return apiFetch(`/api/wallets/riskiest?limit=${limit}`);
}
export async function fetchBacklog(limit) {
    return apiFetch(`/api/backlog?limit=${limit}`);
}
export async function fetchMethodology() {
    return apiFetch("/api/methodology");
}
// PSI (Protocol Solvency Index) API functions
export async function fetchPsiScores() {
    return apiFetch("/api/psi/scores");
}
export async function fetchPsiDetail(slug) {
    return apiFetch(`/api/psi/scores/${encodeURIComponent(slug)}`);
}
export async function fetchCqi(asset, protocol) {
    return apiFetch(`/api/compose/cqi?asset=${encodeURIComponent(asset)}&protocol=${encodeURIComponent(protocol)}`);
}
export async function fetchProtocolExposure(slug) {
    return apiFetch(`/api/protocols/${encodeURIComponent(slug)}/full-exposure`);
}
export async function fetchDriftExploitAnalysis() {
    return apiFetch("/api/analysis/drift-exploit");
}
//# sourceMappingURL=api.js.map