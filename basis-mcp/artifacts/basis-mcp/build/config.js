export const BASE_URL = "https://basisprotocol.xyz";
export const GRADE_ORDER = {
    "A+": 11,
    "A": 10,
    "A-": 9,
    "B+": 8,
    "B": 7,
    "B-": 6,
    "C+": 5,
    "C": 4,
    "C-": 3,
    "D": 2,
    "F": 1,
};
export const API_TIMEOUT_MS = 10_000;
export function gradeRank(grade) {
    if (!grade)
        return 0;
    return GRADE_ORDER[grade] ?? 0;
}
export function isGradeAtLeast(grade, minGrade) {
    return gradeRank(grade) >= gradeRank(minGrade);
}
//# sourceMappingURL=config.js.map