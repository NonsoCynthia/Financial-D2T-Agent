from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple


@dataclass(frozen=True)
class SanityIssue:
    indicator: str
    message: str


def find_sanity_issues(indicators: Dict[str, float]) -> List[SanityIssue]:
    """
    Detect suspicious indicator values that typically indicate concept selection or arithmetic errors.
    """
    issues: List[SanityIssue] = []

    def v(name: str) -> float:
        try:
            return float(indicators.get(name, 0.0) or 0.0)
        except Exception:
            return 0.0

    pe = v(name="P_E")
    pb = v(name="P_B")
    eps = v(name="EPS")
    bvps = v(name="BVPS")

    # Valuation sanity
    if pe < 0:
        issues.append(SanityIssue("P_E", "P/E is negative. Verify EPS sign and price inputs."))
    if pe > 150:
        issues.append(SanityIssue("P_E", "P/E is extremely high. Verify EPS and whether TTM profit is correct."))

    if pb < 0:
        issues.append(SanityIssue("P_B", "P/B is negative. Verify BVPS and equity inputs."))
    if pb > 80:
        issues.append(SanityIssue("P_B", "P/B is extremely high. Verify equity and share count inputs."))

    if eps == 0 and pe != 0:
        issues.append(SanityIssue("EPS", "EPS is zero but P/E is non-zero. Verify earnings and share count."))

    if bvps == 0 and pb != 0:
        issues.append(SanityIssue("BVPS", "BVPS is zero but P/B is non-zero. Verify equity and share count."))

    # Percentage sanity. These are returned as percent numbers.
    percent_metrics = ["GrossMargin", "EBITMargin", "NetMargin", "EBIT_Assets", "ROE", "ROIC"]
    for m in percent_metrics:
        x = v(name=m)
        if x < -200 or x > 200:
            issues.append(SanityIssue(m, f"{m} is outside [-200, 200] percent. Verify numerator and denominator."))

    # Liquidity sanity
    cr = v(name="CurrentRatio")
    if cr < 0:
        issues.append(SanityIssue("CurrentRatio", "CurrentRatio is negative. Verify current assets and liabilities."))
    if cr > 50:
        issues.append(SanityIssue("CurrentRatio", "CurrentRatio is unusually high. Verify current liabilities."))

    return issues


def indicators_to_recompute(issues: List[SanityIssue]) -> List[str]:
    """
    Return a unique list of indicator names that should be recomputed when sanity issues are detected.
    """
    return sorted({i.indicator for i in issues})
