"""
Sanity checks for LLM-computed financial indicators.

Catches common agent errors:
  - Wrong sign (negative P/E, negative CurrentRatio)
  - Extreme values (P/E > 150, CurrentRatio > 50)
  - Zero-value inconsistencies (EPS=0 but P/E != 0)
  - Cross-indicator inconsistencies when price is available
    (e.g. BVPS doesn't match Price / P_B)

Usage:
    issues = find_sanity_issues(indicators=current, price=237.68)
    to_fix = indicators_to_recompute(issues)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional


# If predicted BVPS differs from implied BVPS (= Price / P_B) by more than
# this fraction, flag it for recomputation.
CROSS_VALIDATION_TOLERANCE = 0.15  # 15%


@dataclass(frozen=True)
class SanityIssue:
    """A single sanity-check failure for one indicator."""

    indicator: str
    message: str


def find_sanity_issues(
    indicators: Dict[str, float],
    price: Optional[float] = None,
) -> List[SanityIssue]:
    """
    Detect suspicious indicator values that typically signal concept-selection
    or arithmetic errors from the LLM analyst.

    Args:
        indicators: Mapping of indicator name -> numeric value.
        price:      Current stock price in USD.  When provided, enables
                    cross-validation of per-share ratios (BVPS vs P_B,
                    EPS vs P_E).

    Returns:
        List of SanityIssue objects, one per problem detected.
    """
    issues: List[SanityIssue] = []

    def v(name: str) -> float:
        """Safely retrieve an indicator value, defaulting to 0.0."""
        try:
            return float(indicators.get(name, 0.0) or 0.0)
        except Exception:
            return 0.0

    pe = v("P_E")
    pb = v("P_B")
    eps = v("EPS")
    bvps = v("BVPS")

    # ── 1. Valuation ratio bounds ────────────────────────────────────────
    if pe < 0:
        issues.append(SanityIssue(
            "P_E", "P/E is negative. Verify EPS sign and price inputs."))
    if pe > 150:
        issues.append(SanityIssue(
            "P_E", "P/E is extremely high (>150). Verify EPS and TTM profit."))

    if pb < 0:
        issues.append(SanityIssue(
            "P_B", "P/B is negative. Verify BVPS and equity inputs."))
    if pb > 80:
        issues.append(SanityIssue(
            "P_B", "P/B is extremely high (>80). Verify equity and share count."))

    # ── 2. Zero-value consistency ────────────────────────────────────────
    if eps == 0 and pe != 0:
        issues.append(SanityIssue(
            "EPS",
            "EPS is zero but P/E is non-zero. "
            "Verify earnings and share count."))

    if bvps == 0 and pb != 0:
        issues.append(SanityIssue(
            "BVPS",
            "BVPS is zero but P/B is non-zero. "
            "Verify equity and share count."))

    # ── 3. Cross-validation against stock price ──────────────────────────
    # These checks verify that per-share metrics are internally consistent
    # with valuation ratios:  P_B = Price / BVPS  and  P_E = Price / EPS.
    if price is not None and price > 0:
        tol = CROSS_VALIDATION_TOLERANCE

        # BVPS vs P_B:  by definition P_B = Price / BVPS
        if pb > 0 and bvps > 0:
            implied_bvps = price / pb
            diff = abs(implied_bvps - bvps) / max(abs(implied_bvps), 1e-9)
            if diff > tol:
                issues.append(SanityIssue(
                    "BVPS",
                    f"BVPS={bvps:.2f} inconsistent with Price/P_B="
                    f"{implied_bvps:.2f} (diff={diff:.0%}). "
                    "Recheck equity and share count."))

        # EPS vs P_E:  by definition P_E = Price / EPS
        if pe > 0 and eps > 0:
            implied_eps = price / pe
            diff = abs(implied_eps - eps) / max(abs(implied_eps), 1e-9)
            if diff > tol:
                issues.append(SanityIssue(
                    "EPS",
                    f"EPS={eps:.2f} inconsistent with Price/P_E="
                    f"{implied_eps:.2f} (diff={diff:.0%}). "
                    "Recheck net income and share count."))

    # ── 4. Percentage metrics must be in [-200, 200] ─────────────────────
    # These are returned as percent numbers (e.g. 12.34 means 12.34%).
    percent_metrics = [
        "GrossMargin", "EBITMargin", "NetMargin",
        "EBIT_Assets", "ROE", "ROIC",
    ]
    for m in percent_metrics:
        x = v(m)
        if x < -200 or x > 200:
            issues.append(SanityIssue(
                m,
                f"{m}={x:.2f} is outside [-200, 200]%. "
                "Verify numerator and denominator."))

    # ── 5. Liquidity bounds ──────────────────────────────────────────────
    cr = v("CurrentRatio")
    if cr < 0:
        issues.append(SanityIssue(
            "CurrentRatio",
            "CurrentRatio is negative. "
            "Verify current assets and liabilities."))
    if cr > 50:
        issues.append(SanityIssue(
            "CurrentRatio",
            "CurrentRatio is unusually high (>50). "
            "Verify current liabilities."))

    return issues


def indicators_to_recompute(issues: List[SanityIssue]) -> List[str]:
    """
    Extract the unique indicator names that should be sent back to the
    analyst agent for recomputation.
    """
    return sorted({i.indicator for i in issues})
