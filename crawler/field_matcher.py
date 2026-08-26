"""Contract/position field signature matching — pure code, no AI.

Given a list of column names, determine which contract/position field
categories the table appears to contain. Used by Stage 2 to pre-rank
candidates and provide concrete evidence to the LLM when scoring
market_risk_score.

Match rule: case-insensitive substring of column name against any token
in a category's pattern list. First column to match a category wins;
each category contributes at most once to the count.
"""
from __future__ import annotations

from .types import ColumnInfo


# Core fields — presence of many strongly suggests a position/contract table.
CORE_FIELD_PATTERNS: dict[str, list[str]] = {
    "deal_number":       ["DEAL", "CONTRACT", "CTRNUM", "CTRACT", "CONTRACTNO", "CTR"],
    "schedule_number":   ["SCHED", "DELSCHED", "DELIVERYNO", "BOLNUM", "BOLNO", "LADING", "TICKET", "LIFTING"],
    "counterparty_id":   ["VENDORID", "CUSTOMERID", "SUPPLIERID", "CPTYID", "VENDID", "CUSTID"],
    "counterparty_name": ["VENDORNAME", "CUSTOMERNAME", "SUPPLIERNAME", "CPTYNAME"],
    "quantity":          ["QTY", "QUANTITY", "BUSHEL", "WEIGHT", "VOLUME", "NETWT", "GALLON", "BARREL", "BBL"],
    "commodity":         ["COMMODITY", "PRODUCT", "ITEMID", "ITEMCODE", "GRADE", "CROP", "FUEL", "MATERIAL"],
    "location":          ["LOCATION", "FACILITY", "ELEVATOR", "WAREHOUSE", "SITE", "BIN", "TERMINAL", "RACK", "TANK", "PLANT", "DEPOT"],
    "position_month":    ["POSITIONMONTH", "DELIVERYMONTH", "DELMONTH", "POSMONTH", "PERIOD"],
    "status":            ["STATUS", "STATE"],
    "execution_qty":     ["DELIVERED", "FULFILLED", "REMAINING", "OPENBAL", "OPENQTY", "DELQTY"],
}

# Secondary fields — refine the score (price exposure, hedging metadata).
SECONDARY_FIELD_PATTERNS: dict[str, list[str]] = {
    "contract_price":    ["CONTRACTPRICE", "CTRPRICE", "DEALPRICE"],
    "market_price":      ["MARKETPRICE", "MKTPRICE", "CASHPRICE", "SPOTPRICE", "RACKPRICE", "INDEXPRICE"],
    "futures_price":     ["FUTURESPRICE", "FUTPRICE"],
    "basis_price":       ["BASIS", "BASISPRICE"],
    "other_price":       ["FREIGHT", "DISCOUNT", "PREMIUM", "ADJUSTMENT", "TARIFF", "EXCISE", "SURCHARGE"],
    "futures_month":     ["FUTURESMONTH", "FUTMONTH", "CONTRACTMONTH"],
}


def _match_category(col_name_upper: str, tokens: list[str]) -> bool:
    return any(tok in col_name_upper for tok in tokens)


def _find_first_match(columns: list[ColumnInfo], tokens: list[str]) -> str | None:
    for col in columns:
        if _match_category(col.name.upper(), tokens):
            return col.name
    return None


def compute_contract_signature(columns: list[ColumnInfo]) -> dict:
    """Compute the contract/position field signature for a table.

    Returns:
        {
          "core_matches":      {category: column_name, ...},
          "secondary_matches": {category: column_name, ...},
          "core_count":        int,
          "secondary_count":   int,
        }
    """
    core_matches: dict[str, str] = {}
    for category, tokens in CORE_FIELD_PATTERNS.items():
        match = _find_first_match(columns, tokens)
        if match:
            core_matches[category] = match

    secondary_matches: dict[str, str] = {}
    for category, tokens in SECONDARY_FIELD_PATTERNS.items():
        match = _find_first_match(columns, tokens)
        if match:
            secondary_matches[category] = match

    return {
        "core_matches": core_matches,
        "secondary_matches": secondary_matches,
        "core_count": len(core_matches),
        "secondary_count": len(secondary_matches),
    }


def combined_pre_ai_score(heuristic_score: int, signature: dict) -> float:
    """Compute the pre-AI ranking score used to pick which tables go to Haiku.

    combined = heuristic_score + 1.0 * core_count + 0.5 * secondary_count
    """
    core = signature.get("core_count", 0)
    secondary = signature.get("secondary_count", 0)
    return heuristic_score + 1.0 * core + 0.5 * secondary


def format_signature_for_prompt(signature: dict) -> str:
    """Render the signature as a human-readable block for the LLM prompt.

    Returns empty string if both counts are zero (caller should omit the block).
    """
    core = signature.get("core_matches", {})
    secondary = signature.get("secondary_matches", {})
    if not core and not secondary:
        return ""

    lines = [
        f"Contract/position field signature: {len(core)}/{len(CORE_FIELD_PATTERNS)} core, "
        f"{len(secondary)}/{len(SECONDARY_FIELD_PATTERNS)} secondary"
    ]
    if core:
        lines.append("  Core matches:")
        for category, col in core.items():
            lines.append(f"    {category:<18} -> {col}")
    if secondary:
        lines.append("  Secondary matches:")
        for category, col in secondary.items():
            lines.append(f"    {category:<18} -> {col}")
    return "\n".join(lines)
