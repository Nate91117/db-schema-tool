"""Generate a self-contained HTML report from a stage 3 results.json file.

Renders coverage stats, concept distribution, market_risk distribution, and
an expandable per-table view (description, rationale, contract signature,
column annotations, relationships, typical queries, data quality notes).

Usage:
    from crawler.report_html import generate_report
    generate_report("run3results/results.json", "run3results/RUN3_SUMMARY.html")

CLI:
    dbscan report run3results/results.json
"""
from __future__ import annotations

import json
from collections import Counter
from html import escape
from pathlib import Path


# ── Pill / badge rendering ────────────────────────────────────────────────────

_CONCEPT_PILL = {
    "inventory":    "pill-blue",
    "pricing":      "pill-violet",
    "contracts":    "pill-green",
    "finance":      "pill-amber",
    "production":   "pill-amber",
    "quality":      "pill-violet",
    "counterparty": "pill-muted",
    "setup":        "pill-muted",
    "audit_log":    "pill-muted",
    "noise":        "pill-muted",
    "unknown":      "pill-muted",
}


def _pill_concept(c: str) -> str:
    cls = _CONCEPT_PILL.get(c, "pill-muted")
    return f'<span class="pill {cls}">{escape(str(c))}</span>'


def _pill_score(s: int | None) -> str:
    if s is None:
        return ""
    if s >= 9:
        return f'<span class="pill pill-green">{s}</span>'
    if s >= 8:
        return f'<span class="pill pill-blue">{s}</span>'
    if s >= 7:
        return f'<span class="pill pill-amber">{s}</span>'
    return f'<span class="pill pill-muted">{s}</span>'


def _pill_mr(s: int | None) -> str:
    if s is None or s == 0:
        return '<span class="pill pill-muted">0</span>'
    if s >= 9:
        return f'<span class="pill pill-red">{s}</span>'
    if s >= 7:
        return f'<span class="pill pill-amber">{s}</span>'
    if s >= 4:
        return f'<span class="pill pill-blue">{s}</span>'
    return f'<span class="pill pill-muted">{s}</span>'


# ── CSS (single source of truth for the report look) ─────────────────────────

_CSS = """
:root {
  --bg: #f6f7fb; --card: #fff; --ink: #1a2436; --muted: #64748b; --line: #e5e9f0;
  --accent: #3b82f6; --accent-dark: #1d4ed8; --accent-soft: #dbeafe;
  --green: #10b981; --green-soft: #d1fae5;
  --red: #ef4444; --red-soft: #fee2e2;
  --amber: #f59e0b; --amber-soft: #fef3c7;
  --violet: #8b5cf6; --violet-soft: #ede9fe;
  --navy: #2c3e50;
}
* { box-sizing: border-box; }
html, body { margin: 0; padding: 0; }
body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Inter, system-ui, sans-serif;
       background: var(--bg); color: var(--ink); line-height: 1.55; -webkit-font-smoothing: antialiased; }
.container { max-width: 1180px; margin: 0 auto; padding: 40px 28px 80px; }
.header { background: linear-gradient(135deg, var(--navy) 0%, #6366f1 100%);
          color: #fff; padding: 48px 28px 56px; margin-bottom: -32px; }
.header-inner { max-width: 1180px; margin: 0 auto; }
.eyebrow { font-size: 0.72rem; letter-spacing: 0.14em; text-transform: uppercase;
           opacity: 0.82; font-weight: 600; }
.header h1 { font-family: "DM Serif Display", Georgia, serif; font-weight: 400;
             font-size: 2.4rem; margin: 8px 0 6px; letter-spacing: -0.01em; }
.header p { margin: 0; opacity: 0.88; font-size: 1.02rem; }
h2 { font-family: "DM Serif Display", Georgia, serif; font-weight: 400;
     font-size: 1.55rem; letter-spacing: -0.01em; margin: 44px 0 14px; color: var(--navy); }
h3 { font-size: 0.98rem; margin: 22px 0 10px; color: var(--navy); }
.card { background: var(--card); border: 1px solid var(--line); border-radius: 14px;
        padding: 22px 24px; box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04); }
.grid { display: grid; gap: 16px; }
.grid-4 { grid-template-columns: repeat(4, 1fr); }
@media (max-width: 880px) { .grid-4 { grid-template-columns: 1fr 1fr; } }
@media (max-width: 560px) { .grid-4 { grid-template-columns: 1fr; } }
.stat .label { font-size: 0.72rem; color: var(--muted); text-transform: uppercase;
               letter-spacing: 0.08em; font-weight: 600; }
.stat .value { font-size: 1.9rem; font-weight: 600; margin-top: 6px; color: var(--navy); line-height: 1.1; }
.stat .sub { font-size: 0.82rem; color: var(--muted); margin-top: 4px; }
table.summary { width: 100%; border-collapse: collapse; }
table.summary th, table.summary td { text-align: left; padding: 8px 12px;
                  border-bottom: 1px solid var(--line); font-size: 0.92rem; }
table.summary th { font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.08em;
                   color: var(--muted); font-weight: 600; }
table.summary tr:last-child td { border-bottom: none; }
td.num, th.num { text-align: right; font-variant-numeric: tabular-nums; }
code, .mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
              font-size: 0.88em; background: #f1f5f9; padding: 1px 6px; border-radius: 4px; }
.pill { display: inline-block; padding: 2px 10px; border-radius: 999px;
        font-size: 0.76rem; font-weight: 600; letter-spacing: 0.02em; }
.pill-blue { background: var(--accent-soft); color: var(--accent-dark); }
.pill-green { background: var(--green-soft); color: #047857; }
.pill-red { background: var(--red-soft); color: #b91c1c; }
.pill-amber { background: var(--amber-soft); color: #b45309; }
.pill-violet { background: var(--violet-soft); color: #6d28d9; }
.pill-muted { background: #f1f5f9; color: var(--muted); }
.bar-wrap { display: flex; align-items: center; gap: 10px; }
.bar { flex: 1; height: 8px; background: #eef2f7; border-radius: 999px;
       overflow: hidden; min-width: 120px; }
.bar-fill { height: 100%; background: var(--accent); border-radius: 999px; }
.bar-fill.mr { background: linear-gradient(90deg, #ef4444, #f59e0b); }
.callout-violet { background: var(--violet-soft); border-left: 4px solid var(--violet);
                  padding: 14px 18px; border-radius: 8px; font-size: 0.94rem; }
.two-col { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
@media (max-width: 880px) { .two-col { grid-template-columns: 1fr; } }
.meta-row { display: flex; flex-wrap: wrap; gap: 10px 24px; margin-top: 14px;
            font-size: 0.9rem; color: rgba(255,255,255,0.92); }
footer { margin-top: 48px; color: var(--muted); font-size: 0.85rem; text-align: center; }

details.tbl { background: var(--card); border: 1px solid var(--line); border-radius: 12px;
              padding: 0; margin-bottom: 10px; overflow: hidden; }
details.tbl[open] { box-shadow: 0 4px 12px rgba(15, 23, 42, 0.06); border-color: var(--accent); }
details.tbl > summary {
  list-style: none;
  cursor: pointer;
  padding: 14px 18px;
  display: flex;
  gap: 16px;
  align-items: stretch;
}
details.tbl > summary::-webkit-details-marker { display: none; }
.row-arrow { flex: 0 0 auto; color: var(--muted); font-size: 0.82rem; padding-top: 2px;
             transition: transform 0.15s ease; width: 14px; text-align: center; }
details.tbl[open] .row-arrow { transform: rotate(90deg); }
.row-left { flex: 1 1 auto; min-width: 0; }
.row-right { flex: 0 0 auto; display: flex; gap: 10px; align-items: center;
             white-space: nowrap; padding-top: 2px; }
.tname { font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
         font-size: 0.95rem; color: var(--navy); font-weight: 600; }
.tdesc { color: #334155; font-size: 0.86rem; margin-top: 4px; line-height: 1.4; }
.tmeta { display: flex; flex-wrap: wrap; gap: 6px 14px; margin-top: 8px;
         font-size: 0.74rem; color: var(--muted); }
.tmeta span strong { color: var(--ink); font-weight: 600; }
.tmeta .badge { background: #f1f5f9; padding: 1px 8px; border-radius: 4px;
                font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 0.72rem; }
.row-right .pill { font-size: 0.78rem; }
.row-right .rcol { display: flex; flex-direction: column; align-items: flex-end; gap: 2px; }
.row-right .rcol .lbl { font-size: 0.62rem; text-transform: uppercase;
                        letter-spacing: 0.08em; color: var(--muted); font-weight: 600; }

details.tbl .body { padding: 4px 18px 22px; border-top: 1px solid var(--line);
                    background: #fafbfc; }
details.tbl .body h4 { font-size: 0.78rem; font-weight: 700;
                       text-transform: uppercase; letter-spacing: 0.08em;
                       color: var(--muted); margin: 18px 0 6px; }
details.tbl .body p { margin: 0; font-size: 0.92rem; }
details.tbl .body .rationale { background: var(--violet-soft); border-left: 3px solid var(--violet);
                               padding: 10px 14px; border-radius: 6px; }
details.tbl .body .rationale strong { color: #6d28d9; }
table.cols { width: 100%; border-collapse: collapse; margin-top: 6px; font-size: 0.86rem;
             background: #fff; border: 1px solid var(--line); border-radius: 8px; overflow: hidden; }
table.cols th, table.cols td { padding: 6px 10px; border-bottom: 1px solid var(--line);
                                 vertical-align: top; text-align: left; }
table.cols th { font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.08em;
                color: var(--muted); font-weight: 600; background: #f8fafc; }
table.cols code { font-size: 0.82rem; }
table.cols .meaning { color: var(--muted); font-size: 0.82rem; margin-top: 2px; }
.relationship-row { font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
                    font-size: 0.85rem; padding: 4px 0; border-bottom: 1px dashed var(--line); }
.relationship-row:last-child { border-bottom: none; }
ul.q { padding-left: 18px; margin: 4px 0; }
ul.q li { margin: 4px 0; font-size: 0.9rem; }
.section-controls { display: flex; gap: 12px; margin-bottom: 12px; }
.btn { background: #fff; border: 1px solid var(--line); border-radius: 6px;
       padding: 6px 12px; font-size: 0.85rem; color: var(--ink); cursor: pointer; }
.btn:hover { background: var(--accent-soft); color: var(--accent-dark); border-color: var(--accent); }
@media (max-width: 720px) {
  details.tbl > summary { flex-direction: column; }
  .row-right { flex-wrap: wrap; }
}
"""


# ── Per-table rendering ──────────────────────────────────────────────────────

def _render_table_details(t: dict) -> str:
    description = escape(t.get("description") or "(no description)")
    rationale = escape(t.get("market_risk_rationale") or "(no rationale)")
    columns = t.get("columns") or []
    relationships = t.get("relationships") or []
    typical_queries = t.get("typical_queries") or []
    dq_notes = t.get("data_quality_notes") or ""
    sig = t.get("contract_signature") or {}

    col_rows = []
    for c in columns:
        name = escape(c.get("name", ""))
        dtype = escape(c.get("data_type", ""))
        desc = escape(c.get("description", "") or "")
        meaning = escape(c.get("business_meaning", "") or "")
        is_pk = c.get("is_primary_key", False)
        notes = escape(c.get("notes", "") or "")
        pk_badge = ' <span class="pill pill-green" style="font-size:0.66rem">PK</span>' if is_pk else ""
        notes_block = f'<div class="meaning"><em>note:</em> {notes}</div>' if notes else ""
        col_rows.append(
            f'<tr><td><code>{name}</code>{pk_badge}<br><span class="meaning">{dtype}</span></td>'
            f'<td>{desc}<div class="meaning">{meaning}</div>{notes_block}</td></tr>'
        )
    cols_html = (
        '<table class="cols"><thead><tr><th style="width:30%">Column</th>'
        '<th>Description / Business meaning</th></tr></thead><tbody>'
        + "".join(col_rows) + "</tbody></table>"
        if col_rows else '<p style="color:var(--muted)">no columns annotated</p>'
    )

    rel_rows = []
    for r in relationships:
        rel_rows.append(
            f'<div class="relationship-row">{escape(r.get("from_column",""))} → '
            f'{escape(r.get("to_table",""))}.{escape(r.get("to_column",""))} '
            f'<span class="pill pill-muted" style="font-size:0.65rem">{escape(r.get("relationship_type",""))}</span>'
            f'<div class="meaning">{escape(r.get("description","") or "")}</div></div>'
        )
    rel_html = "".join(rel_rows) or '<p style="color:var(--muted);font-size:0.88rem">none documented</p>'

    queries_html = "".join(f"<li>{escape(q)}</li>" for q in typical_queries) \
                   or '<li style="color:var(--muted)">none</li>'

    sig_block = ""
    if sig:
        core = sig.get("core_matches") or {}
        sec = sig.get("secondary_matches") or {}
        if core or sec:
            core_str = ", ".join(f'{k} → <code>{escape(v)}</code>' for k, v in core.items())
            sec_str = ", ".join(f'{k} → <code>{escape(v)}</code>' for k, v in sec.items())
            sig_block = (
                "<h4>Contract field signature</h4>"
                f"<p><strong>{sig.get('core_count', 0)}/10 core, "
                f"{sig.get('secondary_count', 0)}/6 secondary</strong></p>"
                + (f'<p style="font-size:0.85rem;color:var(--muted)">Core: {core_str}</p>' if core_str else "")
                + (f'<p style="font-size:0.85rem;color:var(--muted)">Secondary: {sec_str}</p>' if sec_str else "")
            )

    dq_block = f"<h4>Data quality notes</h4><p>{escape(dq_notes)}</p>" if dq_notes else ""

    return (
        '<div class="body">'
        f"<h4>Description</h4><p>{description}</p>"
        f'<h4>Market risk rationale</h4><div class="rationale">'
        f"<p><strong>Score {t.get('market_risk_score', 0)}/10.</strong> {rationale}</p></div>"
        f"{sig_block}"
        f"<h4>Columns ({len(columns)})</h4>{cols_html}"
        f"<h4>Relationships</h4>{rel_html}"
        f'<h4>Typical queries</h4><ul class="q">{queries_html}</ul>'
        f"{dq_block}"
        "</div>"
    )


def _render_table_card(t: dict) -> str:
    name = escape(t["name"])
    description = (t.get("description") or "").replace("\n", " ")
    desc_preview = escape(description[:220] + ("…" if len(description) > 220 else ""))

    sig = t.get("contract_signature") or {}
    core = sig.get("core_count", 0)
    sec = sig.get("secondary_count", 0)
    cols_count = len(t.get("columns") or [])
    rel_count = len(t.get("relationships") or [])

    return (
        '<details class="tbl">'
        '<summary>'
        '<div class="row-arrow">▸</div>'
        '<div class="row-left">'
        f'<div class="tname">{name}</div>'
        f'<div class="tdesc">{desc_preview}</div>'
        '<div class="tmeta">'
        f'<span class="badge">{cols_count} cols</span>'
        f'<span class="badge">{rel_count} rels</span>'
        f'<span class="badge">sig {core}c / {sec}s</span>'
        f'<span><strong>{t.get("row_count", 0):,}</strong> rows</span>'
        '</div>'
        '</div>'
        '<div class="row-right">'
        f'<div class="rcol"><span class="lbl">MR</span>{_pill_mr(t.get("market_risk_score"))}</div>'
        f'<div class="rcol"><span class="lbl">Score</span>{_pill_score(t.get("score"))}</div>'
        f'<div class="rcol"><span class="lbl">Concept</span>{_pill_concept(t.get("business_concept"))}</div>'
        '</div>'
        '</summary>'
        f"{_render_table_details(t)}"
        '</details>'
    )


# ── Public API ────────────────────────────────────────────────────────────────

def generate_report(results_path: str, output_path: str | None = None,
                    title: str | None = None) -> str:
    """Render an HTML report from a stage 3 results.json.

    Args:
        results_path: path to a stage 3 results.json file
        output_path:  where to write the HTML (default: results_path with .html suffix)
        title:        custom title for the header (default: derived from folder name)

    Returns:
        Absolute path to the written HTML file.
    """
    src = Path(results_path)
    with src.open() as f:
        data = json.load(f)

    out = Path(output_path) if output_path else src.with_suffix(".html")
    if title is None:
        # Derive from parent folder name (e.g., "run3results" -> "Run 3")
        parent = src.parent.name or "Results"
        title = parent.replace("results", "").replace("_", " ").strip().title() or parent
        if title and title[-1].isdigit():
            title = f"Run {title.split()[-1]}"

    tables = data.get("tables", [])
    meta = data.get("meta", {}) or {}
    sl = data.get("semantic_layer", {}) or {}

    concepts = Counter(t.get("business_concept") for t in tables)
    mr_dist = Counter(t.get("market_risk_score", 0) for t in tables)
    total_rows = sum(t.get("row_count") or 0 for t in tables)
    dur_min = (meta.get("duration_ms", 0) or 0) / 60000

    avg_mr = (sum(t.get("market_risk_score", 0) for t in tables) / len(tables)) if tables else 0
    mr_ge7 = sum(1 for t in tables if t.get("market_risk_score", 0) >= 7)

    stats_html = f"""
    <div class="grid grid-4">
      <div class="card stat"><div class="label">Annotated tables</div>
        <div class="value">{len(tables)}</div>
        <div class="sub">top by stage 2 score</div></div>
      <div class="card stat"><div class="label">Rows covered</div>
        <div class="value">{total_rows / 1_000_000:.2f}M</div>
        <div class="sub">{total_rows:,} total</div></div>
      <div class="card stat"><div class="label">Avg market_risk</div>
        <div class="value">{avg_mr:.1f}</div>
        <div class="sub">{mr_ge7} tables score ≥7</div></div>
      <div class="card stat"><div class="label">Tokens (Sonnet)</div>
        <div class="value">{meta.get('tokens_used', 0) / 1000:.0f}K</div>
        <div class="sub">{dur_min:.0f} min</div></div>
    </div>"""

    concept_rows = []
    total_conc = sum(concepts.values()) or 1
    for c, n in concepts.most_common():
        pct = n / total_conc * 100
        concept_rows.append(
            f'<tr><td>{_pill_concept(c)}</td><td class="num">{n}</td>'
            f'<td><div class="bar-wrap"><div class="bar"><div class="bar-fill" '
            f'style="width:{pct:.1f}%"></div></div></div></td></tr>'
        )

    mr_rows = []
    total_mr = sum(mr_dist.values()) or 1
    for s in sorted(mr_dist, reverse=True):
        pct = mr_dist[s] / total_mr * 100
        mr_rows.append(
            f'<tr><td>{_pill_mr(s)}</td><td class="num">{mr_dist[s]}</td>'
            f'<td><div class="bar-wrap"><div class="bar"><div class="bar-fill mr" '
            f'style="width:{pct:.1f}%"></div></div></div></td></tr>'
        )

    sorted_tables = sorted(
        tables,
        key=lambda t: (
            -(t.get("market_risk_score") or 0),
            -(t.get("score") or 0),
            -(t.get("row_count") or 0),
        ),
    )
    cards_html = "".join(_render_table_card(t) for t in sorted_tables)

    script = (
        "<script>function toggleAll(open){"
        "document.querySelectorAll('details.tbl').forEach(d=>{d.open=open;});}</script>"
    )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{escape(title)} — DB Schema Crawl Report</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=DM+Serif+Display&family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>{_CSS}</style>
{script}
</head>
<body>
  <header class="header">
    <div class="header-inner">
      <div class="eyebrow">DB Schema Tool · Crawl Report</div>
      <h1>{escape(title)} — Hybrid Categorization + Market Risk Overlay</h1>
      <p>Every table gets a primary business concept (general lens, 9 categories) AND an independent 0-10 market_risk_score driven by a contract-field signature.</p>
      <div class="meta-row">
        <span><strong>Model:</strong> <code style="background:rgba(255,255,255,0.15);color:#fff">{escape(meta.get('model', '—'))}</code></span>
        <span><strong>Semantic layer:</strong> v{escape(str(sl.get('version', '1.3')))}</span>
        <span><strong>Tokens (S3):</strong> {meta.get('tokens_used', 0):,}</span>
        <span><strong>Duration (S3):</strong> {dur_min:.0f} min</span>
        <span><strong>Timestamp:</strong> {escape(str(meta.get('timestamp', '')))}</span>
      </div>
    </div>
  </header>
  <div class="container">
    <h2>Coverage</h2>
    {stats_html}

    <div class="callout-violet" style="margin-top:20px">
      <strong>Hybrid lens.</strong> Combines a general 9-category enum (<code>inventory</code> · <code>production</code> · <code>contracts</code> · <code>pricing</code> · <code>counterparty</code> · <code>finance</code> · <code>setup</code> · <code>quality</code> · <code>audit_log</code>) with an independent 0-10 <code>market_risk_score</code> assigned in stage 2 and rationalised in stage 3 against the contract-field signature.
    </div>

    <div class="two-col" style="margin-top:24px">
      <div>
        <h2>Business Concept Distribution</h2>
        <div class="card">
          <table class="summary">
            <thead><tr><th>Concept</th><th class="num">Tables</th><th>Share</th></tr></thead>
            <tbody>{''.join(concept_rows)}</tbody>
          </table>
        </div>
      </div>
      <div>
        <h2>Market Risk Score Distribution</h2>
        <div class="card">
          <table class="summary">
            <thead><tr><th>Score</th><th class="num">Tables</th><th>Share</th></tr></thead>
            <tbody>{''.join(mr_rows)}</tbody>
          </table>
        </div>
      </div>
    </div>

    <h2>All {len(tables)} Tables — sorted by market_risk_score</h2>
    <p style="color:var(--muted);margin-top:-8px;">Click any row to expand the full description, market-risk rationale, contract field signature, column annotations, relationships, typical queries, and data quality notes.</p>
    <div class="section-controls">
      <button class="btn" onclick="toggleAll(true)">Expand all</button>
      <button class="btn" onclick="toggleAll(false)">Collapse all</button>
    </div>
    <div>{cards_html}</div>

    <footer>Source: <code>{escape(str(src))}</code> · {len(tables)} tables fully annotated</footer>
  </div>
</body>
</html>"""

    out.write_text(html)
    return str(out.resolve())
