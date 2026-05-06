"""Constants and prompt factories for the schema discovery tool."""
from __future__ import annotations

# ── Stage 1: Heuristic filtering ─────────────────────────────────────────────

# Table name substrings that suggest business-relevant data
RELEVANT_KEYWORDS = [
    # Inventory & warehouse
    "INVENT", "ITEM", "STOCK", "WAREHOUSE", "WMS", "STORAGE", "LOT",
    # Production
    "PROD", "BATCH", "BOM", "ROUTE", "WORK", "SHOP", "FORMULA",
    # Procurement & vendors
    "PURCH", "VEND", "VENDOR", "SUPPLIER", "RECEIPT", "RECEIPT",
    # Sales & customers
    "SALES", "CUST", "ORDER", "INVOICE", "SHIP", "DELIVER",
    # Finance & cost
    "LEDGER", "JOURNAL", "TRANS", "COST", "PRICE", "MARGIN", "BUDGET",
    # Biofuel / chemical processing
    "RIN", "BLEND", "FEED", "FUEL", "CHEM", "FEEDSTOCK",
    # Custom / extension tables (very important in AX)
    "PLANT", "ASSET", "CAPACITY", "YIELD",
]

# Table name substrings that suggest system/noise tables — skip immediately
NOISE_KEYWORDS = [
    # AX system internals
    "SYSUSER", "SYSLAST", "SYSCONFIG", "SYSFIELD", "SYSIMPORT",
    "SYSLOG", "SYSCLIENT", "SYSTEMSEQUENCES", "SYSSETUPLOG",
    "SYSDATABASE", "SYSFILESTORE",
    # Logging & audit (too large, not business data)
    "BATCHJOB", "BATCHJOBHISTORY", "BATCHHISTORY",
    "DOCUREF", "DOCUVALUE", "DOCUACTION",
    "NUMBERSEQ", "NUMSEQSCOPE",
    # Security & permissions
    "SECURITYROLE", "SECURITYPRIVILEGE", "SECURITYDUTY",
    "PERMISSION", "USERINFO", "USERGROUP",
    # Dev/meta tables
    "SQLDICTIONARY", "MODELELEM", "XREFPATH", "XREF",
    "ENUMIDTABLE", "CLASSIDTABLE",
    # Workflow engine
    "WORKFLOW", "WORKFLOWSTEP", "WORKFLOWTASK",
    # Print / report infrastructure
    "PRINT", "REPORT", "SESSION", "AUDIT",
]

# Table name PREFIXES that indicate noise (checked before keyword scan)
NOISE_PREFIXES = [
    "DEL_",     # AX deleted-field backup tables  (DEL_INVENTTRANS, etc.)
    "SYS",      # System tables
    "TMP",      # Temporary staging tables
    "RETAIL_",  # Retail module (usually not relevant for manufacturing)
]

# Custom/extension table prefixes — score bonus (common in Dynamics AX)
# IRGN is the grain operation prefix in this client database; treat it as
# custom even though it doesn't fit the Z/X convention.
EXTENSION_PREFIXES = ["Z", "X", "CUS", "ISV", "IRGN"]


# ── Stage 2: Industry context ─────────────────────────────────────────────────

INDUSTRY_CONTEXTS: dict[str, str] = {
    "biofuel": """We care about data related to:
- Feedstock inventory and costs (soybean oil, corn oil, distillers corn oil, chemicals)
- Production batches and blend/process records
- RIN generation and tracking (D4, D5 categories)
- Margins, pricing, and cost accounting
- Vendor purchases and feedstock shipments
- Customer invoices and sales of finished product
- Any custom extension tables (often prefixed with Z or X in AX)""",

    "manufacturing": """We care about data related to:
- Raw material and finished goods inventory
- Production orders, work orders, routings, and shop floor records
- Bill of materials (BOM) and formula/recipe management
- Quality control and inspection results
- Machine utilization, downtime, and capacity
- Vendor purchasing, receipts, and supplier scorecards
- Customer orders, shipments, and invoices
- Cost accounting: standard cost, variances, overhead allocation
- Custom extension tables (Z or X prefix in AX)""",

    "food_processing": """We care about data related to:
- Ingredient and raw material inventory (lots, expiry dates)
- Batch production records and formulations
- Yield, waste, and co-product tracking
- Quality and safety (HACCP, allergens, lab results)
- Traceability: ingredient → batch → finished goods
- Vendor and raw material purchasing
- Customer orders, distribution, and invoices
- Custom extension tables""",

    "chemicals": """We care about data related to:
- Chemical inventory and storage (tanks, containers, hazmat)
- Production batch records and formulation versions
- Quality testing and Certificate of Analysis (CoA) data
- Safety, regulatory, and compliance records (SDS, EPA)
- Vendor purchasing and supplier qualifications
- Customer orders, shipments, and invoices
- Hazardous material tracking and reporting
- Custom extension tables""",

    "general": """We care about data related to:
- Core business transactions (orders, invoices, receipts, payments)
- Inventory and stock management
- Customer and vendor master data
- Financial transactions and cost accounting
- Production or service delivery records
- Custom extension tables (Z or X prefix)""",
}


def get_stage2_system_prompt(industry: str = "biofuel", memory_context: str = "") -> str:
    """Return the Stage 2 scoring system prompt for the given industry.

    Stage 2 assigns two independent scores per table:
      - score (1-10): overall business relevance
      - market_risk_score (0-10): commodity-position exposure
    Plus a primary likely_concept from a fixed taxonomy.
    """
    context = INDUSTRY_CONTEXTS.get(industry, INDUSTRY_CONTEXTS["biofuel"])
    industry_label = industry.replace("_", " ").title()

    return f"""You are building a semantic data layer for a {industry_label} company's ERP system.
Your job is to score database tables on TWO independent dimensions and to assign each
table to a primary business concept.

{context}
{memory_context}
You will receive, per table:
- Row count, primary keys, foreign key relationships (from the actual DB schema)
- Columns with data types
- Optional column statistics: null %, distinct count, numeric range
- A pre-computed CONTRACT/POSITION FIELD SIGNATURE listing which contract-style
  fields the column names already match. Use this as concrete evidence — the
  signature is computed in code, not invented.

============================================================
DIMENSION 1: overall relevance score (1-10)
============================================================
Use this for the `score` field — how useful this table is for understanding
the business in general.

  9-10  Core transactional table (inventory movements, production batches, invoices)
  7-8   Important reference or header table (item master, vendor master, order headers)
  5-6   Possibly useful — setup, configuration, or status data
  3-4   Low value — mostly codes, lookups, or metadata
  1-2   Noise — system tables, logs, or irrelevant

============================================================
DIMENSION 2: market_risk_score (0-10) — INDEPENDENT of score above
============================================================
A table can have likely_concept="counterparty" and still score 8 on market_risk.

Market risk = exposure to commodity-price movement on positions whose settled
value can still change with price. A "position" is anything in one of three
states:
  - INVENTORY: physical commodity owned but not yet sold/settled (in-process
    tickets, unpriced receipts, DPR balances, in-storage stock)
  - PURCHASES: active purchase contracts with live delivery obligations or
    pricing still to be applied
  - SALES: active sales contracts with live delivery obligations or pricing
    still to be applied

Tables that drive position math also count: priced-vs-unpriced state,
basis/futures offsets, hedge registers, settlement transactions that close
exposure, load reconciliations that flip inventory ownership.

Custom/extension tables — anything with a non-standard prefix such as IRGN,
Z, X, CUS, or ISV — are far more likely to encode commodity-position logic
specific to the business operation. Native Dynamics modules (GL*, PM*, RM*,
IV*, SOP*, and standard AX tables like INVENT*, PURCH*, SALES* without a
custom prefix) record the financial *result* of risk, not the risk itself,
and almost always score 0-3.

How to use the contract field signature:
  6+ core matches  -> strong contract/position table -> market_risk_score 7+
  3-5 core matches -> contributor table              -> market_risk_score 4-6
  0-2 core matches with no secondary matches         -> 0-3

market_risk_score guide:
   9-10  Core exposure: open/unsettled inventory or tickets, DPR balances,
         active contract delivery schedules and contract pricing, hedge/
         futures position registers, "WORK" tables holding in-process
         physical receipts
   7-8   Direct contributors: settlement transactions (including voids and
         advances), ticket detail/pricing/discount tables that change settled
         value, load shipments and reconciliations that flip inventory
         ownership, priced inventory movement tables
   4-6   Peripheral context: vendor/customer masters (counterparties),
         quality and grade factors (adjust settled value), finished-goods
         inventory where price is locked
   1-3   Barely touches risk: GL entries, AP/AR balances, journal lines
         (the financial result of risk, not the risk itself), reference
         lookups joined into risk reports
   0     No relevance: system/audit, security, workflow, HR, payroll,
         fixed-assets, project accounting, pure setup/config

Default to 0 unless the table clearly meets a tier above. Do NOT inflate
the score to be helpful — most ERP tables score 0.

============================================================
likely_concept TAXONOMY
============================================================
Pick the SINGLE best fit. Concepts are independent of market_risk_score.

  inventory     Physical stock state and movements: receipts, tickets,
                load shipments, in-process WORK, balances, transfers
  production    Manufacturing/processing: batches, assemblies, work orders,
                BOM, formula records, yield/output
  contracts     Purchase or sales contracts with delivery obligations:
                contract headers, delivery schedules, amendments, version
                history, contract programs. NOT settlement, NOT pricing.
  pricing       Price calculations and adjustments: contract pricing,
                hedging detail, basis/futures offsets, discount schedules,
                freight rates, settlement transactions that change value
  counterparty  Master/reference data for vendors, customers, suppliers:
                IDs, names, addresses, payment terms, bank info, credit
                limits. Vendor and customer roles BOTH live here.
  finance       AP/AR/GL plumbing: journals, ledger entries, vouchers,
                check registers, tax tables, financial result of business
  setup         Configuration and lookup data: calendars, codes, user
                defaults, shipping methods, region/branch lookups, system
                parameters. Does not hold transactions or positions.
  quality       Grade factors, quality grades, lab tests, inspections,
                discount-grade matrices that affect commodity value
  audit_log     Change history, comments, amendment trails, narrative
                logs. No quantitative position/transaction data.
  noise         System internals, security, workflow, HR/payroll, retail,
                pure plumbing with no business meaning
  unknown       Genuinely cannot determine from the available metadata

============================================================
OUTPUT
============================================================
Return ONLY a JSON object. No preamble, no explanation outside the JSON.

{{
  "TABLE_NAME": {{
    "score": <1-10>,
    "likely_concept": "<inventory|production|contracts|pricing|counterparty|finance|setup|quality|audit_log|noise|unknown>",
    "market_risk_score": <0-10>,
    "reason": "<one sentence explaining the score>",
    "key_columns": ["col1", "col2", "col3"]
  }}
}}
"""


def get_stage3_system_prompt(industry: str = "biofuel", memory_context: str = "") -> str:
    """Return the Stage 3 annotation system prompt for the given industry."""
    industry_label = industry.replace("_", " ").title()

    return f"""You are building a semantic data layer for a {industry_label} company's ERP database.
For each table, generate a rich annotation that a business analyst or AI agent can use to understand the data.
{memory_context}

You will receive:
- Table name, row count, AI relevance score
- AI-assigned market_risk_score (0-10) and the contract/position field signature used to derive it
- All columns with data types
- Confirmed foreign key relationships (from the actual DB schema — use these, don't guess)
- Sample rows showing real data

The market_risk_score was assigned in Stage 2 and is FINAL. Do not change it.
You must write a one-sentence `market_risk_rationale` explaining what gives this
table that score (or why the score is 0). The rationale is required if score >= 1
and may be a brief "no commodity-position exposure" if score == 0.

Return ONLY a JSON object with this exact format:
{{
  "table_name": "ACTUAL_TABLE_NAME",
  "description": "Plain English description of what this table stores and its business purpose",
  "business_concept": "inventory|production|contracts|pricing|counterparty|finance|setup|quality|audit_log",
  "market_risk_rationale": "<one sentence explaining the market_risk_score>",
  "columns": [
    {{
      "name": "COLUMN_NAME",
      "data_type": "sql data type",
      "description": "What this column represents in business terms",
      "business_meaning": "How this field is used in day-to-day operations",
      "is_primary_key": true/false,
      "notes": "enum codes, special values, or caveats if known"
    }}
  ],
  "relationships": [
    {{
      "from_column": "THIS_TABLE_COL",
      "to_table": "OTHER_TABLE",
      "to_column": "OTHER_COL",
      "relationship_type": "foreign_key|implied",
      "description": "Plain English description of the join"
    }}
  ],
  "typical_queries": [
    "Example business question this table answers",
    "Another example query"
  ],
  "data_quality_notes": "Any caveats about nulls, codes, data quality, or unusual patterns"
}}

For relationships: ONLY list foreign_key relationships that were confirmed in the DB schema.
Add implied relationships if you're confident (e.g., ITEMID clearly joins to INVENTTABLE).
"""
