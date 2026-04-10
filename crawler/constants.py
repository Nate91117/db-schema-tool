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
EXTENSION_PREFIXES = ["Z", "X", "CUS", "ISV"]


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
    """Return the Stage 2 scoring system prompt for the given industry."""
    context = INDUSTRY_CONTEXTS.get(industry, INDUSTRY_CONTEXTS["biofuel"])
    industry_label = industry.replace("_", " ").title()

    return f"""You are building a semantic data layer for a {industry_label} company's ERP system.
Your job is to score database tables for relevance to business operations.

{context}
{memory_context}
You will receive table metadata including:
- Row count and primary keys
- Foreign key relationships (from the actual DB schema)
- Columns with data types
- Column statistics: null %, distinct count, numeric range (from a sample)

Use this information to score each table's business relevance.
Tables with many nulls on key columns, very low distinct counts, or nonsensical ranges may be noise.
Tables with rich FK relationships to other tables, date columns, and numeric amounts are usually valuable.

Return ONLY a JSON object. No preamble, no explanation outside the JSON.

Format:
{{
  "TABLE_NAME": {{
    "score": <1-10>,
    "reason": "<one sentence explaining the score>",
    "likely_concept": "<inventory|production|rin|pricing|vendor|customer|finance|planning|quality|noise|unknown>",
    "key_columns": ["col1", "col2", "col3"]
  }}
}}

Score guide:
  9-10  Core transactional table (inventory movements, production batches, invoices)
  7-8   Important reference or header table (item master, vendor master, order headers)
  5-6   Possibly useful — setup, configuration, or status data
  3-4   Low value — mostly codes, lookups, or metadata
  1-2   Noise — system tables, logs, or completely irrelevant
"""


def get_stage3_system_prompt(industry: str = "biofuel", memory_context: str = "") -> str:
    """Return the Stage 3 annotation system prompt for the given industry."""
    industry_label = industry.replace("_", " ").title()

    return f"""You are building a semantic data layer for a {industry_label} company's ERP database.
For each table, generate a rich annotation that a business analyst or AI agent can use to understand the data.
{memory_context}

You will receive:
- Table name, row count, AI relevance score
- All columns with data types
- Confirmed foreign key relationships (from the actual DB schema — use these, don't guess)
- Sample rows showing real data

Return ONLY a JSON object with this exact format:
{{
  "table_name": "ACTUAL_TABLE_NAME",
  "description": "Plain English description of what this table stores and its business purpose",
  "business_concept": "inventory|production|rin|pricing|vendor|customer|finance|planning|quality",
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
