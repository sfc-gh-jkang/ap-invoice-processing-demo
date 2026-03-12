-- =============================================================================
-- 06_automate.sql — Stream + Task for Automated Extraction (OPTIONAL)
--
-- Set this up AFTER you've validated batch extraction works in 04.
--
-- This creates:
--   1. A Stream on RAW_DOCUMENTS to detect new files
--   2. SP_EXTRACT_NEW_DOCUMENTS — SQL proc for invoice extraction
--      (populates both fixed columns AND raw_extraction VARIANT)
--   3. SP_EXTRACT_BY_DOC_TYPE — Python proc that reads DOCUMENT_TYPE_CONFIG
--      and dynamically builds AI_EXTRACT prompts for ANY document type
--   4. A Task that runs every 5 minutes (only when new data exists)
--
-- The result: drop a new file on the stage, and it's automatically
-- extracted within 5 minutes — no manual intervention.
-- =============================================================================

USE ROLE AI_EXTRACT_APP;
USE DATABASE AI_EXTRACT_POC;
USE SCHEMA DOCUMENTS;
USE WAREHOUSE AI_EXTRACT_WH;

-- ---------------------------------------------------------------------------
-- Stream: Detect new rows inserted into RAW_DOCUMENTS
-- ---------------------------------------------------------------------------
CREATE STREAM IF NOT EXISTS RAW_DOCUMENTS_STREAM
    ON TABLE RAW_DOCUMENTS
    APPEND_ONLY = TRUE
    COMMENT = 'Detects newly staged documents for automated extraction';

-- ---------------------------------------------------------------------------
-- Stored Procedure (SQL): Extract data from unprocessed INVOICE documents
-- ---------------------------------------------------------------------------
-- This wraps the same logic from 04_batch_extract.sql into a callable proc.
-- Populates BOTH fixed columns (field_1..field_10) AND raw_extraction VARIANT.
-- For non-invoice doc types, use SP_EXTRACT_BY_DOC_TYPE instead.

CREATE OR REPLACE PROCEDURE SP_EXTRACT_NEW_DOCUMENTS()
    RETURNS VARCHAR
    LANGUAGE SQL
    EXECUTE AS CALLER
AS
$$
BEGIN
    LET files_processed INT := 0;

    ALTER SESSION SET QUERY_TAG = 'ai_extract:proc=SP_EXTRACT_NEW_DOCUMENTS:doc_type=INVOICE';

    -- Entity extraction for unprocessed files
    -- Populates fixed columns AND raw_extraction VARIANT
    INSERT INTO EXTRACTED_FIELDS (
        file_name, field_1, field_2, field_3, field_4, field_5,
        field_6, field_7, field_8, field_9, field_10,
        raw_extraction
    )
    SELECT
        r.file_name,
        ext.extraction:response:vendor_name::VARCHAR,
        ext.extraction:response:document_number::VARCHAR,
        ext.extraction:response:reference::VARCHAR,
        TRY_TO_DATE(ext.extraction:response:document_date::VARCHAR),
        TRY_TO_DATE(ext.extraction:response:due_date::VARCHAR),
        ext.extraction:response:terms::VARCHAR,
        ext.extraction:response:recipient::VARCHAR,
        TRY_TO_NUMBER(REGEXP_REPLACE(ext.extraction:response:subtotal::VARCHAR, '[^0-9.]', ''), 12, 2),
        TRY_TO_NUMBER(REGEXP_REPLACE(ext.extraction:response:tax::VARCHAR, '[^0-9.]', ''), 12, 2),
        TRY_TO_NUMBER(REGEXP_REPLACE(ext.extraction:response:total::VARCHAR, '[^0-9.]', ''), 12, 2),
        ext.extraction:response                 -- Store full JSON in VARIANT
    FROM RAW_DOCUMENTS r,
        LATERAL (
            SELECT
                AI_EXTRACT(
                    TO_FILE('@DOCUMENT_STAGE', r.file_name),
                    {
                        'vendor_name':    'What is the vendor or company name on this document?',
                        'document_number':'What is the invoice number or document ID?',
                        'reference':      'What is the PO number, reference number, or order number?',
                        'document_date':  'What is the document date or invoice date? Return in YYYY-MM-DD format.',
                        'due_date':       'What is the due date or expiration date? Return in YYYY-MM-DD format.',
                        'terms':          'What are the payment terms or contract terms (e.g., Net 30)?',
                        'recipient':      'Who is this document addressed to? Return name and address.',
                        'subtotal':       'What is the subtotal amount before tax? Return as a number only.',
                        'tax':            'What is the tax amount? Return as a number only.',
                        'total':          'What is the total amount? Return as a number only.'
                    }
                ) AS extraction
        ) AS ext
    WHERE r.extracted = FALSE
      AND r.file_name NOT IN (SELECT file_name FROM EXTRACTED_FIELDS);

    -- Table extraction for unprocessed files
    -- Populates fixed columns AND raw_line_data VARIANT
    INSERT INTO EXTRACTED_TABLE_DATA (
        file_name, record_id, line_number, col_1, col_2, col_3, col_4, col_5,
        raw_line_data
    )
    WITH extracted AS (
        SELECT
            r.file_name,
            ef.field_2 AS record_id,
            AI_EXTRACT(
                file => TO_FILE('@DOCUMENT_STAGE', r.file_name),
                responseFormat => {
                    'schema': {
                        'type': 'object',
                        'properties': {
                            'line_items': {
                                'description': 'The table of line items on the document',
                                'type': 'object',
                                'column_ordering': ['Line', 'Description', 'Category', 'Qty', 'Unit Price', 'Total'],
                                'properties': {
                                    'Line':       { 'description': 'Line item number',          'type': 'array' },
                                    'Description':{ 'description': 'Product or service name',   'type': 'array' },
                                    'Category':   { 'description': 'Product category or type',  'type': 'array' },
                                    'Qty':        { 'description': 'Quantity',                  'type': 'array' },
                                    'Unit Price': { 'description': 'Price per unit in dollars', 'type': 'array' },
                                    'Total':      { 'description': 'Line total in dollars',     'type': 'array' }
                                }
                            }
                        }
                    }
                }
            ) AS extraction
        FROM RAW_DOCUMENTS r
            JOIN EXTRACTED_FIELDS ef ON r.file_name = ef.file_name
        WHERE r.extracted = FALSE
          AND r.file_name NOT IN (SELECT DISTINCT file_name FROM EXTRACTED_TABLE_DATA)
    )
    SELECT
        e.file_name,
        e.record_id,
        TRY_TO_NUMBER(ln.value::VARCHAR)                                            AS line_number,
        pr.value::VARCHAR                                                           AS col_1,
        ca.value::VARCHAR                                                           AS col_2,
        TRY_TO_NUMBER(REGEXP_REPLACE(qt.value::VARCHAR, '[^0-9.]', ''), 10, 2)     AS col_3,
        TRY_TO_NUMBER(REGEXP_REPLACE(up.value::VARCHAR, '[^0-9.]', ''), 10, 2)     AS col_4,
        TRY_TO_NUMBER(REGEXP_REPLACE(tl.value::VARCHAR, '[^0-9.]', ''), 12, 2)     AS col_5,
        OBJECT_CONSTRUCT(
            'Description', pr.value::VARCHAR,
            'Category', ca.value::VARCHAR,
            'Qty', qt.value::VARCHAR,
            'Unit Price', up.value::VARCHAR,
            'Total', tl.value::VARCHAR
        )                                                                           AS raw_line_data
    FROM extracted e,
        LATERAL FLATTEN(INPUT => e.extraction:response:line_items:Line)             ln,
        LATERAL FLATTEN(INPUT => e.extraction:response:line_items:Description)      pr,
        LATERAL FLATTEN(INPUT => e.extraction:response:line_items:Category)         ca,
        LATERAL FLATTEN(INPUT => e.extraction:response:line_items:Qty)              qt,
        LATERAL FLATTEN(INPUT => e.extraction:response:line_items:"Unit Price")     up,
        LATERAL FLATTEN(INPUT => e.extraction:response:line_items:Total)            tl
    WHERE ln.index = pr.index
      AND ln.index = ca.index
      AND ln.index = qt.index
      AND ln.index = up.index
      AND ln.index = tl.index;

    -- Mark files as extracted
    SELECT COUNT(*) INTO :files_processed
    FROM RAW_DOCUMENTS WHERE extracted = FALSE
      AND file_name IN (SELECT file_name FROM EXTRACTED_FIELDS);

    UPDATE RAW_DOCUMENTS
    SET extracted = TRUE,
        extracted_at = CURRENT_TIMESTAMP()
    WHERE extracted = FALSE
      AND file_name IN (SELECT file_name FROM EXTRACTED_FIELDS);

    CALL SP_POPULATE_DOC_METADATA();

    RETURN 'Processed ' || :files_processed || ' new document(s)';
END;
$$;

-- ---------------------------------------------------------------------------
-- Stored Procedure (Python): Config-driven extraction for ANY document type
-- ---------------------------------------------------------------------------
-- Reads extraction_prompt from DOCUMENT_TYPE_CONFIG and dynamically builds
-- the AI_EXTRACT call. Supports any number of fields — no schema changes.
-- Use this for custom doc types (utility bills, medical claims, etc.)
--
-- Usage:  CALL SP_EXTRACT_BY_DOC_TYPE('UTILITY_BILL');
--         CALL SP_EXTRACT_BY_DOC_TYPE('ALL');  -- processes all active types

CREATE OR REPLACE PROCEDURE SP_EXTRACT_BY_DOC_TYPE(P_DOC_TYPE VARCHAR)
    RETURNS VARCHAR
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'run'
    EXECUTE AS CALLER
AS
$$
import json
import re
from datetime import datetime

# ── A2: Post-processing normalization ──────────────────────────────────────
def _normalize(value, field_type):
    """Normalize an extracted value based on its declared field type.

    Rules:
      DATE    → YYYY-MM-DD string (tries multiple input formats)
      NUMBER  → plain numeric string, no $, commas, or units; null/None → "0"
      VARCHAR → stripped string; null/None → None
    """
    if value is None:
        return "0" if field_type == "NUMBER" else None

    raw = str(value).strip()
    if raw.lower() in ('null', 'none', 'n/a', ''):
        return "0" if field_type == "NUMBER" else None

    if field_type == "DATE":
        # Strip ordinal suffixes (1st, 2nd, 3rd, 4th)
        cleaned = re.sub(r'(\d+)(st|nd|rd|th)\b', r'\1', raw)
        for fmt in (
            '%Y-%m-%d', '%m/%d/%Y', '%m-%d-%Y', '%d/%m/%Y',
            '%B %d, %Y', '%b %d, %Y', '%B %d %Y', '%b %d %Y',
            '%d %B %Y', '%d %b %Y', '%Y/%m/%d',
        ):
            try:
                return datetime.strptime(cleaned, fmt).strftime('%Y-%m-%d')
            except ValueError:
                continue
        return raw  # return as-is if no format matches

    if field_type == "NUMBER":
        # Remove currency symbols, commas, units (kWh, kW, etc.)
        cleaned = re.sub(r'[^0-9.\-]', '', re.sub(r'\s*(kWh|kW|kwh|kw|%)\s*', '', raw))
        if not cleaned or cleaned in ('.', '-'):
            return "0"
        return cleaned

    # VARCHAR — just return stripped
    return raw


# ── Per-field extraction descriptions ─────────────────────────────────────
# These disambiguate commonly confused fields.  The key is the extraction
# field name (from extraction_prompt); the value is the precise question
# passed to AI_EXTRACT.  Fields not listed here fall back to a generic
# question derived from the field name.
_FIELD_DESCRIPTIONS = {
    # ── Monetary amounts (disambiguate total vs subtotal vs balance) ──
    'total_due':        "What is the FINAL total amount the customer must pay? "
                        "Look for a line labeled 'Total Due', 'Amount Due', 'Total', or 'Balance Due'. "
                        "Do NOT add previous_balance + current_charges yourself. "
                        "Return the single number printed on the document as the total.",
    'total_amount':     "What is the grand total amount on this document? "
                        "Look for a line labeled 'Total', 'Total Amount', 'Amount Due', or 'Grand Total'. "
                        "This should include tax. Return as a plain number.",
    'total_paid':       "What is the total amount paid? Look for 'Total', 'Total Paid', or 'Amount Charged'. "
                        "Return as a plain number.",
    'total_value':      "What is the total contract value? Return as a plain number.",
    'subtotal':         "What is the subtotal amount BEFORE tax? "
                        "Look for a line labeled 'Subtotal' or 'Sub-Total'. "
                        "Do NOT return the grand total. Return as a plain number.",
    'tax_amount':       "What is the tax amount? Look for 'Tax', 'Sales Tax', or 'VAT'. "
                        "Return as a plain number.",
    'current_charges':  "What are the current period charges ONLY? "
                        "Look for 'Current Charges', 'New Charges', or 'Charges This Period'. "
                        "Do NOT include previous balance or payments. Return as a plain number.",
    'previous_balance': "What is the previous balance or prior amount due? "
                        "Look for 'Previous Balance', 'Prior Balance', or 'Balance Forward'. "
                        "Return as a plain number.",
    'base_value':       "What is the base contract value before adjustments? Return as a plain number.",
    'adjustments':      "What are the contract adjustments or amendments amount? Return as a plain number.",

    # ── Dates (disambiguate due date vs billing/invoice/statement date) ──
    'due_date':         "What is the PAYMENT due date — the date by which payment must be received? "
                        "Look for 'Due Date', 'Payment Due', 'Pay By', or 'Due By'. "
                        "Do NOT return the invoice date, billing date, statement date, or billing period end date. "
                        "Return in YYYY-MM-DD format.",
    'invoice_date':     "What is the invoice date or document date — the date the invoice was issued? "
                        "Look for 'Invoice Date', 'Date', or 'Issued'. "
                        "Do NOT return the due date. Return in YYYY-MM-DD format.",
    'purchase_date':    "What is the purchase or transaction date? Return in YYYY-MM-DD format.",
    'return_by_date':   "What is the return-by or refund deadline date? Return in YYYY-MM-DD format.",
    'effective_date':   "What is the contract effective or start date? Return in YYYY-MM-DD format.",
    'expiration_date':  "What is the contract expiration or end date? Return in YYYY-MM-DD format.",
    'billing_period_start': "What is the START date of the billing period? "
                            "Look for 'Billing Period', 'Service From', or 'Period Start'. "
                            "Return in YYYY-MM-DD format.",
    'billing_period_end':   "What is the END date of the billing period? "
                            "Look for 'Billing Period', 'Service To', or 'Period End'. "
                            "Return in YYYY-MM-DD format.",

    # ── Entity names (full legal name, not abbreviations) ──
    'vendor_name':      "What is the full legal name of the vendor or company that issued this document? "
                        "Return the complete name, not abbreviations.",
    'utility_company':  "What is the full legal name of the utility company? "
                        "Return the complete registered name (e.g. 'Public Service Electric and Gas' "
                        "not 'PSE&G', 'Pacific Gas and Electric Company' not 'PG&E').",
    'merchant_name':    "What is the full name of the merchant or store?",
    'party_name':       "What is the full legal name of the primary party on this contract?",
    'counterparty':     "What is the full legal name of the counterparty on this contract?",
    'recipient':        "Who is this document addressed to? Return name and address.",
    'buyer':            "Who is the buyer or customer on this receipt?",

    # ── Reference numbers ──
    'invoice_number':   "What is the invoice number or document ID?",
    'po_number':        "What is the PO number, purchase order number, or reference number?",
    'account_number':   "What is the customer account number?",
    'meter_number':     "What is the utility meter number or meter ID?",
    'receipt_number':   "What is the receipt number?",
    'transaction_id':   "What is the transaction ID or confirmation number?",
    'contract_number':  "What is the contract number or agreement ID?",
    'reference_id':     "What is the reference ID?",

    # ── Other ──
    'payment_terms':    "What are the payment terms (e.g. Net 30, Net 60, Due on Receipt)?",
    'terms':            "What are the contract terms or conditions summary?",
    'payment_method':   "What payment method was used (e.g. Credit Card, Cash, Check)?",
    'service_address':  "What is the service address where utility service is provided?",
    'rate_schedule':    "What is the rate schedule or tariff name?",
    'kwh_usage':        "What is the total kWh electricity usage for this billing period? "
                        "Return as a plain number without 'kWh' units.",
    'demand_kw':        "What is the peak demand in kW? Return as a plain number without 'kW' units.",

    # ── Lease fields ──
    'landlord_name':    "What is the full legal name of the landlord or property management company?",
    'tenant_name':      "What is the full legal name of the tenant or lessee?",
    'lease_number':     "What is the lease number or agreement ID?",
    'property_address': "What is the full street address of the property being leased?",
    'lease_start_date': "What is the lease commencement or start date? Return in YYYY-MM-DD format.",
    'lease_end_date':   "What is the lease expiration or end date? Return in YYYY-MM-DD format.",
    'lease_term_months': "What is the lease term in months? Return as a plain number.",
    'monthly_rent':     "What is the monthly base rent amount? Return as a plain number.",
    'security_deposit': "What is the security deposit amount? Return as a plain number.",
    'payment_due_day':  "What day of the month is rent due? Return as a number (e.g. 1 for the 1st).",
    'late_fee':         "What is the late payment fee or penalty amount? Return as a plain number.",
    'total_lease_value': "What is the total lease value over the full term? Return as a plain number.",
}


# ── Company abbreviation → full legal name lookup ─────────────────────────
# Post-extraction normalization: if the AI returns an abbreviation, map it
# to the full legal name.  Keys are lowercased for case-insensitive lookup.
_COMPANY_ABBREVIATIONS = {
    # New York / New Jersey utilities
    'pseg':                     'Public Service Electric and Gas',
    'pse&g':                    'Public Service Electric and Gas',
    'pseg long island':         'PSEG Long Island',
    'con edison':               'Consolidated Edison',
    'coned':                    'Consolidated Edison',
    'con ed':                   'Consolidated Edison',
    'conedison':                'Consolidated Edison',
    'o&r':                      'Orange and Rockland Utilities',
    'oru':                      'Orange and Rockland Utilities',
    'orange & rockland':        'Orange and Rockland Utilities',
    'jcp&l':                    'Jersey Central Power & Light',
    'jcpl':                     'Jersey Central Power & Light',
    'nat grid':                 'National Grid',
    'natl grid':                'National Grid',
    # Major US utilities
    'pg&e':                     'Pacific Gas and Electric Company',
    'pge':                      'Pacific Gas and Electric Company',
    'sce':                      'Southern California Edison',
    'socal edison':             'Southern California Edison',
    'sdge':                     'San Diego Gas & Electric',
    'sdg&e':                    'San Diego Gas & Electric',
    'dte':                      'DTE Energy',
    'ppl':                      'PPL Electric Utilities',
    'aps':                      'Arizona Public Service',
    'fpl':                      'Florida Power & Light',
    'duke':                     'Duke Energy',
    'dominion':                 'Dominion Energy',
    'xcel':                     'Xcel Energy',
    'eversource':               'Eversource Energy',
    'ameren':                   'Ameren Corporation',
    'entergy':                  'Entergy Corporation',
    'consumers':                'Consumers Energy',
    'we energies':              'We Energies',
    'alliant':                  'Alliant Energy',
    'aep':                      'American Electric Power',
    'firstenergy':              'FirstEnergy',
    'nstar':                    'Eversource Energy',
    'pepco':                    'Potomac Electric Power Company',
    'bge':                      'Baltimore Gas and Electric',
    'bg&e':                     'Baltimore Gas and Electric',
    'cmp':                      'Central Maine Power',
    'united illuminating':      'United Illuminating',
    'ui':                       'United Illuminating',
    'nyseg':                    'New York State Electric & Gas',
    'rge':                      'Rochester Gas and Electric',
    'central hudson':           'Central Hudson Gas & Electric',
    'chge':                     'Central Hudson Gas & Electric',
}


def _resolve_company_name(value):
    """Resolve a company abbreviation to its full legal name.

    Performs case-insensitive lookup against _COMPANY_ABBREVIATIONS.
    Returns the full name if found, otherwise returns the original value.
    """
    if not value or not isinstance(value, str):
        return value
    key = value.strip().lower()
    return _COMPANY_ABBREVIATIONS.get(key, value)


# ── Heuristic confidence scoring ──────────────────────────────────────────
def _compute_heuristic_confidence(normalized, field_types, validation_warnings):
    """Compute a per-field confidence score (0.0–1.0) based on heuristics.

    Scoring rules (each field starts at 1.0):
      - Non-null value present:         +0.0 (baseline)
      - Null/missing/zero for NUMBER:   score = 0.1
      - Validation warning on field:    -0.3 per warning
      - DATE field matches ISO format:  +0.0 (no penalty)
      - DATE field raw (not ISO):       -0.2
      - NUMBER field is zero:           -0.1 (might be legitimate)
    Clamped to [0.0, 1.0].

    Returns dict: {field_name: float_score, ...}
    """
    # Build set of fields with warnings
    warned_fields = {}
    for w in (validation_warnings or []):
        fn = w.get('field', '')
        warned_fields[fn] = warned_fields.get(fn, 0) + 1

    scores = {}
    for fn, val in normalized.items():
        ftype = field_types.get(fn, 'VARCHAR')
        score = 1.0

        # Null / missing
        if val is None or (isinstance(val, str) and val.strip() == ''):
            score = 0.1
            scores[fn] = round(max(0.0, min(1.0, score)), 2)
            continue

        # NUMBER: zero might be legitimate but reduce confidence slightly
        if ftype == 'NUMBER':
            try:
                if float(str(val)) == 0.0:
                    score -= 0.1
            except (ValueError, TypeError):
                score -= 0.3

        # DATE: check ISO format
        if ftype == 'DATE':
            if not re.match(r'\d{4}-\d{2}-\d{2}$', str(val)):
                score -= 0.2

        # Validation warnings
        if fn in warned_fields:
            score -= 0.3 * warned_fields[fn]

        scores[fn] = round(max(0.0, min(1.0, score)), 2)

    return scores


def _build_prompt_with_confidence(field_names):
    """Build AI_EXTRACT prompt dict with per-field descriptive questions.

    Uses _FIELD_DESCRIPTIONS for known fields; falls back to a generic
    question for unknown fields.  The _confidence key is omitted because
    AI_EXTRACT does not populate it reliably.
    """
    prompt_parts = []
    for fn in field_names:
        desc = _FIELD_DESCRIPTIONS.get(fn)
        if desc:
            # Escape single quotes for SQL literal safety
            desc_escaped = desc.replace("'", "\\'")
            prompt_parts.append(f"'{fn}': '{desc_escaped}'")
        else:
            label = fn.replace('_', ' ').title()
            prompt_parts.append(f"'{fn}': 'What is the {label.lower()}? Return the value.'")
    return '{' + ', '.join(prompt_parts) + '}'


# ── Validation rules engine ───────────────────────────────────────────────
def _apply_validation_rules(normalized, field_types, validation_rules):
    """Apply per-field validation rules to normalized extraction results.

    Rules format (from DOCUMENT_TYPE_CONFIG.validation_rules):
        {
            "field_name": {
                "required": true,
                "min": 0,
                "max": 100000,
                "pattern": "^\\\\d{4}-\\\\d{2}-\\\\d{2}$",
                "date_min": "2020-01-01",
                "date_max": "2030-12-31"
            }
        }

    Returns a list of warning dicts: [{"field": ..., "rule": ..., "message": ...}]
    Does NOT reject or modify data — only flags issues for review.
    """
    if not validation_rules:
        return []

    warnings = []
    for field_name, rules in validation_rules.items():
        if not isinstance(rules, dict):
            continue
        value = normalized.get(field_name)
        ftype = field_types.get(field_name, 'VARCHAR')

        # Required check
        if rules.get('required') and (value is None or str(value).strip() == '' or value == '0' and ftype != 'NUMBER'):
            warnings.append({
                'field': field_name,
                'rule': 'required',
                'message': f'{field_name} is required but missing or empty'
            })

        if value is None:
            continue

        # Numeric range checks
        if ftype == 'NUMBER' and value is not None:
            try:
                num_val = float(str(value))
                if 'min' in rules and num_val < rules['min']:
                    warnings.append({
                        'field': field_name,
                        'rule': 'min',
                        'message': f'{field_name}={num_val} is below minimum {rules["min"]}'
                    })
                if 'max' in rules and num_val > rules['max']:
                    warnings.append({
                        'field': field_name,
                        'rule': 'max',
                        'message': f'{field_name}={num_val} exceeds maximum {rules["max"]}'
                    })
            except (ValueError, TypeError):
                pass

        # Date range checks
        if ftype == 'DATE' and value is not None:
            try:
                date_val = datetime.strptime(str(value), '%Y-%m-%d')
                if 'date_min' in rules:
                    min_date = datetime.strptime(rules['date_min'], '%Y-%m-%d')
                    if date_val < min_date:
                        warnings.append({
                            'field': field_name,
                            'rule': 'date_min',
                            'message': f'{field_name}={value} is before {rules["date_min"]}'
                        })
                if 'date_max' in rules:
                    max_date = datetime.strptime(rules['date_max'], '%Y-%m-%d')
                    if date_val > max_date:
                        warnings.append({
                            'field': field_name,
                            'rule': 'date_max',
                            'message': f'{field_name}={value} is after {rules["date_max"]}'
                        })
            except (ValueError, TypeError):
                pass

        # Pattern check (regex)
        if 'pattern' in rules and value is not None:
            if not re.match(rules['pattern'], str(value)):
                warnings.append({
                    'field': field_name,
                    'rule': 'pattern',
                    'message': f'{field_name}={value} does not match pattern {rules["pattern"]}'
                })

    return warnings


# ── A3: Config-driven table extraction ────────────────────────────────────
def _extract_table_data(session, fname, record_id, table_schema):
    """Run table/line-item extraction using responseFormat from config."""
    if not table_schema:
        return

    schema = json.loads(table_schema) if isinstance(table_schema, str) else table_schema
    columns = schema.get('columns', [])
    descriptions = schema.get('descriptions', [])
    if not columns:
        return

    # Build responseFormat properties
    props = {}
    for i, col in enumerate(columns):
        desc = descriptions[i] if i < len(descriptions) else col
        props[col] = {'description': desc, 'type': 'array'}

    response_format = {
        'schema': {
            'type': 'object',
            'properties': {
                'line_items': {
                    'description': 'The table of line items on the document',
                    'type': 'object',
                    'column_ordering': columns,
                    'properties': props
                }
            }
        }
    }

    rf_sql = json.dumps(response_format)

    try:
        result = session.sql(f"""
            SELECT AI_EXTRACT(
                file => TO_FILE('@DOCUMENT_STAGE', '{fname}'),
                responseFormat => PARSE_JSON('{rf_sql.replace("'", "''")}')
            ) AS extraction
        """).collect()

        if not result:
            return

        extraction = result[0]['EXTRACTION']
        ext_json = json.loads(extraction) if isinstance(extraction, str) else extraction
        response = ext_json.get('response', ext_json)
        line_items = response.get('line_items', {})

        if not line_items:
            return

        # Determine row count from first column array
        first_col = line_items.get(columns[0], [])
        if not isinstance(first_col, list):
            return
        num_rows = len(first_col)

        for row_idx in range(num_rows):
            col_values = []
            raw_line = {}
            for ci, col in enumerate(columns):
                arr = line_items.get(col, [])
                val = arr[row_idx] if row_idx < len(arr) else None
                raw_line[col] = str(val) if val is not None else None
                # Map to col_1..col_5 for fixed schema
                if ci < 5:
                    col_values.append(str(val) if val is not None else None)
            # Pad to 5
            while len(col_values) < 5:
                col_values.append(None)

            session.sql(
                "INSERT INTO EXTRACTED_TABLE_DATA "
                "(file_name, record_id, line_number, col_1, col_2, col_3, col_4, col_5, raw_line_data) "
                "SELECT ?, ?, ?, ?, ?, "
                "TRY_TO_NUMBER(REGEXP_REPLACE(?::VARCHAR, '[^0-9.]', ''), 10, 2), "
                "TRY_TO_NUMBER(REGEXP_REPLACE(?::VARCHAR, '[^0-9.]', ''), 10, 2), "
                "TRY_TO_NUMBER(REGEXP_REPLACE(?::VARCHAR, '[^0-9.]', ''), 12, 2), "
                "PARSE_JSON(?)",
                params=[
                    fname,
                    record_id,
                    row_idx + 1,
                    col_values[0],  # col_1 (description-like)
                    col_values[1],  # col_2 (category-like)
                    col_values[2],  # col_3 (qty-like)
                    col_values[3],  # col_4 (unit price-like)
                    col_values[4],  # col_5 (total-like)
                    json.dumps(raw_line),
                ]
            ).collect()

    except Exception:
        pass  # table extraction is best-effort; entity extraction already succeeded


# ── Main handler ──────────────────────────────────────────────────────────
def run(session, p_doc_type):
    """Config-driven extraction for any document type."""
    total_processed = 0

    # Get active doc type configs
    cols = "doc_type, extraction_prompt, field_labels, review_fields, table_extraction_schema, validation_rules"
    if p_doc_type == 'ALL':
        configs = session.sql(
            f"SELECT {cols} FROM DOCUMENT_TYPE_CONFIG WHERE active = TRUE"
        ).collect()
    else:
        configs = session.sql(
            f"SELECT {cols} FROM DOCUMENT_TYPE_CONFIG WHERE doc_type = ? AND active = TRUE",
            params=[p_doc_type]
        ).collect()

    if not configs:
        return f"No active config found for doc_type={p_doc_type}"

    for cfg in configs:
        doc_type = cfg['DOC_TYPE']
        prompt_text = cfg['EXTRACTION_PROMPT']
        labels_raw = cfg['FIELD_LABELS']
        labels = json.loads(labels_raw) if isinstance(labels_raw, str) else labels_raw

        # Parse review_fields for type info (A2 normalization)
        rf_raw = cfg['REVIEW_FIELDS']
        review_fields = json.loads(rf_raw) if isinstance(rf_raw, str) else (rf_raw or {})
        field_types = review_fields.get('types', {}) if review_fields else {}

        # Table extraction schema (A3)
        table_schema = cfg['TABLE_EXTRACTION_SCHEMA']

        # Validation rules (Phase 2)
        try:
            vr_raw = cfg['VALIDATION_RULES']
        except (KeyError, IndexError):
            vr_raw = None
        validation_rules = json.loads(vr_raw) if isinstance(vr_raw, str) else (vr_raw or {})

        # Find unprocessed files of this doc_type
        files = session.sql(
            "SELECT file_name FROM RAW_DOCUMENTS "
            "WHERE extracted = FALSE AND doc_type = ? "
            "AND file_name NOT IN (SELECT file_name FROM EXTRACTED_FIELDS)",
            params=[doc_type]
        ).collect()

        if not files:
            continue

        session.sql(
            f"ALTER SESSION SET QUERY_TAG = 'ai_extract:proc=SP_EXTRACT_BY_DOC_TYPE:doc_type={doc_type}'"
        ).collect()

        # Parse field names from the extraction_prompt
        # Format: "Extract the following fields from this X: field_a, field_b, ... FORMATTING RULES: ..."
        # Strip everything after "FORMATTING RULES:" or ". FORMATTING" before parsing fields
        prompt_for_parsing = re.split(r'\.\s*FORMATTING\s+RULES', prompt_text, maxsplit=1)[0]
        match = re.search(r':\s*(.+)$', prompt_for_parsing)
        if not match:
            continue
        field_names = [f.strip().rstrip('.') for f in match.group(1).split(',')]

        # A4: Build prompt with confidence scores
        prompt_obj = _build_prompt_with_confidence(field_names)

        for file_row in files:
            fname = file_row['FILE_NAME']
            try:
                # Run AI_EXTRACT
                result = session.sql(f"""
                    SELECT AI_EXTRACT(
                        TO_FILE('@DOCUMENT_STAGE', '{fname}'),
                        {prompt_obj}
                    ) AS extraction
                """).collect()

                if not result:
                    continue

                extraction = result[0]['EXTRACTION']
                ext_json = json.loads(extraction) if isinstance(extraction, str) else extraction
                response = ext_json.get('response', ext_json)

                # Remove any stale _confidence key (no longer requested in prompt)
                response.pop('_confidence', None)

                # A2: Normalize each field value using type info
                normalized = {}
                for fn in field_names:
                    raw_val = response.get(fn)
                    ftype = field_types.get(fn, 'VARCHAR')
                    normalized[fn] = _normalize(raw_val, ftype)

                # Resolve company abbreviations for name fields
                _NAME_FIELDS = {
                    'utility_company', 'vendor_name', 'merchant_name',
                    'party_name', 'counterparty',
                }
                for fn in field_names:
                    if fn in _NAME_FIELDS and normalized.get(fn):
                        normalized[fn] = _resolve_company_name(normalized[fn])

                # Apply validation rules and store warnings
                val_warnings = _apply_validation_rules(normalized, field_types, validation_rules)

                # Compute heuristic confidence scores
                confidence = _compute_heuristic_confidence(
                    normalized, field_types, val_warnings
                )

                # Build raw_extraction payload with normalized values + metadata
                store_response = dict(normalized)
                store_response['_confidence'] = confidence
                if val_warnings:
                    store_response['_validation_warnings'] = val_warnings

                # Map to field_1..field_10 for backward compat
                field_values = []
                for i, fn in enumerate(field_names[:10]):
                    field_values.append(normalized.get(fn))

                # Pad to 10 if fewer fields
                while len(field_values) < 10:
                    field_values.append(None)

                # Build INSERT using TABLE column types (physical schema),
                # not extraction field types.  The EXTRACTED_FIELDS table has:
                #   field_1-3: VARCHAR, field_4-5: DATE, field_6-7: VARCHAR,
                #   field_8-10: NUMBER(12,2)
                # TRY_TO_DATE / TRY_TO_NUMBER return NULL for incompatible
                # values instead of raising an error.
                TABLE_COL_TYPES = {
                    4: 'DATE', 5: 'DATE',
                    8: 'NUMBER', 9: 'NUMBER', 10: 'NUMBER',
                }
                placeholders = []
                params = [fname]
                for i in range(10):
                    val = field_values[i]
                    col_type = TABLE_COL_TYPES.get(i + 1, 'VARCHAR')
                    if col_type == 'DATE':
                        placeholders.append("TRY_TO_DATE(?::VARCHAR)")
                    elif col_type == 'NUMBER':
                        placeholders.append("TRY_TO_NUMBER(REGEXP_REPLACE(?::VARCHAR, '[^0-9.]', ''), 12, 2)")
                    else:
                        placeholders.append("?")
                    params.append(val)

                params.append(json.dumps(store_response))

                session.sql(
                    "INSERT INTO EXTRACTED_FIELDS "
                    "(file_name, field_1, field_2, field_3, field_4, field_5, "
                    " field_6, field_7, field_8, field_9, field_10, raw_extraction) "
                    "SELECT ?, " + ', '.join(placeholders) + ", PARSE_JSON(?)",
                    params=params
                ).collect()

                # Get the record_id we just inserted (for table extraction)
                rid_result = session.sql(
                    "SELECT record_id FROM EXTRACTED_FIELDS WHERE file_name = ? "
                    "ORDER BY record_id DESC LIMIT 1",
                    params=[fname]
                ).collect()
                record_id = rid_result[0]['RECORD_ID'] if rid_result else None

                # A3: Config-driven table extraction
                if record_id and table_schema:
                    _extract_table_data(session, fname, record_id, table_schema)

                # Mark as extracted
                session.sql(
                    "UPDATE RAW_DOCUMENTS SET extracted = TRUE, extracted_at = CURRENT_TIMESTAMP() "
                    "WHERE file_name = ?",
                    params=[fname]
                ).collect()

                total_processed += 1

            except Exception as e:
                # Record error but continue
                session.sql(
                    "UPDATE RAW_DOCUMENTS SET extraction_error = ? WHERE file_name = ?",
                    params=[str(e)[:500], fname]
                ).collect()

    session.sql("CALL SP_POPULATE_DOC_METADATA()").collect()

    return f"Processed {total_processed} document(s) via config-driven extraction"
$$;


-- ---------------------------------------------------------------------------
-- A5: Bulk re-extraction SP — clears and re-processes a doc type
-- ---------------------------------------------------------------------------
-- Usage:  CALL SP_REEXTRACT_DOC_TYPE('UTILITY_BILL');
--         CALL SP_REEXTRACT_DOC_TYPE('ALL');

CREATE OR REPLACE PROCEDURE SP_REEXTRACT_DOC_TYPE(P_DOC_TYPE VARCHAR)
    RETURNS VARCHAR
    LANGUAGE SQL
    EXECUTE AS CALLER
AS
$$
BEGIN
    IF (:P_DOC_TYPE = 'ALL') THEN
        -- Delete table data for all config-driven doc types
        DELETE FROM EXTRACTED_TABLE_DATA
        WHERE file_name IN (
            SELECT file_name FROM RAW_DOCUMENTS
            WHERE doc_type IN (SELECT doc_type FROM DOCUMENT_TYPE_CONFIG WHERE active = TRUE)
        );
        -- Delete extracted fields
        DELETE FROM EXTRACTED_FIELDS
        WHERE file_name IN (
            SELECT file_name FROM RAW_DOCUMENTS
            WHERE doc_type IN (SELECT doc_type FROM DOCUMENT_TYPE_CONFIG WHERE active = TRUE)
        );
        -- Reset extracted flag
        UPDATE RAW_DOCUMENTS
        SET extracted = FALSE, extracted_at = NULL, extraction_error = NULL
        WHERE doc_type IN (SELECT doc_type FROM DOCUMENT_TYPE_CONFIG WHERE active = TRUE);
    ELSE
        DELETE FROM EXTRACTED_TABLE_DATA
        WHERE file_name IN (
            SELECT file_name FROM RAW_DOCUMENTS WHERE doc_type = :P_DOC_TYPE
        );
        DELETE FROM EXTRACTED_FIELDS
        WHERE file_name IN (
            SELECT file_name FROM RAW_DOCUMENTS WHERE doc_type = :P_DOC_TYPE
        );
        UPDATE RAW_DOCUMENTS
        SET extracted = FALSE, extracted_at = NULL, extraction_error = NULL
        WHERE doc_type = :P_DOC_TYPE;
    END IF;

    -- Now re-extract using config-driven SP
    CALL SP_EXTRACT_BY_DOC_TYPE(:P_DOC_TYPE);

    RETURN 'Re-extraction complete for doc_type=' || :P_DOC_TYPE;
END;
$$;

-- ---------------------------------------------------------------------------
-- Task: Run extraction every 5 minutes when new documents exist
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TASK EXTRACT_NEW_DOCUMENTS_TASK
    WAREHOUSE = AI_EXTRACT_WH
    SCHEDULE = '5 MINUTE'
    COMMENT = 'Auto-extract newly staged documents using AI_EXTRACT'
    WHEN SYSTEM$STREAM_HAS_DATA('RAW_DOCUMENTS_STREAM')
AS
    CALL SP_EXTRACT_NEW_DOCUMENTS();

-- Enable the task (tasks are created in SUSPENDED state by default)
ALTER TASK EXTRACT_NEW_DOCUMENTS_TASK RESUME;

-- ---------------------------------------------------------------------------
-- Verify
-- ---------------------------------------------------------------------------
SHOW TASKS LIKE 'EXTRACT_NEW_DOCUMENTS_TASK';

-- To test the task manually (without waiting 5 minutes):
-- EXECUTE TASK EXTRACT_NEW_DOCUMENTS_TASK;

-- To process a specific doc type using config-driven extraction:
-- CALL SP_EXTRACT_BY_DOC_TYPE('UTILITY_BILL');
-- CALL SP_EXTRACT_BY_DOC_TYPE('ALL');

-- To pause the task:
-- ALTER TASK EXTRACT_NEW_DOCUMENTS_TASK SUSPEND;
