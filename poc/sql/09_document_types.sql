-- =============================================================================
-- 09_document_types.sql — Document Type Configuration
--
-- Creates:
--   1. DOCUMENT_TYPE_CONFIG  — Stores per-type prompts, UI label mappings,
--      table extraction schemas, reviewable field definitions, and active flag
--
-- Each row defines how a document type should be extracted and displayed:
--   - extraction_prompt: The AI_EXTRACT prompt template for entity fields
--   - field_labels: JSON mapping of generic field names to display labels
--   - table_extraction_schema: JSON schema for line-item/table extraction
--   - review_fields: JSON defining which fields are correctable and their types
--   - active: Whether this doc type is enabled for extraction
--
-- Seed rows: INVOICE, CONTRACT, RECEIPT, UTILITY_BILL
-- Add your own types with a single INSERT statement — no code changes needed.
-- =============================================================================

USE ROLE AI_EXTRACT_APP;
USE DATABASE AI_EXTRACT_POC;
USE SCHEMA DOCUMENTS;
USE WAREHOUSE AI_EXTRACT_WH;

-- ---------------------------------------------------------------------------
-- DOCUMENT_TYPE_CONFIG: Prompt + label mapping per document type
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS DOCUMENT_TYPE_CONFIG (
    doc_type                 VARCHAR NOT NULL PRIMARY KEY,
    display_name             VARCHAR NOT NULL,
    extraction_prompt        VARCHAR,
    field_labels             VARIANT NOT NULL,      -- JSON: {"field_1": "Label", ...}
    table_extraction_schema  VARIANT,               -- JSON: table/line-item schema for AI_EXTRACT
    review_fields            VARIANT,               -- JSON: correctable fields + types
    validation_rules         VARIANT,               -- JSON: per-field validation (min, max, pattern, required)
    active                   BOOLEAN DEFAULT TRUE,   -- FALSE = skip during extraction
    created_at               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------------
-- Seed: INVOICE
-- ---------------------------------------------------------------------------
MERGE INTO DOCUMENT_TYPE_CONFIG AS tgt
USING (SELECT 'INVOICE' AS doc_type) AS src
ON tgt.doc_type = src.doc_type
WHEN MATCHED THEN UPDATE SET
    display_name = 'Invoice',
    extraction_prompt = 'Extract the following fields from this invoice: vendor_name, invoice_number, po_number, invoice_date, due_date, payment_terms, recipient, subtotal, tax_amount, total_amount. FORMATTING RULES: Return all dates in YYYY-MM-DD format. Return all monetary values as plain numbers without currency symbols or commas (e.g. 1234.56 not $1,234.56). Return numeric values without units. Return 0 for zero or missing amounts, not null. Return the full legal company or person name, not abbreviations.',
    field_labels = PARSE_JSON('{"field_1":"Vendor Name","field_2":"Invoice Number","field_3":"PO Number","field_4":"Invoice Date","field_5":"Due Date","field_6":"Payment Terms","field_7":"Recipient","field_8":"Subtotal","field_9":"Tax Amount","field_10":"Total Amount","sender_label":"Vendor / Sender","amount_label":"Total Amount","date_label":"Invoice Date","reference_label":"Invoice #","secondary_ref_label":"PO #"}'),
    table_extraction_schema = PARSE_JSON('{"columns":["Line","Description","Category","Qty","Unit Price","Total"],"descriptions":["Line item number","Product or service name","Product category","Quantity","Price per unit","Line total"]}'),
    review_fields = PARSE_JSON('{"correctable":["vendor_name","invoice_number","po_number","invoice_date","due_date","payment_terms","recipient","subtotal","tax_amount","total_amount"],"display_order":["vendor_name","invoice_number","po_number","invoice_date","due_date","payment_terms","recipient","subtotal","tax_amount","total_amount"],"types":{"vendor_name":"VARCHAR","invoice_number":"VARCHAR","po_number":"VARCHAR","invoice_date":"DATE","due_date":"DATE","payment_terms":"VARCHAR","recipient":"VARCHAR","subtotal":"NUMBER","tax_amount":"NUMBER","total_amount":"NUMBER"}}'),
    validation_rules = PARSE_JSON('{"total_amount":{"required":true,"min":0,"max":10000000},"subtotal":{"min":0,"max":10000000},"tax_amount":{"min":0,"max":1000000},"invoice_date":{"required":true,"date_min":"2020-01-01","date_max":"2030-12-31"},"due_date":{"date_min":"2020-01-01","date_max":"2030-12-31"},"vendor_name":{"required":true}}'),
    updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (doc_type, display_name, extraction_prompt, field_labels, table_extraction_schema, review_fields, validation_rules)
VALUES (
    'INVOICE',
    'Invoice',
    'Extract the following fields from this invoice: vendor_name, invoice_number, po_number, invoice_date, due_date, payment_terms, recipient, subtotal, tax_amount, total_amount. FORMATTING RULES: Return all dates in YYYY-MM-DD format. Return all monetary values as plain numbers without currency symbols or commas (e.g. 1234.56 not $1,234.56). Return numeric values without units. Return 0 for zero or missing amounts, not null. Return the full legal company or person name, not abbreviations.',
    PARSE_JSON('{"field_1":"Vendor Name","field_2":"Invoice Number","field_3":"PO Number","field_4":"Invoice Date","field_5":"Due Date","field_6":"Payment Terms","field_7":"Recipient","field_8":"Subtotal","field_9":"Tax Amount","field_10":"Total Amount","sender_label":"Vendor / Sender","amount_label":"Total Amount","date_label":"Invoice Date","reference_label":"Invoice #","secondary_ref_label":"PO #"}'),
    PARSE_JSON('{"columns":["Line","Description","Category","Qty","Unit Price","Total"],"descriptions":["Line item number","Product or service name","Product category","Quantity","Price per unit","Line total"]}'),
    PARSE_JSON('{"correctable":["vendor_name","invoice_number","po_number","invoice_date","due_date","payment_terms","recipient","subtotal","tax_amount","total_amount"],"display_order":["vendor_name","invoice_number","po_number","invoice_date","due_date","payment_terms","recipient","subtotal","tax_amount","total_amount"],"types":{"vendor_name":"VARCHAR","invoice_number":"VARCHAR","po_number":"VARCHAR","invoice_date":"DATE","due_date":"DATE","payment_terms":"VARCHAR","recipient":"VARCHAR","subtotal":"NUMBER","tax_amount":"NUMBER","total_amount":"NUMBER"}}'),
    PARSE_JSON('{"total_amount":{"required":true,"min":0,"max":10000000},"subtotal":{"min":0,"max":10000000},"tax_amount":{"min":0,"max":1000000},"invoice_date":{"required":true,"date_min":"2020-01-01","date_max":"2030-12-31"},"due_date":{"date_min":"2020-01-01","date_max":"2030-12-31"},"vendor_name":{"required":true}}')
);

-- ---------------------------------------------------------------------------
-- Seed: CONTRACT
-- ---------------------------------------------------------------------------
MERGE INTO DOCUMENT_TYPE_CONFIG AS tgt
USING (SELECT 'CONTRACT' AS doc_type) AS src
ON tgt.doc_type = src.doc_type
WHEN MATCHED THEN UPDATE SET
    display_name = 'Contract',
    extraction_prompt = 'Extract the following fields from this contract: party_name, contract_number, reference_id, effective_date, expiration_date, terms, counterparty, base_value, adjustments, total_value. FORMATTING RULES: Return all dates in YYYY-MM-DD format. Return all monetary values as plain numbers without currency symbols or commas (e.g. 1234.56 not $1,234.56). Return numeric values without units. Return 0 for zero or missing amounts, not null. Return the full legal company or person name, not abbreviations.',
    field_labels = PARSE_JSON('{"field_1":"Party Name","field_2":"Contract Number","field_3":"Reference ID","field_4":"Effective Date","field_5":"Expiration Date","field_6":"Terms","field_7":"Counterparty","field_8":"Base Value","field_9":"Adjustments","field_10":"Total Value","sender_label":"Party","amount_label":"Total Value","date_label":"Effective Date","reference_label":"Contract #","secondary_ref_label":"Ref ID"}'),
    table_extraction_schema = PARSE_JSON('{"columns":["Milestone","Due Date","Amount","Status"],"descriptions":["Milestone name","Payment due date","Payment amount","Milestone status"]}'),
    review_fields = PARSE_JSON('{"correctable":["party_name","contract_number","reference_id","effective_date","expiration_date","terms","counterparty","base_value","adjustments","total_value"],"display_order":["party_name","contract_number","reference_id","effective_date","expiration_date","terms","counterparty","base_value","adjustments","total_value"],"types":{"party_name":"VARCHAR","contract_number":"VARCHAR","reference_id":"VARCHAR","effective_date":"DATE","expiration_date":"DATE","terms":"VARCHAR","counterparty":"VARCHAR","base_value":"NUMBER","adjustments":"NUMBER","total_value":"NUMBER"}}'),
    updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (doc_type, display_name, extraction_prompt, field_labels, table_extraction_schema, review_fields)
VALUES (
    'CONTRACT',
    'Contract',
    'Extract the following fields from this contract: party_name, contract_number, reference_id, effective_date, expiration_date, terms, counterparty, base_value, adjustments, total_value. FORMATTING RULES: Return all dates in YYYY-MM-DD format. Return all monetary values as plain numbers without currency symbols or commas (e.g. 1234.56 not $1,234.56). Return numeric values without units. Return 0 for zero or missing amounts, not null. Return the full legal company or person name, not abbreviations.',
    PARSE_JSON('{"field_1":"Party Name","field_2":"Contract Number","field_3":"Reference ID","field_4":"Effective Date","field_5":"Expiration Date","field_6":"Terms","field_7":"Counterparty","field_8":"Base Value","field_9":"Adjustments","field_10":"Total Value","sender_label":"Party","amount_label":"Total Value","date_label":"Effective Date","reference_label":"Contract #","secondary_ref_label":"Ref ID"}'),
    PARSE_JSON('{"columns":["Milestone","Due Date","Amount","Status"],"descriptions":["Milestone name","Payment due date","Payment amount","Milestone status"]}'),
    PARSE_JSON('{"correctable":["party_name","contract_number","reference_id","effective_date","expiration_date","terms","counterparty","base_value","adjustments","total_value"],"display_order":["party_name","contract_number","reference_id","effective_date","expiration_date","terms","counterparty","base_value","adjustments","total_value"],"types":{"party_name":"VARCHAR","contract_number":"VARCHAR","reference_id":"VARCHAR","effective_date":"DATE","expiration_date":"DATE","terms":"VARCHAR","counterparty":"VARCHAR","base_value":"NUMBER","adjustments":"NUMBER","total_value":"NUMBER"}}')
);

-- ---------------------------------------------------------------------------
-- Seed: RECEIPT
-- ---------------------------------------------------------------------------
MERGE INTO DOCUMENT_TYPE_CONFIG AS tgt
USING (SELECT 'RECEIPT' AS doc_type) AS src
ON tgt.doc_type = src.doc_type
WHEN MATCHED THEN UPDATE SET
    display_name = 'Receipt',
    extraction_prompt = 'Extract the following fields from this receipt: merchant_name, receipt_number, transaction_id, purchase_date, return_by_date, payment_method, buyer, subtotal, tax_amount, total_paid. FORMATTING RULES: Return all dates in YYYY-MM-DD format. Return all monetary values as plain numbers without currency symbols or commas (e.g. 1234.56 not $1,234.56). Return numeric values without units. Return 0 for zero or missing amounts, not null. Return the full legal company or person name, not abbreviations.',
    field_labels = PARSE_JSON('{"field_1":"Merchant Name","field_2":"Receipt Number","field_3":"Transaction ID","field_4":"Purchase Date","field_5":"Return By Date","field_6":"Payment Method","field_7":"Buyer","field_8":"Subtotal","field_9":"Tax Amount","field_10":"Total Paid","sender_label":"Merchant","amount_label":"Total Paid","date_label":"Purchase Date","reference_label":"Receipt #","secondary_ref_label":"Transaction ID"}'),
    table_extraction_schema = PARSE_JSON('{"columns":["Item","Qty","Price","Total"],"descriptions":["Item purchased","Quantity","Unit price","Line total"]}'),
    review_fields = PARSE_JSON('{"correctable":["merchant_name","receipt_number","total_paid"],"display_order":["merchant_name","receipt_number","transaction_id","purchase_date","return_by_date","payment_method","buyer","subtotal","tax_amount","total_paid"],"types":{"merchant_name":"VARCHAR","receipt_number":"VARCHAR","transaction_id":"VARCHAR","purchase_date":"DATE","return_by_date":"DATE","payment_method":"VARCHAR","buyer":"VARCHAR","subtotal":"NUMBER","tax_amount":"NUMBER","total_paid":"NUMBER"}}'),
    updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (doc_type, display_name, extraction_prompt, field_labels, table_extraction_schema, review_fields)
VALUES (
    'RECEIPT',
    'Receipt',
    'Extract the following fields from this receipt: merchant_name, receipt_number, transaction_id, purchase_date, return_by_date, payment_method, buyer, subtotal, tax_amount, total_paid. FORMATTING RULES: Return all dates in YYYY-MM-DD format. Return all monetary values as plain numbers without currency symbols or commas (e.g. 1234.56 not $1,234.56). Return numeric values without units. Return 0 for zero or missing amounts, not null. Return the full legal company or person name, not abbreviations.',
    PARSE_JSON('{"field_1":"Merchant Name","field_2":"Receipt Number","field_3":"Transaction ID","field_4":"Purchase Date","field_5":"Return By Date","field_6":"Payment Method","field_7":"Buyer","field_8":"Subtotal","field_9":"Tax Amount","field_10":"Total Paid","sender_label":"Merchant","amount_label":"Total Paid","date_label":"Purchase Date","reference_label":"Receipt #","secondary_ref_label":"Transaction ID"}'),
    PARSE_JSON('{"columns":["Item","Qty","Price","Total"],"descriptions":["Item purchased","Quantity","Unit price","Line total"]}'),
    PARSE_JSON('{"correctable":["merchant_name","receipt_number","total_paid"],"display_order":["merchant_name","receipt_number","transaction_id","purchase_date","return_by_date","payment_method","buyer","subtotal","tax_amount","total_paid"],"types":{"merchant_name":"VARCHAR","receipt_number":"VARCHAR","transaction_id":"VARCHAR","purchase_date":"DATE","return_by_date":"DATE","payment_method":"VARCHAR","buyer":"VARCHAR","subtotal":"NUMBER","tax_amount":"NUMBER","total_paid":"NUMBER"}}')
);

-- ---------------------------------------------------------------------------
-- Seed: UTILITY_BILL
-- ---------------------------------------------------------------------------
MERGE INTO DOCUMENT_TYPE_CONFIG AS tgt
USING (SELECT 'UTILITY_BILL' AS doc_type) AS src
ON tgt.doc_type = src.doc_type
WHEN MATCHED THEN UPDATE SET
    display_name = 'Utility Bill',
    extraction_prompt = 'Extract the following fields from this utility bill: utility_company, account_number, meter_number, service_address, billing_period_start, billing_period_end, rate_schedule, kwh_usage, demand_kw, previous_balance, current_charges, total_due, due_date. FORMATTING RULES: Return all dates in YYYY-MM-DD format. Return all monetary values as plain numbers without currency symbols or commas (e.g. 1234.56 not $1,234.56). Return numeric values without units (e.g. 898 not 898 kWh). Return 0 for zero or missing amounts, not null. Return the full legal company name, not abbreviations (e.g. Public Service Electric and Gas not PSE&G). CRITICAL: For total_due, return ONLY the number printed next to "Total Due" or "Amount Due" — do NOT compute it by adding other fields. For due_date, return ONLY the payment due date, NOT the billing period end date or statement date.',
    field_labels = PARSE_JSON('{"field_1":"Utility Company","field_2":"Account Number","field_3":"Meter Number","field_4":"Service Address","field_5":"Billing Period Start","field_6":"Billing Period End","field_7":"Rate Schedule","field_8":"kWh Usage","field_9":"Demand kW","field_10":"Previous Balance","field_11":"Current Charges","field_12":"Total Due","field_13":"Due Date","sender_label":"Utility Company","amount_label":"Total Due","date_label":"Due Date","reference_label":"Account #","secondary_ref_label":"Meter #"}'),
    table_extraction_schema = PARSE_JSON('{"columns":["Tier","kWh Range","Rate per kWh","Amount"],"descriptions":["Rate tier name","kWh range for this tier","Rate per kWh in dollars","Tier charge amount"]}'),
    review_fields = PARSE_JSON('{"correctable":["utility_company","account_number","meter_number","kwh_usage","total_due","due_date"],"display_order":["utility_company","account_number","meter_number","service_address","billing_period_start","billing_period_end","rate_schedule","kwh_usage","demand_kw","previous_balance","current_charges","total_due","due_date"],"types":{"utility_company":"VARCHAR","account_number":"VARCHAR","meter_number":"VARCHAR","service_address":"VARCHAR","billing_period_start":"DATE","billing_period_end":"DATE","rate_schedule":"VARCHAR","kwh_usage":"NUMBER","demand_kw":"NUMBER","previous_balance":"NUMBER","current_charges":"NUMBER","total_due":"NUMBER","due_date":"DATE"}}'),
    validation_rules = PARSE_JSON('{"total_due":{"required":true,"min":0,"max":100000},"current_charges":{"required":true,"min":0,"max":100000},"due_date":{"required":true,"date_min":"2020-01-01","date_max":"2030-12-31"},"billing_period_start":{"required":true},"billing_period_end":{"required":true},"kwh_usage":{"min":0,"max":999999},"demand_kw":{"min":0,"max":99999},"previous_balance":{"min":0,"max":100000},"utility_company":{"required":true}}'),
    updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (doc_type, display_name, extraction_prompt, field_labels, table_extraction_schema, review_fields, validation_rules)
VALUES (
    'UTILITY_BILL',
    'Utility Bill',
    'Extract the following fields from this utility bill: utility_company, account_number, meter_number, service_address, billing_period_start, billing_period_end, rate_schedule, kwh_usage, demand_kw, previous_balance, current_charges, total_due, due_date. FORMATTING RULES: Return all dates in YYYY-MM-DD format. Return all monetary values as plain numbers without currency symbols or commas (e.g. 1234.56 not $1,234.56). Return numeric values without units (e.g. 898 not 898 kWh). Return 0 for zero or missing amounts, not null. Return the full legal company name, not abbreviations (e.g. Public Service Electric and Gas not PSE&G). CRITICAL: For total_due, return ONLY the number printed next to "Total Due" or "Amount Due" — do NOT compute it by adding other fields. For due_date, return ONLY the payment due date, NOT the billing period end date or statement date.',
    PARSE_JSON('{"field_1":"Utility Company","field_2":"Account Number","field_3":"Meter Number","field_4":"Service Address","field_5":"Billing Period Start","field_6":"Billing Period End","field_7":"Rate Schedule","field_8":"kWh Usage","field_9":"Demand kW","field_10":"Previous Balance","field_11":"Current Charges","field_12":"Total Due","field_13":"Due Date","sender_label":"Utility Company","amount_label":"Total Due","date_label":"Due Date","reference_label":"Account #","secondary_ref_label":"Meter #"}'),
    PARSE_JSON('{"columns":["Tier","kWh Range","Rate per kWh","Amount"],"descriptions":["Rate tier name","kWh range for this tier","Rate per kWh in dollars","Tier charge amount"]}'),
    PARSE_JSON('{"correctable":["utility_company","account_number","meter_number","kwh_usage","total_due","due_date"],"display_order":["utility_company","account_number","meter_number","service_address","billing_period_start","billing_period_end","rate_schedule","kwh_usage","demand_kw","previous_balance","current_charges","total_due","due_date"],"types":{"utility_company":"VARCHAR","account_number":"VARCHAR","meter_number":"VARCHAR","service_address":"VARCHAR","billing_period_start":"DATE","billing_period_end":"DATE","rate_schedule":"VARCHAR","kwh_usage":"NUMBER","demand_kw":"NUMBER","previous_balance":"NUMBER","current_charges":"NUMBER","total_due":"NUMBER","due_date":"DATE"}}'),
    PARSE_JSON('{"total_due":{"required":true,"min":0,"max":100000},"current_charges":{"required":true,"min":0,"max":100000},"due_date":{"required":true,"date_min":"2020-01-01","date_max":"2030-12-31"},"billing_period_start":{"required":true},"billing_period_end":{"required":true},"kwh_usage":{"min":0,"max":999999},"demand_kw":{"min":0,"max":99999},"previous_balance":{"min":0,"max":100000},"utility_company":{"required":true}}')
);

-- Verify
SELECT doc_type, display_name, active,
       field_labels, table_extraction_schema, review_fields, validation_rules
FROM DOCUMENT_TYPE_CONFIG
ORDER BY doc_type;
