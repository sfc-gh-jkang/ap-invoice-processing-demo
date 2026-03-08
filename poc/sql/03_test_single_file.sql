-- =============================================================================
-- 03_test_single_file.sql — Test AI_EXTRACT on ONE File Before Batch
--
-- ALWAYS run this first. It lets you:
--   1. See the raw JSON output from AI_EXTRACT
--   2. Validate your prompts return the right data
--   3. Adjust prompts before committing to a batch run
--
-- This script does NOT write to any tables — it's read-only exploration.
-- =============================================================================

USE ROLE AI_EXTRACT_APP;
USE DATABASE AI_EXTRACT_POC;
USE SCHEMA DOCUMENTS;
USE WAREHOUSE AI_EXTRACT_WH;

-- ---------------------------------------------------------------------------
-- Step 1: Pick a file to test
-- ---------------------------------------------------------------------------
-- List your staged files and pick one to test with:
SELECT RELATIVE_PATH, SIZE, LAST_MODIFIED
FROM DIRECTORY(@DOCUMENT_STAGE)
ORDER BY LAST_MODIFIED DESC
LIMIT 20;

-- Set the file you want to test (replace with an actual filename from above):
SET test_file = 'your_document.pdf';  -- <-- EDIT THIS


-- =============================================================================
-- ENTITY EXTRACTION — Pull key-value fields from the document
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Step 2: Run entity extraction on your test file
-- ---------------------------------------------------------------------------
-- The prompt format is: { 'label': 'question or description' }
-- AI_EXTRACT answers each question by reading the document.
--
-- TIP: Be specific in your prompts. Compare:
--   BAD:  'date'           → ambiguous (which date?)
--   GOOD: 'What is the invoice date? Return in YYYY-MM-DD format.'
--
-- TIP: For numeric fields, add "Return as a number only" to avoid
--   getting "$1,234.56" (string) instead of 1234.56 (number).

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  EXAMPLE: Invoice prompts (the default)                                │
-- │  Replace these with prompts appropriate for YOUR document type.        │
-- └─────────────────────────────────────────────────────────────────────────┘

SELECT AI_EXTRACT(
    TO_FILE('@DOCUMENT_STAGE', $test_file),
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
) AS extraction;

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  EXAMPLE: Contract prompts                                             │
-- └─────────────────────────────────────────────────────────────────────────┘
-- SELECT AI_EXTRACT(
--     TO_FILE('@DOCUMENT_STAGE', $test_file),
--     {
--         'party_a':        'Who is the first party or client in this contract?',
--         'party_b':        'Who is the second party or service provider?',
--         'effective_date': 'What is the effective date or start date? Return in YYYY-MM-DD format.',
--         'expiration':     'What is the expiration or end date? Return in YYYY-MM-DD format.',
--         'contract_value': 'What is the total contract value? Return as a number only.',
--         'governing_law':  'What state or jurisdiction governs this contract?',
--         'auto_renew':     'Does this contract auto-renew? Return YES or NO.'
--     }
-- ) AS extraction;

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  EXAMPLE: Receipt prompts                                              │
-- └─────────────────────────────────────────────────────────────────────────┘
-- SELECT AI_EXTRACT(
--     TO_FILE('@DOCUMENT_STAGE', $test_file),
--     {
--         'store_name':      'What is the store or merchant name?',
--         'receipt_number':  'What is the receipt or transaction number?',
--         'date':            'What is the transaction date? Return in YYYY-MM-DD format.',
--         'total':           'What is the total amount paid? Return as a number only.',
--         'payment_method':  'What payment method was used (cash, credit card, etc.)?',
--         'card_last_four':  'What are the last 4 digits of the card number?'
--     }
-- ) AS extraction;


-- =============================================================================
-- TABLE EXTRACTION — Pull tabular/line-item data from the document
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Step 3: Run table extraction on the same test file
-- ---------------------------------------------------------------------------
-- Table extraction uses a JSON schema format to describe the columns.
-- Each column is an array — AI_EXTRACT returns parallel arrays that you
-- FLATTEN to get rows.
--
-- LIMITS: Max 10 table extraction questions per call.
-- Each table question counts as 10 entity questions.
-- Max output: 4096 tokens per table.

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  EXAMPLE: Invoice line items (the default)                             │
-- │  Rename the columns to match the tables in YOUR documents.             │
-- └─────────────────────────────────────────────────────────────────────────┘

SELECT AI_EXTRACT(
    file => TO_FILE('@DOCUMENT_STAGE', $test_file),
    responseFormat => {
        'schema': {
            'type': 'object',
            'properties': {
                'line_items': {
                    'description': 'The table of line items on the document',
                    'type': 'object',
                    'column_ordering': ['Line', 'Description', 'Category', 'Qty', 'Unit Price', 'Total'],
                    'properties': {
                        'Line':       { 'description': 'Line item number',             'type': 'array' },
                        'Description':{ 'description': 'Product or service name',      'type': 'array' },
                        'Category':   { 'description': 'Product category or type',     'type': 'array' },
                        'Qty':        { 'description': 'Quantity',                     'type': 'array' },
                        'Unit Price': { 'description': 'Price per unit in dollars',    'type': 'array' },
                        'Total':      { 'description': 'Line total in dollars',        'type': 'array' }
                    }
                }
            }
        }
    }
) AS extraction;

-- ---------------------------------------------------------------------------
-- Step 4: Review the output
-- ---------------------------------------------------------------------------
-- Check: Do the entity fields match what's on the document?
-- Check: Are the table columns populated and aligned correctly?
-- Check: Are dates in YYYY-MM-DD format? Are numbers clean (no $ or commas)?
--
-- If the output looks wrong:
--   - Make your prompts more specific
--   - Add format instructions ("Return in YYYY-MM-DD format")
--   - For tables, add 'description' fields to help locate the right table
--
-- If the output looks right → proceed to 04_batch_extract.sql
