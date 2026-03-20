"""
Page 4: Admin — Manage document type configurations.

No-JSON interface for non-technical users. Guided field builder auto-generates
extraction prompts, field labels, review fields, and table schemas from simple
form inputs. Supports re-extraction and test extraction.
"""

import json
import re
import tempfile
import os
import shutil
from datetime import datetime
import streamlit as st
import pandas as pd
from config import (
    DB, STAGE, get_session, get_all_doc_type_configs, get_doc_type_config,
    get_field_names_from_labels,
    inject_custom_css, sidebar_branding,
)

st.set_page_config(page_title="Admin: Document Types", page_icon="⚙️", layout="wide")

inject_custom_css()
with st.sidebar:
    sidebar_branding()

session = get_session()

st.title("Document Type Configuration")
st.caption("Add or edit document types — no coding or JSON required")

# ── Helpers ──────────────────────────────────────────────────────────────────

FIELD_TYPE_OPTIONS = ["Text", "Number", "Date"]
TYPE_MAP = {"Text": "VARCHAR", "Number": "NUMBER", "Date": "DATE"}
REVERSE_TYPE_MAP = {"VARCHAR": "Text", "NUMBER": "Number", "DATE": "Date"}

# Physical column types in EXTRACTED_FIELDS table (field_4-5=DATE, field_8-10=NUMBER)
TABLE_COL_TYPES = {4: 'DATE', 5: 'DATE', 8: 'NUMBER', 9: 'NUMBER', 10: 'NUMBER'}


def _normalize_value(value, field_type):
    """Normalize an extracted value (mirrors SP_EXTRACT_BY_DOC_TYPE._normalize)."""
    if value is None:
        return "0" if field_type == "NUMBER" else None
    raw = str(value).strip()
    if raw.lower() in ('null', 'none', 'n/a', ''):
        return "0" if field_type == "NUMBER" else None
    if field_type == "DATE":
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
        return raw
    if field_type == "NUMBER":
        cleaned = re.sub(r'[^0-9.\-]', '', re.sub(r'\s*(kWh|kW|kwh|kw|%)\s*', '', raw))
        if not cleaned or cleaned in ('.', '-'):
            return "0"
        return cleaned
    return raw


def _compute_confidence(value, field_type):
    """Compute a heuristic confidence score (0.0-1.0) for a single field."""
    if value is None or (isinstance(value, str) and value.strip() == ''):
        return 0.1
    score = 1.0
    if field_type == 'NUMBER':
        try:
            if float(str(value)) == 0.0:
                score -= 0.1
        except (ValueError, TypeError):
            score -= 0.3
    if field_type == 'DATE':
        if not re.match(r'\d{4}-\d{2}-\d{2}$', str(value)):
            score -= 0.2
    return round(max(0.0, min(1.0, score)), 2)


def _parse_field_names(prompt_text):
    """Parse field names from an extraction_prompt string."""
    prompt_for_parsing = re.split(r'\.\s*FORMATTING\s+RULES', prompt_text, maxsplit=1)[0]
    match = re.search(r':\s*(.+)$', prompt_for_parsing)
    if not match:
        return []
    return [f.strip().rstrip('.') for f in match.group(1).split(',')]


def _sanitize_filename(name: str) -> str:
    """Strip path traversal and SQL-dangerous characters from a filename.

    Allows only alphanumeric, dots, hyphens, underscores, and spaces.
    Raises ValueError if the result is empty or starts with a dot.
    """
    clean = re.sub(r'[^a-zA-Z0-9._\- ]', '_', name)
    clean = clean.lstrip('.')
    if not clean:
        raise ValueError("Invalid filename after sanitization")
    return clean


def _sql_escape(value: str) -> str:
    """Escape a string for use in a SQL single-quoted literal.

    Doubles single quotes to prevent SQL injection.
    """
    return value.replace("'", "''")


def _cleanup_test_state(session_obj):
    """Remove staged test file and clear session_state."""
    fname = st.session_state.get('test_ext_fname')
    if fname:
        try:
            safe_fname = _sanitize_filename(fname)
            session_obj.sql(
                f"REMOVE @{DB}.DOCUMENT_STAGE/{safe_fname}"
            ).collect()
        except Exception:
            pass
    for key in ('test_ext_result', 'test_ext_fname', 'test_ext_field_names',
                'test_ext_doc_type', 'test_ext_cfg', 'test_ext_saved',
                'test_ext_table_data'):
        st.session_state.pop(key, None)


def _build_config_from_fields(doc_type_code, display_name, fields, table_columns=None):
    """Build all config JSON from a list of field dicts.

    Each field dict: {"name": "vendor_name", "label": "Vendor Name", "type": "Text", "correctable": True}
    Returns: (prompt, field_labels, review_fields, table_extraction_schema)
    """
    # Extraction prompt
    field_names = [f["name"] for f in fields]
    prompt = (
        f"Extract the following fields from this {display_name.lower()}: "
        + ", ".join(field_names)
        + ". FORMATTING RULES: Return all dates in YYYY-MM-DD format. "
        "Return all monetary values as plain numbers without currency symbols or commas "
        "(e.g. 1234.56 not $1,234.56). Return numeric values without units. "
        "Return 0 for zero or missing amounts, not null. "
        "Return the full legal company or person name, not abbreviations."
    )

    # Field labels
    field_labels = {}
    for i, f in enumerate(fields):
        field_labels[f"field_{i+1}"] = f["label"]
    # Add meta-labels (use first field as sender, last NUMBER as amount, first DATE as date)
    if fields:
        field_labels["sender_label"] = fields[0]["label"]
    for f in fields:
        if f["type"] == "Number":
            field_labels["amount_label"] = f["label"]
    for f in fields:
        if f["type"] == "Date":
            field_labels["date_label"] = f["label"]
    # Reference labels from first two text fields
    text_fields = [f for f in fields if f["type"] == "Text"]
    if len(text_fields) >= 2:
        field_labels["reference_label"] = text_fields[1]["label"]
    if len(text_fields) >= 3:
        field_labels["secondary_ref_label"] = text_fields[2]["label"]

    # Review fields
    correctable = [f["name"] for f in fields if f.get("correctable", True)]
    types = {f["name"]: TYPE_MAP[f["type"]] for f in fields}
    review_fields = {"correctable": correctable, "types": types}

    # Table extraction schema
    table_schema = None
    if table_columns:
        cols = [tc["name"] for tc in table_columns]
        descs = [tc.get("description", tc["name"]) for tc in table_columns]
        table_schema = {"columns": cols, "descriptions": descs}

    return prompt, field_labels, review_fields, table_schema


# ── Current Configurations ───────────────────────────────────────────────────
st.subheader("Existing Document Types")

configs = get_all_doc_type_configs(session)

if configs:
    summary_data = []
    for cfg in configs:
        labels = cfg.get("field_labels") or {}
        field_count = len([k for k in labels if k.startswith("field_")])
        summary_data.append({
            "Doc Type": cfg["doc_type"],
            "Display Name": cfg["display_name"],
            "Fields": field_count,
            "Active": cfg.get("active", True),
        })

    st.dataframe(
        pd.DataFrame(summary_data),
        hide_index=True,
        use_container_width=True,
        column_config={
            "Active": st.column_config.CheckboxColumn("Active"),
        },
    )

    # ── Detail viewer / editor ──────────────────────────────────────────────
    st.divider()
    st.subheader("View / Edit Configuration")

    selected_type = st.selectbox(
        "Select type to view",
        [c["doc_type"] for c in configs],
    )

    if selected_type:
        cfg = get_doc_type_config(session, selected_type)
        if cfg:
            tab_view, tab_actions = st.tabs(["Configuration", "Actions"])

            with tab_view:
                col1, col2 = st.columns(2)

                with col1:
                    st.markdown("**Extraction Prompt:**")
                    st.code(cfg["extraction_prompt"] or "", language="text")

                    st.markdown("**Fields:**")
                    labels = cfg.get("field_labels") or {}
                    review_fields = cfg.get("review_fields") or {}
                    types = review_fields.get("types", {})
                    correctable = review_fields.get("correctable", [])

                    field_keys = get_field_names_from_labels(labels)
                    field_data = []
                    for fk in field_keys:
                        idx = int(fk.split("_")[1]) - 1
                        label = labels.get(fk, "")
                        # Derive extraction name from correctable list or label
                        if correctable and idx < len(correctable):
                            ext_name = correctable[idx]
                        else:
                            ext_name = label.lower().replace(" ", "_")
                        ftype = types.get(ext_name, "VARCHAR")
                        field_data.append({
                            "#": idx + 1,
                            "Field Name": ext_name,
                            "Label": label,
                            "Type": REVERSE_TYPE_MAP.get(ftype, "Text"),
                            "Correctable": ext_name in correctable,
                        })

                    if field_data:
                        st.dataframe(
                            pd.DataFrame(field_data),
                            hide_index=True,
                            use_container_width=True,
                        )

                with col2:
                    st.markdown("**Table Extraction Schema:**")
                    table_schema = cfg.get("table_extraction_schema")
                    if table_schema:
                        cols = table_schema.get("columns", [])
                        descs = table_schema.get("descriptions", [])
                        if cols:
                            tbl_data = []
                            for i, c in enumerate(cols):
                                tbl_data.append({
                                    "Column": c,
                                    "Description": descs[i] if i < len(descs) else "",
                                })
                            st.dataframe(pd.DataFrame(tbl_data), hide_index=True, use_container_width=True)
                    else:
                        st.info("No table extraction schema configured.")

                    st.markdown("**Validation Rules:**")
                    validation = cfg.get("validation_rules")
                    if validation:
                        st.json(validation)
                    else:
                        st.info("No validation rules configured.")

                # Toggle active status
                st.divider()
                is_active = cfg.get("active", True)
                new_active = st.checkbox(
                    f"Active (currently {'enabled' if is_active else 'disabled'})",
                    value=is_active,
                    key=f"active_{selected_type}",
                )
                if new_active != is_active:
                    session.sql(
                        f"UPDATE {DB}.DOCUMENT_TYPE_CONFIG SET active = ?, updated_at = CURRENT_TIMESTAMP() WHERE doc_type = ?",
                        params=[new_active, selected_type],
                    ).collect()
                    st.success(f"Updated {selected_type} active = {new_active}")
                    st.rerun()

            with tab_actions:
                st.markdown("**Re-Extract Documents**")
                st.caption(
                    "Clear all extracted data for this document type and re-run extraction "
                    "with the current prompt. Use after changing extraction prompts."
                )
                col_re1, col_re2 = st.columns([1, 3])
                with col_re1:
                    if st.button(f"Re-Extract {selected_type}", type="secondary"):
                        with st.spinner(f"Re-extracting {selected_type} documents..."):
                            try:
                                result = session.sql(
                                    f"CALL {DB}.SP_REEXTRACT_DOC_TYPE(?)",
                                    params=[selected_type],
                                ).collect()
                                st.success(result[0][0] if result else "Done")
                            except Exception as e:
                                st.error(f"Re-extraction failed: {e}")

                st.divider()
                st.markdown("**Test Extraction & Save**")
                st.caption(
                    "Upload a document to test extraction. Review the results, "
                    "then choose to **save** (persist to database) or **discard**."
                )
                st.warning(
                    "**Duplicate handling:** Documents are identified by **filename only**. "
                    "Uploading a file with the same name as an existing document will **overwrite** "
                    "the stage file and **update** the extraction results (MERGE). "
                    "Review corrections linked to that document are preserved. "
                    "To add a truly new document, ensure the filename is unique."
                )

                # ── State cleanup: if doc type changed, clear stale test state ──
                prev_type = st.session_state.get('test_ext_doc_type')
                if prev_type and prev_type != selected_type:
                    _cleanup_test_state(session)

                test_file = st.file_uploader(
                    "Upload a test document (PDF)",
                    type=["pdf"],
                    key=f"test_upload_{selected_type}",
                )

                # ── Phase 1: Run Extraction ─────────────────────────────────
                has_result = 'test_ext_result' in st.session_state and \
                             st.session_state.get('test_ext_doc_type') == selected_type
                already_saved = st.session_state.get('test_ext_saved', False)

                if test_file and not has_result and not already_saved:
                    if st.button("Run Test Extraction", type="primary"):
                        with st.spinner("Uploading and running AI_EXTRACT..."):
                            try:
                                test_fname = _sanitize_filename(test_file.name)
                                test_file.seek(0)
                                tmp_dir = tempfile.mkdtemp()
                                local_path = os.path.join(tmp_dir, test_fname)
                                with open(local_path, 'wb') as f:
                                    f.write(test_file.read())
                                session.file.put(
                                    local_path, f"@{DB}.DOCUMENT_STAGE",
                                    auto_compress=False, overwrite=True,
                                )
                                shutil.rmtree(tmp_dir, ignore_errors=True)

                                prompt = cfg.get("extraction_prompt", "")
                                fnames = _parse_field_names(prompt) if prompt else []
                                if not fnames:
                                    st.error("Could not parse field names from extraction prompt.")
                                else:
                                    prompt_parts = []
                                    for fn in fnames:
                                        lbl = fn.replace('_', ' ').title()
                                        prompt_parts.append(
                                            f"'{fn}': 'What is the {lbl.lower()}?'"
                                        )
                                    prompt_obj = '{' + ', '.join(prompt_parts) + '}'

                                    safe_fname_sql = _sql_escape(test_fname)
                                    result = session.sql(f"""
                                        SELECT AI_EXTRACT(
                                            TO_FILE('@{DB}.DOCUMENT_STAGE', '{safe_fname_sql}'),
                                            {prompt_obj}
                                        ) AS extraction
                                    """).collect()

                                    if result:
                                        ext = result[0]['EXTRACTION']
                                        ext_json = json.loads(ext) if isinstance(ext, str) else ext
                                        response = ext_json.get('response', ext_json)

                                        # Store in session_state for Phase 2
                                        st.session_state['test_ext_result'] = response
                                        st.session_state['test_ext_fname'] = test_fname
                                        st.session_state['test_ext_field_names'] = fnames
                                        st.session_state['test_ext_doc_type'] = selected_type
                                        st.session_state['test_ext_cfg'] = cfg
                                        st.session_state['test_ext_saved'] = False
                                        st.rerun()
                                    else:
                                        st.warning("No extraction result returned.")
                                        session.sql(
                                            f"REMOVE @{DB}.DOCUMENT_STAGE/{_sanitize_filename(test_fname)}"
                                        ).collect()
                            except Exception as e:
                                st.error(f"Test extraction failed: {e}")

                # ── Phase 2: Display Results + Validation + Save/Discard ─────
                if has_result and not already_saved:
                    response = st.session_state['test_ext_result']
                    test_fname = st.session_state['test_ext_fname']
                    fnames = st.session_state['test_ext_field_names']
                    test_cfg = st.session_state['test_ext_cfg']

                    review_fields = test_cfg.get("review_fields") or {}
                    field_types = review_fields.get("types", {})

                    st.success(
                        f"Extraction complete for `{test_fname}` "
                        f"using **{test_cfg.get('doc_type', 'UNKNOWN')}** configuration"
                    )

                    # ── Raw extraction results ──
                    with st.expander("Raw Extraction Results", expanded=True):
                        st.json(response)

                    # ── Validation & Preview panel ──
                    with st.expander("Validation & Field Mapping Preview", expanded=True):
                        # Build validation data
                        mapping_rows = []
                        for i, fn in enumerate(fnames):
                            raw_val = response.get(fn)
                            ftype = field_types.get(fn, 'VARCHAR')
                            normalized = _normalize_value(raw_val, ftype)
                            col_idx = i + 1
                            target_col = f"field_{col_idx}" if col_idx <= 10 else "raw_extraction (overflow)"
                            phys_type = TABLE_COL_TYPES.get(col_idx, 'VARCHAR')
                            confidence = _compute_confidence(normalized, ftype)

                            # Predict cast result
                            if phys_type == 'DATE' and normalized:
                                cast_ok = bool(re.match(r'\d{4}-\d{2}-\d{2}$', str(normalized)))
                                cast_note = normalized if cast_ok else f"NULL (bad format: {normalized})"
                            elif phys_type == 'NUMBER' and normalized:
                                try:
                                    float(str(normalized))
                                    cast_note = normalized
                                    cast_ok = True
                                except (ValueError, TypeError):
                                    cast_note = f"NULL (non-numeric: {normalized})"
                                    cast_ok = False
                            else:
                                cast_note = str(normalized) if normalized else "(NULL)"
                                cast_ok = True

                            # Confidence badge
                            if confidence >= 0.8:
                                conf_display = f"HIGH ({confidence})"
                            elif confidence >= 0.5:
                                conf_display = f"MED ({confidence})"
                            else:
                                conf_display = f"LOW ({confidence})"

                            mapping_rows.append({
                                "Column": target_col,
                                "Field": fn,
                                "Raw Value": str(raw_val) if raw_val is not None else "(null)",
                                "Normalized": str(normalized) if normalized is not None else "(null)",
                                "DB Type": phys_type,
                                "Cast Preview": cast_note,
                                "Confidence": conf_display,
                            })

                        st.dataframe(
                            pd.DataFrame(mapping_rows),
                            hide_index=True,
                            use_container_width=True,
                        )

                        # Cast warnings
                        cast_warnings = [
                            r for r in mapping_rows
                            if "NULL" in str(r.get("Cast Preview", "")) and r["Column"] != "raw_extraction (overflow)"
                        ]
                        if cast_warnings:
                            st.warning(
                                f"{len(cast_warnings)} field(s) will cast to NULL due to type mismatch: "
                                + ", ".join(r["Field"] for r in cast_warnings)
                            )

                        # Duplicate check
                        dup_check = session.sql(
                            f"SELECT COUNT(*) AS cnt FROM {DB}.RAW_DOCUMENTS WHERE file_name = ?",
                            params=[test_fname],
                        ).collect()
                        if dup_check and dup_check[0]['CNT'] > 0:
                            st.error(
                                f"⚠️ **DUPLICATE DETECTED:** A document named `{test_fname}` "
                                "already exists. Clicking **Save** will **overwrite** the "
                                "existing extraction results (MERGE). The stage file will be "
                                "replaced. Review corrections linked to this document are "
                                "preserved. If this is a different document, **rename the file** "
                                "before uploading."
                            )
                        else:
                            st.success(f"No duplicate found — `{test_fname}` is a new document.")

                    # ── Table/Line-Item Preview ──
                    table_schema = test_cfg.get("table_extraction_schema")
                    if table_schema:
                        with st.expander("Table/Line-Item Preview", expanded=True):
                            tbl_cols = table_schema.get("columns", [])
                            tbl_descs = table_schema.get("descriptions", [])
                            if tbl_cols:
                                # Run table extraction (cached in session_state)
                                if 'test_ext_table_data' not in st.session_state:
                                    with st.spinner("Extracting line items..."):
                                        try:
                                            props = {}
                                            for ti, col in enumerate(tbl_cols):
                                                desc = tbl_descs[ti] if ti < len(tbl_descs) else col
                                                props[col] = {'description': desc, 'type': 'array'}
                                            response_format = {
                                                'schema': {
                                                    'type': 'object',
                                                    'properties': {
                                                        'line_items': {
                                                            'description': 'The table of line items',
                                                            'type': 'object',
                                                            'column_ordering': tbl_cols,
                                                            'properties': props,
                                                        }
                                                    }
                                                }
                                            }
                                            rf_sql = json.dumps(response_format)
                                            safe_fname_sql = _sql_escape(test_fname)
                                            safe_rf_sql = _sql_escape(rf_sql)
                                            tbl_result = session.sql(f"""
                                                SELECT AI_EXTRACT(
                                                    file => TO_FILE('@{DB}.DOCUMENT_STAGE', '{safe_fname_sql}'),
                                                    responseFormat => PARSE_JSON('{safe_rf_sql}')
                                                ) AS extraction
                                            """).collect()
                                            if tbl_result:
                                                tbl_ext = tbl_result[0]['EXTRACTION']
                                                # Handle double-encoded strings
                                                if isinstance(tbl_ext, str):
                                                    tbl_json = json.loads(tbl_ext)
                                                    if isinstance(tbl_json, str):
                                                        tbl_json = json.loads(tbl_json)
                                                else:
                                                    tbl_json = tbl_ext
                                                tbl_resp = tbl_json.get('response', tbl_json)
                                                line_items = tbl_resp.get('line_items', {})
                                                st.session_state['test_ext_table_data'] = line_items
                                                st.session_state['test_ext_table_debug'] = str(tbl_ext)[:2000]
                                            else:
                                                st.session_state['test_ext_table_data'] = {}
                                        except Exception as e:
                                            st.warning(f"Table extraction failed: {e}")
                                            st.session_state['test_ext_table_data'] = {}

                                line_items = st.session_state.get('test_ext_table_data', {})
                                if line_items:
                                    # Build a dataframe from columnar arrays
                                    first_col_data = line_items.get(tbl_cols[0], [])
                                    num_rows = len(first_col_data) if isinstance(first_col_data, list) else 0
                                    if num_rows > 0:
                                        table_rows = []
                                        for row_idx in range(num_rows):
                                            row = {}
                                            for col in tbl_cols:
                                                arr = line_items.get(col, [])
                                                row[col] = arr[row_idx] if row_idx < len(arr) else None
                                            table_rows.append(row)
                                        st.dataframe(
                                            pd.DataFrame(table_rows),
                                            hide_index=True,
                                            use_container_width=True,
                                        )
                                        st.caption(f"{num_rows} line item(s) found")
                                    else:
                                        st.info("No line items found in this document.")
                                else:
                                    st.info("No line items extracted.")
                                # Debug: show raw AI_EXTRACT response
                                debug_raw = st.session_state.get('test_ext_table_debug')
                                if debug_raw:
                                    with st.expander("Debug: Raw Table Extraction Response"):
                                        st.code(debug_raw, language="json")

                    # ── Save / Discard buttons ──
                    st.divider()
                    col_save, col_discard, _ = st.columns([1, 1, 2])

                    with col_save:
                        if st.button("Save Document & Results", type="primary"):
                            with st.spinner("Saving to database..."):
                                try:
                                    # 0. Upsert into RAW_DOCUMENTS
                                    dup_exists = (dup_check and dup_check[0]['CNT'] > 0)
                                    if not dup_exists:
                                        session.sql(
                                            f"INSERT INTO {DB}.RAW_DOCUMENTS "
                                            "(file_name, file_path, doc_type, staged_at, extracted, extracted_at) "
                                            "SELECT ?, ?, ?, CURRENT_TIMESTAMP(), TRUE, CURRENT_TIMESTAMP()",
                                            params=[
                                                test_fname,
                                                f"@{STAGE}/{test_fname}",
                                                selected_type,
                                            ],
                                        ).collect()
                                    else:
                                        session.sql(
                                            f"UPDATE {DB}.RAW_DOCUMENTS "
                                            "SET extracted = TRUE, extracted_at = CURRENT_TIMESTAMP(), "
                                            "    extraction_error = NULL "
                                            "WHERE file_name = ?",
                                            params=[test_fname],
                                        ).collect()

                                    # 1. Normalize all fields
                                    normalized_vals = {}
                                    for fn in fnames:
                                        ftype = field_types.get(fn, 'VARCHAR')
                                        normalized_vals[fn] = _normalize_value(response.get(fn), ftype)

                                    # 3. Build raw_extraction payload
                                    store_response = dict(normalized_vals)
                                    store_response['_confidence'] = {
                                        fn: _compute_confidence(normalized_vals[fn], field_types.get(fn, 'VARCHAR'))
                                        for fn in fnames
                                    }

                                    # 2. Map fields to field_1..field_10
                                    field_values = []
                                    for i, fn in enumerate(fnames[:10]):
                                        field_values.append(normalized_vals.get(fn))
                                    while len(field_values) < 10:
                                        field_values.append(None)

                                    # 3. MERGE into EXTRACTED_FIELDS (preserves record_id + review links)
                                    set_clauses = []
                                    insert_cols = ["file_name"]
                                    for i in range(10):
                                        col_type = TABLE_COL_TYPES.get(i + 1, 'VARCHAR')
                                        if col_type == 'DATE':
                                            cast_expr = f"TRY_TO_DATE(s.f{i+1}::VARCHAR)"
                                        elif col_type == 'NUMBER':
                                            cast_expr = f"TRY_TO_NUMBER(REGEXP_REPLACE(s.f{i+1}::VARCHAR, '[^0-9.]', ''), 12, 2)"
                                        else:
                                            cast_expr = f"s.f{i+1}"
                                        set_clauses.append(f"t.field_{i+1} = {cast_expr}")
                                        insert_cols.append(f"field_{i+1}")
                                    set_clauses.append("t.raw_extraction = PARSE_JSON(s.raw_ext)")
                                    insert_cols.append("raw_extraction")

                                    insert_vals = ["s.file_name"]
                                    for i in range(10):
                                        col_type = TABLE_COL_TYPES.get(i + 1, 'VARCHAR')
                                        if col_type == 'DATE':
                                            insert_vals.append(f"TRY_TO_DATE(s.f{i+1}::VARCHAR)")
                                        elif col_type == 'NUMBER':
                                            insert_vals.append(f"TRY_TO_NUMBER(REGEXP_REPLACE(s.f{i+1}::VARCHAR, '[^0-9.]', ''), 12, 2)")
                                        else:
                                            insert_vals.append(f"s.f{i+1}")
                                    insert_vals.append("PARSE_JSON(s.raw_ext)")

                                    source_cols = ["? AS file_name"]
                                    merge_params = [test_fname]
                                    for i in range(10):
                                        source_cols.append(f"? AS f{i+1}")
                                        merge_params.append(field_values[i])
                                    source_cols.append("? AS raw_ext")
                                    merge_params.append(json.dumps(store_response))

                                    merge_sql = (
                                        f"MERGE INTO {DB}.EXTRACTED_FIELDS t "
                                        f"USING (SELECT {', '.join(source_cols)}) s "
                                        "ON t.file_name = s.file_name "
                                        f"WHEN MATCHED THEN UPDATE SET {', '.join(set_clauses)} "
                                        f"WHEN NOT MATCHED THEN INSERT ({', '.join(insert_cols)}) "
                                        f"VALUES ({', '.join(insert_vals)})"
                                    )
                                    session.sql(merge_sql, params=merge_params).collect()

                                    # 3. Get the record_id (preserved for existing, new for fresh)
                                    rid_result = session.sql(
                                        f"SELECT record_id FROM {DB}.EXTRACTED_FIELDS "
                                        "WHERE file_name = ? ORDER BY record_id DESC LIMIT 1",
                                        params=[test_fname],
                                    ).collect()
                                    record_id = rid_result[0]['RECORD_ID'] if rid_result else None

                                    # 4. Table data: scoped DELETE + INSERT using cached preview data
                                    table_lines_saved = 0
                                    if record_id and table_schema:
                                        try:
                                            # Delete old line items for this record only
                                            session.sql(
                                                f"DELETE FROM {DB}.EXTRACTED_TABLE_DATA WHERE record_id = ?",
                                                params=[record_id],
                                            ).collect()

                                            # Reuse cached table data from preview (no extra AI_EXTRACT call)
                                            cached_line_items = st.session_state.get('test_ext_table_data', {})
                                            tbl_cols = table_schema.get('columns', [])
                                            if cached_line_items and tbl_cols:
                                                first_col_data = cached_line_items.get(tbl_cols[0], [])
                                                if isinstance(first_col_data, list):
                                                    for row_idx in range(len(first_col_data)):
                                                        col_values_tbl = []
                                                        raw_line = {}
                                                        for ci, col in enumerate(tbl_cols):
                                                            arr = cached_line_items.get(col, [])
                                                            val = arr[row_idx] if row_idx < len(arr) else None
                                                            raw_line[col] = str(val) if val is not None else None
                                                            if ci < 5:
                                                                col_values_tbl.append(str(val) if val is not None else None)
                                                        while len(col_values_tbl) < 5:
                                                            col_values_tbl.append(None)
                                                        session.sql(
                                                            f"INSERT INTO {DB}.EXTRACTED_TABLE_DATA "
                                                            "(file_name, record_id, line_number, col_1, col_2, col_3, col_4, col_5, raw_line_data) "
                                                            "SELECT ?, ?, ?, ?, ?, "
                                                            "TRY_TO_NUMBER(REGEXP_REPLACE(?::VARCHAR, '[^0-9.]', ''), 10, 2), "
                                                            "TRY_TO_NUMBER(REGEXP_REPLACE(?::VARCHAR, '[^0-9.]', ''), 10, 2), "
                                                            "TRY_TO_NUMBER(REGEXP_REPLACE(?::VARCHAR, '[^0-9.]', ''), 12, 2), "
                                                            "PARSE_JSON(?)",
                                                            params=[
                                                                test_fname, record_id, row_idx + 1,
                                                                col_values_tbl[0], col_values_tbl[1],
                                                                col_values_tbl[2], col_values_tbl[3], col_values_tbl[4],
                                                                json.dumps(raw_line),
                                                            ],
                                                        ).collect()
                                                        table_lines_saved += 1
                                        except Exception:
                                            pass  # table extraction is best-effort

                                    # 5. Refresh metadata
                                    try:
                                        session.sql(f"CALL {DB}.SP_POPULATE_DOC_METADATA()").collect()
                                    except Exception:
                                        pass  # non-critical

                                    # ── Post-Save Verification ──
                                    st.session_state['test_ext_saved'] = True

                                    st.success("Document and extraction results saved!")

                                    # Verify RAW_DOCUMENTS
                                    verify_raw = session.sql(
                                        f"SELECT file_name, doc_type, extracted, extracted_at "
                                        f"FROM {DB}.RAW_DOCUMENTS WHERE file_name = ?",
                                        params=[test_fname],
                                    ).collect()
                                    # Verify EXTRACTED_FIELDS
                                    verify_ext = session.sql(
                                        f"SELECT record_id, file_name, field_1, field_2, field_3, "
                                        f"field_4, field_5, field_6, field_7, field_8, field_9, field_10 "
                                        f"FROM {DB}.EXTRACTED_FIELDS WHERE file_name = ? "
                                        f"ORDER BY record_id DESC LIMIT 1",
                                        params=[test_fname],
                                    ).collect()
                                    # Verify table data
                                    verify_tbl = session.sql(
                                        f"SELECT COUNT(*) AS cnt FROM {DB}.EXTRACTED_TABLE_DATA "
                                        f"WHERE file_name = ?",
                                        params=[test_fname],
                                    ).collect()

                                    with st.expander("Post-Save Verification", expanded=True):
                                        chk1 = bool(verify_raw)
                                        chk2 = bool(verify_ext)
                                        tbl_cnt = verify_tbl[0]['CNT'] if verify_tbl else 0

                                        st.markdown(
                                            f"{'OK' if chk1 else 'FAIL'} **RAW_DOCUMENTS**: "
                                            f"{'Record found, extracted=TRUE' if chk1 else 'Record NOT found!'}"
                                        )
                                        st.markdown(
                                            f"{'OK' if chk2 else 'FAIL'} **EXTRACTED_FIELDS**: "
                                            f"{'Record saved (record_id=' + str(verify_ext[0]['RECORD_ID']) + ')' if chk2 else 'Record NOT found!'}"
                                        )
                                        st.markdown(
                                            f"{'OK' if tbl_cnt > 0 or not table_schema else 'SKIP'} "
                                            f"**EXTRACTED_TABLE_DATA**: "
                                            f"{tbl_cnt} line(s) saved"
                                        )

                                        if chk2:
                                            st.markdown("**Saved field values:**")
                                            saved_row = verify_ext[0]
                                            saved_data = []
                                            for i, fn in enumerate(fnames[:10]):
                                                col_key = f"FIELD_{i+1}"
                                                saved_data.append({
                                                    "Column": f"field_{i+1}",
                                                    "Field": fn,
                                                    "Saved Value": str(saved_row[col_key]) if saved_row[col_key] is not None else "(NULL)",
                                                })
                                            st.dataframe(
                                                pd.DataFrame(saved_data),
                                                hide_index=True,
                                                use_container_width=True,
                                            )

                                except Exception as e:
                                    st.error(f"Save failed: {e}")

                    with col_discard:
                        if st.button("Discard", type="secondary"):
                            _cleanup_test_state(session)
                            st.info("Test document discarded.")
                            st.rerun()

                # ── After save: show summary and reset option ────────────
                if already_saved:
                    st.success(
                        f"Document `{st.session_state.get('test_ext_fname', '')}` "
                        f"saved as **{selected_type}**."
                    )
                    if st.button("Test Another Document"):
                        # Clear state without removing the saved file from stage
                        for key in ('test_ext_result', 'test_ext_fname', 'test_ext_field_names',
                                    'test_ext_doc_type', 'test_ext_cfg', 'test_ext_saved',
                                    'test_ext_table_data'):
                            st.session_state.pop(key, None)
                        st.rerun()

else:
    st.info("No document types configured. Add one below.")

# ── Add New Document Type (Guided Builder) ────────────────────────────────────
st.divider()
st.subheader("Add New Document Type")
st.caption("Define your fields using the builder below — prompts and config are auto-generated")

with st.form("add_doc_type", clear_on_submit=True):
    col_basic1, col_basic2 = st.columns(2)
    with col_basic1:
        new_doc_type = st.text_input(
            "Document Type Code",
            placeholder="UTILITY_BILL",
            help="Uppercase with underscores, e.g. PURCHASE_ORDER",
        )
    with col_basic2:
        new_display_name = st.text_input(
            "Display Name",
            placeholder="Utility Bill",
            help="Human-readable name shown in the UI",
        )

    st.markdown("---")
    st.markdown("**Entity Fields** — define the data fields to extract from this document type")

    # Dynamic field builder (up to 15 fields)
    fields = []
    for i in range(15):
        col_name, col_label, col_type, col_corr = st.columns([2, 2, 1, 1])
        with col_name:
            fname = st.text_input(
                f"Field {i+1} name",
                key=f"fn_{i}",
                placeholder="e.g. vendor_name" if i == 0 else "",
                label_visibility="collapsed" if i > 0 else "visible",
            )
        with col_label:
            flabel = st.text_input(
                f"Field {i+1} label",
                key=f"fl_{i}",
                placeholder="e.g. Vendor Name" if i == 0 else "",
                label_visibility="collapsed" if i > 0 else "visible",
            )
        with col_type:
            ftype = st.selectbox(
                f"Type {i+1}",
                FIELD_TYPE_OPTIONS,
                key=f"ft_{i}",
                label_visibility="collapsed" if i > 0 else "visible",
            )
        with col_corr:
            fcorr = st.checkbox(
                "Editable",
                value=True,
                key=f"fc_{i}",
                label_visibility="collapsed" if i > 0 else "visible",
            )

        if fname and fname.strip():
            fields.append({
                "name": fname.strip().lower().replace(" ", "_"),
                "label": flabel.strip() if flabel else fname.strip().replace("_", " ").title(),
                "type": ftype,
                "correctable": fcorr,
            })

    st.markdown("---")
    st.markdown("**Table/Line-Item Columns** (optional) — for documents with tabular data")

    table_columns = []
    for i in range(6):
        tc_name, tc_desc = st.columns(2)
        with tc_name:
            tcn = st.text_input(
                f"Column {i+1}",
                key=f"tc_{i}",
                placeholder="e.g. Description" if i == 0 else "",
                label_visibility="collapsed" if i > 0 else "visible",
            )
        with tc_desc:
            tcd = st.text_input(
                f"Description {i+1}",
                key=f"td_{i}",
                placeholder="e.g. Product or service name" if i == 0 else "",
                label_visibility="collapsed" if i > 0 else "visible",
            )
        if tcn and tcn.strip():
            table_columns.append({
                "name": tcn.strip(),
                "description": tcd.strip() if tcd else tcn.strip(),
            })

    submitted = st.form_submit_button("Add Document Type", type="primary")

    if submitted:
        errors = []
        if not new_doc_type or not new_doc_type.strip():
            errors.append("Document Type Code is required.")
        if not new_display_name or not new_display_name.strip():
            errors.append("Display Name is required.")
        if not fields:
            errors.append("At least one field is required.")

        if errors:
            for err in errors:
                st.error(err)
        else:
            doc_type_clean = new_doc_type.strip().upper().replace(" ", "_")

            # Check for duplicates
            existing = get_doc_type_config(session, doc_type_clean)
            if existing:
                st.error(f"Document type '{doc_type_clean}' already exists.")
            else:
                prompt, field_labels, review_fields, table_schema = _build_config_from_fields(
                    doc_type_clean, new_display_name.strip(), fields,
                    table_columns if table_columns else None,
                )

                session.sql(
                    f"""
                    INSERT INTO {DB}.DOCUMENT_TYPE_CONFIG (
                        doc_type, display_name, extraction_prompt,
                        field_labels, table_extraction_schema, review_fields
                    ) SELECT ?, ?, ?, PARSE_JSON(?), PARSE_JSON(?), PARSE_JSON(?)
                    """,
                    params=[
                        doc_type_clean,
                        new_display_name.strip(),
                        prompt,
                        json.dumps(field_labels),
                        json.dumps(table_schema) if table_schema else None,
                        json.dumps(review_fields),
                    ],
                ).collect()

                st.success(f"Added document type: {doc_type_clean}")

                # Show generated config for transparency
                with st.expander("Generated Configuration (for reference)"):
                    st.markdown("**Extraction Prompt:**")
                    st.code(prompt, language="text")
                    st.markdown("**Field Labels:**")
                    st.json(field_labels)
                    st.markdown("**Review Fields:**")
                    st.json(review_fields)
                    if table_schema:
                        st.markdown("**Table Schema:**")
                        st.json(table_schema)

                st.rerun()
