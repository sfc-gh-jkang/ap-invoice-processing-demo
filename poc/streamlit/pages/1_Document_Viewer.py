"""
Page 1: Document Viewer — Browse documents, view extracted fields + source PDF.

Dynamically renders fields from raw_extraction VARIANT when available,
falling back to fixed field_1..field_10 columns for backward compatibility.
Line items are editable with append-only audit trail via LINE_ITEM_REVIEW.
"""

import json
import streamlit as st
import pandas as pd
import tempfile
import os
import pypdfium2 as pdfium
from config import (
    DB, STAGE, get_session, get_doc_type_labels, get_doc_types,
    get_doc_type_config, get_field_names_from_labels, _parse_variant,
    inject_custom_css, sidebar_branding,
)

st.set_page_config(page_title="Document Viewer", page_icon="📋", layout="wide")

inject_custom_css()
with st.sidebar:
    sidebar_branding()

session = get_session()

if "line_save_result" not in st.session_state:
    st.session_state.line_save_result = None

st.title("Document Viewer")
st.caption("Browse extracted documents — select any row to view source PDF and extracted fields")


# ---------------------------------------------------------------------------
# Helper: Render fields dynamically from raw_extraction VARIANT
# ---------------------------------------------------------------------------
def _label_for_key(key: str, doc_labels: dict) -> str:
    """Look up a display label for a raw_extraction field key."""
    for lbl_key, lbl_val in doc_labels.items():
        if lbl_val and key.lower().replace("_", " ") == lbl_val.lower().replace("_", " "):
            return lbl_val
    return key.replace("_", " ").title()


def _render_dynamic_fields(raw: dict, doc_labels: dict):
    """Render all fields from raw_extraction JSON using config labels."""
    field_keys = list(raw.keys())
    mid = (len(field_keys) + 1) // 2
    left_keys = field_keys[:mid]
    right_keys = field_keys[mid:]

    h1, h2 = st.columns(2)
    with h1:
        for key in left_keys:
            label = _label_for_key(key, doc_labels)
            val = raw.get(key)
            st.markdown(f"**{label}:** {val or 'N/A'}")
    with h2:
        for key in right_keys:
            label = _label_for_key(key, doc_labels)
            val = raw.get(key)
            st.markdown(f"**{label}:** {val or 'N/A'}")


# --- Filters ---
col_f0, col_f1, col_f2 = st.columns(3)

doc_types = get_doc_types(session)

with col_f0:
    selected_type = st.selectbox("Document Type", ["ALL"] + doc_types, index=0)

effective_type = selected_type if selected_type != "ALL" else "INVOICE"
labels = get_doc_type_labels(session, effective_type)
config = get_doc_type_config(session, effective_type)

# Filter sender list by selected doc_type
sender_type_clause = ""
sender_type_params = []
if selected_type != "ALL":
    sender_type_clause = "AND rd.doc_type = ?"
    sender_type_params = [selected_type]

senders = session.sql(
    f"""
    SELECT DISTINCT ef.field_1 AS sender
    FROM {DB}.EXTRACTED_FIELDS ef
        JOIN {DB}.RAW_DOCUMENTS rd ON ef.file_name = rd.file_name
    WHERE ef.field_1 IS NOT NULL {sender_type_clause}
    ORDER BY sender
    """,
    params=sender_type_params,
).to_pandas()
sender_list = ["ALL"] + senders["SENDER"].tolist()

with col_f1:
    selected_sender = st.selectbox(labels.get("sender_label", "Sender / Vendor"), sender_list)

with col_f2:
    selected_status = st.selectbox("Status", ["ALL", "EXTRACTED"])

# --- Build filtered query ---

where_clauses = []
params = []
if selected_type != "ALL":
    where_clauses.append("rd.doc_type = ?")
    params.append(selected_type)
if selected_sender != "ALL":
    where_clauses.append("ef.field_1 = ?")
    params.append(selected_sender)
if selected_status != "ALL":
    where_clauses.append("ef.status = ?")
    params.append(selected_status)

where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

ledger_query = f"""
    SELECT
        ef.record_id,
        ef.file_name,
        ef.field_1       AS sender,
        ef.field_2       AS document_number,
        ef.field_4       AS document_date,
        ef.field_5       AS due_date,
        ef.field_6       AS terms,
        ef.field_8       AS subtotal,
        ef.field_9       AS tax,
        ef.field_10      AS total_amount,
        ef.status
    FROM {DB}.EXTRACTED_FIELDS ef
        JOIN {DB}.RAW_DOCUMENTS rd ON ef.file_name = rd.file_name
    {where_sql}
    ORDER BY ef.field_4 DESC NULLS LAST
"""

ledger_df = session.sql(ledger_query, params=params).to_pandas()

# --- Document list ---
st.subheader(f"Documents ({len(ledger_df)} results)")

if len(ledger_df) > 0:
    # --- Aging cards (only if due_date fields exist) ---
    aging_type_clause = ""
    aging_params = []
    if selected_type != "ALL":
        aging_type_clause = "AND doc_type = ?"
        aging_params = [selected_type]

    aging_data = session.sql(
        f"""
        SELECT
            aging_bucket,
            COUNT(*)              AS document_count,
            SUM(total_amount)     AS total_amount,
            sort_order
        FROM (
            SELECT
                total_amount,
                doc_type,
                CASE
                    WHEN due_date IS NULL              THEN 'N/A'
                    WHEN due_date >= CURRENT_DATE()    THEN 'Current'
                    WHEN DATEDIFF('day', due_date, CURRENT_DATE()) <= 30  THEN '1-30 Days'
                    WHEN DATEDIFF('day', due_date, CURRENT_DATE()) <= 60  THEN '31-60 Days'
                    WHEN DATEDIFF('day', due_date, CURRENT_DATE()) <= 90  THEN '61-90 Days'
                    ELSE '90+ Days'
                END AS aging_bucket,
                CASE
                    WHEN due_date IS NULL              THEN 99
                    WHEN due_date >= CURRENT_DATE()    THEN 0
                    WHEN DATEDIFF('day', due_date, CURRENT_DATE()) <= 30  THEN 1
                    WHEN DATEDIFF('day', due_date, CURRENT_DATE()) <= 60  THEN 2
                    WHEN DATEDIFF('day', due_date, CURRENT_DATE()) <= 90  THEN 3
                    ELSE 4
                END AS sort_order
            FROM {DB}.V_DOCUMENT_LEDGER
        ) sub
        WHERE 1=1 {aging_type_clause}
        GROUP BY aging_bucket, sort_order
        ORDER BY sort_order
        """,
        params=aging_params,
    ).to_pandas()
    if len(aging_data) > 0:
        aging_cols = st.columns(len(aging_data))
        for i, row in aging_data.iterrows():
            with aging_cols[i]:
                label = row["AGING_BUCKET"]
                count = int(row["DOCUMENT_COUNT"])
                amount = row["TOTAL_AMOUNT"]
                if amount and amount > 0:
                    st.metric(label=label, value=f"${amount:,.0f}", delta=f"{count} docs")
                else:
                    st.metric(label=label, value=f"{count} docs")
        st.divider()

    st.dataframe(
        ledger_df,
        column_config={
            "RECORD_ID": None,
            "FILE_NAME": None,
            "SENDER": labels.get("sender_label", "Sender"),
            "DOCUMENT_NUMBER": labels.get("reference_label", "Document #"),
            "DOCUMENT_DATE": st.column_config.DateColumn(labels.get("date_label", "Date")),
            "DUE_DATE": st.column_config.DateColumn("Due Date"),
            "TERMS": "Terms",
            "SUBTOTAL": st.column_config.NumberColumn("Subtotal", format="$%.2f"),
            "TAX": st.column_config.NumberColumn("Tax", format="$%.2f"),
            "TOTAL_AMOUNT": st.column_config.NumberColumn(labels.get("amount_label", "Total"), format="$%.2f"),
            "STATUS": "Status",
        },
        hide_index=True,
        use_container_width=True,
    )

    # --- Drill-down: select a document ---
    st.divider()
    st.subheader("Document Detail")

    doc_options = ledger_df["DOCUMENT_NUMBER"].dropna().tolist()
    if not doc_options:
        doc_options = ledger_df["FILE_NAME"].tolist()

    selected_doc = st.selectbox("Select a document to view details", doc_options)

    if selected_doc:
        # Get the full record including raw_extraction VARIANT
        file_row = session.sql(
            f"""
            SELECT ef.file_name, ef.record_id, rd.doc_type,
                   ef.field_1, ef.field_2, ef.field_3, ef.field_4, ef.field_5,
                   ef.field_6, ef.field_7, ef.field_8, ef.field_9, ef.field_10,
                   ef.raw_extraction
            FROM {DB}.EXTRACTED_FIELDS ef
                JOIN {DB}.RAW_DOCUMENTS rd ON ef.file_name = rd.file_name
            WHERE ef.field_2 = ? OR ef.file_name = ?
            LIMIT 1
            """,
            params=[selected_doc, selected_doc],
        ).to_pandas()

        col_pdf, col_fields = st.columns([1, 1])

        # Left: Source PDF
        with col_pdf:
            st.markdown("**Source Document**")
            if len(file_row) > 0 and file_row.iloc[0]["FILE_NAME"]:
                file_name = file_row.iloc[0]["FILE_NAME"]
                try:
                    with tempfile.TemporaryDirectory() as tmpdir:
                        stage_path = f"@{STAGE}/{file_name}"
                        session.file.get(stage_path, tmpdir)
                        local_path = os.path.join(tmpdir, file_name)
                        pdf = pdfium.PdfDocument(local_path)
                        for page_idx in range(len(pdf)):
                            page = pdf[page_idx]
                            bitmap = page.render(scale=2)
                            pil_image = bitmap.to_pil()
                            st.image(pil_image, use_container_width=True)
                        pdf.close()
                except Exception as e:
                    st.warning(f"Could not render document: {e}")
            else:
                st.info("No source file available.")

        # Right: Extracted fields + line items
        with col_fields:
            st.markdown("**Extracted Fields**")
            if len(file_row) > 0:
                inv = file_row.iloc[0]
                doc_type = inv.get("DOC_TYPE", "INVOICE")
                doc_labels = get_doc_type_labels(session, doc_type)

                # Try to use raw_extraction VARIANT for dynamic fields
                raw = _parse_variant(inv.get("RAW_EXTRACTION")) or {}

                if raw and len(raw) > 10:
                    # Dynamic rendering: raw_extraction has more fields than fixed columns
                    _render_dynamic_fields(raw, doc_labels)
                else:
                    # Fallback: render fixed columns (backward compat)
                    h1, h2 = st.columns(2)
                    with h1:
                        st.markdown(f"**{doc_labels.get('field_1', 'Sender')}:** {inv['FIELD_1'] or 'N/A'}")
                        st.markdown(f"**{doc_labels.get('field_2', 'Document #')}:** {inv['FIELD_2'] or 'N/A'}")
                        st.markdown(f"**{doc_labels.get('field_3', 'Reference')}:** {inv['FIELD_3'] or 'N/A'}")
                        st.markdown(f"**{doc_labels.get('field_7', 'Recipient')}:** {inv['FIELD_7'] or 'N/A'}")
                    with h2:
                        st.markdown(f"**{doc_labels.get('field_4', 'Date')}:** {inv['FIELD_4'] or 'N/A'}")
                        st.markdown(f"**{doc_labels.get('field_5', 'Due Date')}:** {inv['FIELD_5'] or 'N/A'}")
                        st.markdown(f"**{doc_labels.get('field_6', 'Terms')}:** {inv['FIELD_6'] or 'N/A'}")

                    t1, t2, t3 = st.columns(3)
                    subtotal = inv["FIELD_8"] or 0
                    tax = inv["FIELD_9"] or 0
                    total = inv["FIELD_10"] or 0
                    t1.metric(doc_labels.get("field_8", "Subtotal"), f"${subtotal:,.2f}")
                    t2.metric(doc_labels.get("field_9", "Tax"), f"${tax:,.2f}")
                    t3.metric(doc_labels.get("field_10", "Total"), f"${total:,.2f}")

        st.divider()
        st.markdown("**Extracted Line Items**")

        file_name_for_lines = str(file_row.iloc[0]["FILE_NAME"]) if len(file_row) > 0 else ""

        if st.session_state.line_save_result and st.session_state.line_save_result.get("file") == file_name_for_lines:
            result = st.session_state.line_save_result
            st.success(f"Saved {result['count']} line item correction(s) — audit rows appended to LINE_ITEM_REVIEW")
            if st.button("Continue Editing"):
                st.session_state.line_save_result = None
                st.rerun()

        line_items = session.sql(
            f"""
            SELECT
                line_id,
                line_number,
                description,
                category,
                quantity,
                unit_price,
                line_total
            FROM {DB}.V_LINE_ITEM_DETAIL
            WHERE file_name = ?
            ORDER BY line_number
            """,
            params=[file_name_for_lines],
        ).to_pandas()

        if len(line_items) > 0:
            line_filter_key = f"lines|{file_name_for_lines}"
            if "line_orig_key" not in st.session_state or st.session_state.line_orig_key != line_filter_key:
                st.session_state.line_orig_snapshot = line_items.copy()
                st.session_state.line_orig_key = line_filter_key
            line_orig = st.session_state.line_orig_snapshot

            edited_lines = st.data_editor(
                line_items,
                column_config={
                    "LINE_ID": None,
                    "LINE_NUMBER": st.column_config.NumberColumn("#", disabled=True),
                    "DESCRIPTION": st.column_config.TextColumn("Description"),
                    "CATEGORY": st.column_config.TextColumn("Category"),
                    "QUANTITY": st.column_config.NumberColumn("Qty", format="%.0f"),
                    "UNIT_PRICE": st.column_config.NumberColumn("Unit Price", format="$%.2f"),
                    "LINE_TOTAL": st.column_config.NumberColumn("Total", format="$%.2f"),
                },
                hide_index=True,
                use_container_width=True,
                num_rows="fixed",
                key=f"line_editor_{file_name_for_lines}",
            )
            for c in edited_lines.columns:
                edited_lines[c] = edited_lines[c].apply(
                    lambda v: v.item() if hasattr(v, "item") else v
                )

            def _lnorm(val):
                if val is None:
                    return ""
                if isinstance(val, float) and pd.isna(val):
                    return ""
                return str(val).strip()

            line_changes = []
            for idx in range(min(len(line_orig), len(edited_lines))):
                orig = line_orig.iloc[idx]
                edit = edited_lines.iloc[idx]
                row_diffs = {}
                for col in ["DESCRIPTION", "CATEGORY", "QUANTITY", "UNIT_PRICE", "LINE_TOTAL"]:
                    if _lnorm(orig.get(col)) != _lnorm(edit.get(col)):
                        row_diffs[col] = (_lnorm(orig.get(col)), _lnorm(edit.get(col)))
                if row_diffs:
                    line_changes.append({"idx": idx, "line_id": int(edit["LINE_ID"]), "diffs": row_diffs})

            st.divider()

            if line_changes:
                st.warning(f"**{len(line_changes)} line item(s) with unsaved changes**")

                change_rows = []
                for ch in line_changes:
                    for col, (was, now) in ch["diffs"].items():
                        ln_val = edited_lines.iloc[ch["idx"]]["LINE_NUMBER"]
                        change_rows.append({
                            "Line #": int(ln_val) if pd.notna(ln_val) else ch["idx"] + 1,
                            "Field": col.replace("_", " ").title(),
                            "Was": was if was else "(empty)",
                            "Now": now if now else "(empty)",
                        })
                st.dataframe(pd.DataFrame(change_rows), hide_index=True, use_container_width=True)

                if st.button(f"Save {len(line_changes)} Line Item Change(s)", type="primary"):
                    import numpy as np

                    def _to_native(v):
                        if v is None:
                            return None
                        if isinstance(v, (np.integer,)):
                            return int(v.item())
                        if isinstance(v, (np.floating,)):
                            return float(v.item())
                        if isinstance(v, (np.str_, np.bytes_)):
                            return str(v)
                        if isinstance(v, np.bool_):
                            return bool(v.item())
                        return v

                    def _safe_str(v):
                        if v is None:
                            return None
                        if isinstance(v, float) and pd.isna(v):
                            return None
                        s = str(v).strip()
                        return s if s else None

                    def _safe_num(v):
                        if v is None:
                            return None
                        if isinstance(v, float) and pd.isna(v):
                            return None
                        try:
                            return float(v)
                        except (ValueError, TypeError):
                            return None

                    COL_MAP = {
                        "DESCRIPTION": "col_1",
                        "CATEGORY": "col_2",
                        "QUANTITY": "col_3",
                        "UNIT_PRICE": "col_4",
                        "LINE_TOTAL": "col_5",
                    }

                    validation_errors = []
                    for ch in line_changes:
                        row = edited_lines.iloc[ch["idx"]]
                        ln = int(row["LINE_NUMBER"]) if pd.notna(row["LINE_NUMBER"]) else ch["idx"] + 1
                        for col in ["QUANTITY", "UNIT_PRICE", "LINE_TOTAL"]:
                            raw_val = row.get(col)
                            if raw_val is not None and not (isinstance(raw_val, float) and pd.isna(raw_val)):
                                try:
                                    float(raw_val)
                                except (ValueError, TypeError):
                                    validation_errors.append(f"Line #{ln} — {col.replace('_', ' ').title()}: '{raw_val}' is not a valid number")

                    if validation_errors:
                        st.error("**Validation failed — changes not saved:**")
                        for err in validation_errors:
                            st.markdown(f"- {err}")
                        st.stop()

                    saved = 0
                    for ch in line_changes:
                        row = edited_lines.iloc[ch["idx"]]
                        line_id = int(row["LINE_ID"]) if pd.notna(row["LINE_ID"]) else ch["idx"] + 1
                        record_id = str(file_row.iloc[0].get("RECORD_ID", "")) or None
                        corrections_dict = {}
                        for disp_col, ext_col in COL_MAP.items():
                            val = row.get(disp_col)
                            if ext_col in ("col_3", "col_4", "col_5"):
                                cval = _safe_num(val)
                            else:
                                cval = _safe_str(val)
                            if cval is not None:
                                corrections_dict[ext_col] = cval

                        session.sql(
                            f"""
                            INSERT INTO {DB}.LINE_ITEM_REVIEW (
                                line_id, file_name, record_id,
                                corrected_col_1, corrected_col_2,
                                corrected_col_3, corrected_col_4, corrected_col_5,
                                corrections
                            ) SELECT
                                ?, ?, ?,
                                ?, ?, ?, ?, ?,
                                PARSE_JSON(?)
                            """,
                            params=[_to_native(p) for p in [
                                int(line_id),
                                str(file_name_for_lines),
                                record_id,
                                _safe_str(row.get("DESCRIPTION")),
                                _safe_str(row.get("CATEGORY")),
                                _safe_num(row.get("QUANTITY")),
                                _safe_num(row.get("UNIT_PRICE")),
                                _safe_num(row.get("LINE_TOTAL")),
                                json.dumps(corrections_dict),
                            ]],
                        ).collect()
                        saved += 1

                    st.session_state.line_save_result = {"count": saved, "file": file_name_for_lines}
                    st.session_state.line_orig_key = None
                    st.rerun()
            else:
                st.caption("No pending changes — edit any cell above, then save")
        else:
            st.info("No line items found for this document.")
else:
    st.info("No documents match the selected filters.")

