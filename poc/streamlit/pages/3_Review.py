"""
Page 3: Review & Approve — Inline-editable document table with append-only
audit trail. (v2: view-column-based type mapping)

Reads from V_DOCUMENT_SUMMARY (view — instant, no lag), presents an editable
grid via st.data_editor, highlights pending changes, and INSERTs a new row
into INVOICE_REVIEW for every changed record (never updates/merges — full
traceability of every edit).

Fully config-driven: columns, labels, editable fields, and corrections are
all derived from DOCUMENT_TYPE_CONFIG. Works for any document type with
zero code changes.
"""

import json
import streamlit as st
import pandas as pd
from config import (
    DB, get_session, get_doc_type_labels, get_doc_types,
    get_doc_type_config, get_field_names_from_labels,
    get_field_name_for_key, _parse_variant,
    inject_custom_css, sidebar_branding,
)

st.set_page_config(page_title="Review & Approve", page_icon="✅", layout="wide")

inject_custom_css()
with st.sidebar:
    sidebar_branding()

session = get_session()

# ── Session state init ───────────────────────────────────────────────────────
if "save_result" not in st.session_state:
    st.session_state.save_result = None  # {"count": N, "record_ids": [...]}
if "line_save_result" not in st.session_state:
    st.session_state.line_save_result = None

st.title("Review & Approve")
st.caption("Edit any cell directly in the table, then save all changes at once")
st.info(
    "**Note:** Documents are identified by **filename**. "
    "Re-uploading a file with the same name overwrites the extraction but preserves review corrections. "
    "If you need to track a different document, ensure the filename is unique."
)

# ── Post-save confirmation ───────────────────────────────────────────────────
if st.session_state.save_result:
    result = st.session_state.save_result
    st.success(f"Saved {result['count']} change(s) — new audit rows appended to INVOICE_REVIEW")

    st.subheader("Saved Records (Current Values)")
    st.caption("Live values from the view — your corrections are already applied")

    record_ids = result["record_ids"]
    placeholders = ",".join(["?"] * len(record_ids))
    saved_df = session.sql(
        f"SELECT * FROM {DB}.V_DOCUMENT_SUMMARY WHERE record_id IN ({placeholders})",
        params=record_ids
    ).to_pandas()

    if len(saved_df) > 0:
        st.dataframe(saved_df, hide_index=True, use_container_width=True)
    else:
        st.info("No matching records found.")

    if st.button("Continue Editing"):
        st.session_state.save_result = None
        st.rerun()

    st.stop()

# ── Document Type + Filters ───────────────────────────────────────────────────
doc_types = get_doc_types(session)
selected_type = st.selectbox("Document Type", ["ALL"] + doc_types, index=0, key="review_doc_type")

effective_type = selected_type if selected_type != "ALL" else (doc_types[0] if doc_types else "INVOICE")
labels = get_doc_type_labels(session, effective_type)
config = get_doc_type_config(session, effective_type)

# Determine which fields are correctable from config
review_cfg = config.get("review_fields") if config else None
correctable_fields = review_cfg.get("correctable", []) if review_cfg else []
field_types = review_cfg.get("types", {}) if review_cfg else {}

# Build field_key -> extraction_name mapping
field_keys = get_field_names_from_labels(labels)
field_key_to_name = {}
for fk in field_keys:
    ext_name = get_field_name_for_key(labels, review_cfg, fk)
    if ext_name:
        field_key_to_name[fk] = ext_name

# Build display column name mapping: VENDOR_NAME -> "Vendor Name" etc.
# The view exposes fixed columns: vendor_name, invoice_number, ... (invoice-specific)
# For non-invoice types, we use raw_extraction + corrections VARIANT columns
# The view always has: record_id, file_name, doc_type, field_1..10 aliases, 
# line_item_count, computed_line_total, review_status, reviewer_notes, etc.

col_f1, col_f2 = st.columns(2)

with col_f1:
    review_filter = st.selectbox(
        "Review Status",
        ["Pending Review", "ALL", "APPROVED", "REJECTED", "CORRECTED"],
    )

with col_f2:
    sender_label = labels.get("sender_label", "Sender")
    # The first field (field_1) is typically the sender/vendor
    first_field_alias = "vendor_name"  # V_DOCUMENT_SUMMARY alias
    vendor_type_clause = ""
    vendor_type_params = []
    if selected_type != "ALL":
        vendor_type_clause = "AND doc_type = ?"
        vendor_type_params = [selected_type]

    try:
        vendor_list_df = session.sql(
            f"SELECT DISTINCT vendor_name FROM {DB}.V_DOCUMENT_SUMMARY "
            f"WHERE vendor_name IS NOT NULL {vendor_type_clause} ORDER BY vendor_name",
            params=vendor_type_params,
        ).to_pandas()
        vendor_options = ["ALL"] + vendor_list_df["VENDOR_NAME"].tolist()
    except Exception as e:
        st.error(f"Could not load vendor list: {e}")
        vendor_options = ["ALL"]
    selected_vendor = st.selectbox(sender_label, vendor_options)

# ── Build query ──────────────────────────────────────────────────────────────
where_parts = []
params = []

if selected_type != "ALL":
    where_parts.append("doc_type = ?")
    params.append(selected_type)

if review_filter == "Pending Review":
    where_parts.append("review_status IS NULL")
elif review_filter in ("APPROVED", "REJECTED", "CORRECTED"):
    where_parts.append("review_status = ?")
    params.append(review_filter)

if selected_vendor != "ALL":
    where_parts.append("vendor_name = ?")
    params.append(selected_vendor)

where_sql = "WHERE " + " AND ".join(where_parts) if where_parts else ""

# The view has fixed aliases: vendor_name, invoice_number, etc. (for invoice compat)
# Plus raw_extraction and corrections for any doc type
query = f"""
    SELECT *
    FROM {DB}.V_DOCUMENT_SUMMARY
    {where_sql}
    ORDER BY record_id DESC
"""

try:
    original_df = session.sql(query, params=params).to_pandas()
except Exception as e:
    st.error(f"Could not load documents: {e}")
    original_df = pd.DataFrame()

display_name = config.get("display_name", "Document") if config else "Document"
st.subheader(f"{display_name}s ({len(original_df)} results)")

if len(original_df) == 0:
    st.info(f"No {display_name.lower()}s match the selected filters.")
    st.stop()

# ── Prepare editable copy ────────────────────────────────────────────────────
edit_df = original_df.copy()
edit_df["REVIEW_STATUS"] = edit_df["REVIEW_STATUS"].fillna("")
edit_df["REVIEWER_NOTES"] = edit_df["REVIEWER_NOTES"].fillna("")

# Store original snapshot keyed by filter state so we always diff against the
# DB values, not against a stale snapshot from a different filter
filter_key = f"{selected_type}|{review_filter}|{selected_vendor}"
if "orig_snapshot_key" not in st.session_state or st.session_state.orig_snapshot_key != filter_key:
    st.session_state.orig_snapshot = edit_df.copy()
    st.session_state.orig_snapshot_key = filter_key

orig_snapshot = st.session_state.orig_snapshot

# ── Build dynamic column config ─────────────────────────────────────────────
# Map view column names to display config based on doc type labels + types
column_config = {
    "RECORD_ID": None,
    "FILE_NAME": st.column_config.TextColumn("File", disabled=True),
    "DOC_TYPE": st.column_config.TextColumn("Type", disabled=True),
}

# Map the view's fixed invoice-alias columns using labels
# The view always outputs: vendor_name, invoice_number, po_number, invoice_date,
# due_date, payment_terms, recipient, subtotal, tax_amount, total_amount
VIEW_COLUMN_ORDER = [
    "VENDOR_NAME", "INVOICE_NUMBER", "PO_NUMBER", "INVOICE_DATE",
    "DUE_DATE", "PAYMENT_TERMS", "RECIPIENT", "SUBTOTAL",
    "TAX_AMOUNT", "TOTAL_AMOUNT",
]

# Build editable columns list
EDITABLE_COLS = []

VIEW_COL_TYPES = {
    "INVOICE_DATE": "DATE",
    "DUE_DATE": "DATE",
    "SUBTOTAL": "NUMBER",
    "TAX_AMOUNT": "NUMBER",
    "TOTAL_AMOUNT": "NUMBER",
    "LINE_ITEM_COUNT": "NUMBER",
    "COMPUTED_LINE_TOTAL": "NUMBER",
}

for i, view_col in enumerate(VIEW_COLUMN_ORDER):
    fk = f"field_{i+1}"
    label = labels.get(fk, view_col.replace("_", " ").title())
    ftype = VIEW_COL_TYPES.get(view_col, "VARCHAR")

    if ftype == "DATE":
        column_config[view_col] = st.column_config.DateColumn(label)
    elif ftype == "NUMBER":
        column_config[view_col] = st.column_config.NumberColumn(label, format="%.2f")
    else:
        column_config[view_col] = st.column_config.TextColumn(label)

    EDITABLE_COLS.append(view_col)

# Add line_item_count and computed_line_total as editable
column_config["LINE_ITEM_COUNT"] = st.column_config.NumberColumn("Line Items", format="%d")
column_config["COMPUTED_LINE_TOTAL"] = st.column_config.NumberColumn("Line Total", format="%.2f")
EDITABLE_COLS.extend(["LINE_ITEM_COUNT", "COMPUTED_LINE_TOTAL"])

# Status + notes
column_config["REVIEW_STATUS"] = st.column_config.SelectboxColumn(
    "Status", options=["", "APPROVED", "REJECTED", "CORRECTED"],
)
column_config["REVIEWER_NOTES"] = st.column_config.TextColumn("Notes")
EDITABLE_COLS.extend(["REVIEW_STATUS", "REVIEWER_NOTES"])

# Non-editable metadata
column_config["EXTRACTION_STATUS"] = st.column_config.TextColumn("Ext. Status", disabled=True)
column_config["EXTRACTED_AT"] = st.column_config.TextColumn("Extracted At", disabled=True)
column_config["REVIEWED_BY"] = st.column_config.TextColumn("Reviewed By", disabled=True)
column_config["REVIEWED_AT"] = st.column_config.TextColumn("Reviewed At", disabled=True)
column_config["RAW_EXTRACTION"] = None
column_config["CORRECTIONS"] = None

# ── Editable grid ────────────────────────────────────────────────────────────
edited_df = st.data_editor(
    edit_df,
    column_config=column_config,
    hide_index=True,
    use_container_width=True,
    num_rows="fixed",
    key="doc_editor",
)
for c in edited_df.columns:
    edited_df[c] = edited_df[c].apply(
        lambda v: v.item() if hasattr(v, "item") else v
    )

# ── Detect changes ───────────────────────────────────────────────────────────

def _norm(val):
    """Normalize for comparison — coerce everything to stripped string."""
    if val is None:
        return ""
    if isinstance(val, float) and pd.isna(val):
        return ""
    if isinstance(val, pd.Timestamp):
        return str(val.date())
    try:
        if pd.isna(val):
            return ""
    except (TypeError, ValueError):
        pass
    return str(val).strip()


changed_rows = []
change_details = []

for idx in range(min(len(orig_snapshot), len(edited_df))):
    orig_row = orig_snapshot.iloc[idx]
    edit_row = edited_df.iloc[idx]
    row_changes = {}

    for col in EDITABLE_COLS:
        old_val = _norm(orig_row.get(col))
        new_val = _norm(edit_row.get(col))
        if old_val != new_val:
            row_changes[col] = (old_val, new_val)

    if row_changes:
        changed_rows.append(idx)
        ref_label = labels.get("reference_label", "Record")
        # Use the reference field (field_2 = invoice_number, etc.) or file_name
        ref_val = _norm(edit_row.get("INVOICE_NUMBER")) or _norm(edit_row.get("FILE_NAME"))
        for col, (old_v, new_v) in row_changes.items():
            change_details.append({
                ref_label: ref_val,
                "Field": col.replace("_", " ").title(),
                "Was": old_v if old_v else "(empty)",
                "Now": new_v if new_v else "(empty)",
            })

# ── Change summary + save ────────────────────────────────────────────────────

st.divider()

if changed_rows:
    st.warning(f"**{len(changed_rows)} document(s) with unsaved changes**")

    st.markdown("**Pending changes:**")
    changes_display = pd.DataFrame(change_details)
    st.dataframe(changes_display, hide_index=True, use_container_width=True)

    if st.button(f"Save {len(changed_rows)} Change(s)", type="primary"):
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
                f = float(v)
            except (ValueError, TypeError):
                return None
            # NUMBER(12,2) range: -9999999999.99 to 9999999999.99
            if f > 9999999999.99 or f < -9999999999.99:
                return None
            return f

        def _safe_date(v):
            if v is None:
                return None
            if isinstance(v, float) and pd.isna(v):
                return None
            try:
                return str(pd.to_datetime(v).date())
            except Exception:
                return None

        # Map view column -> extraction field name -> type for corrections
        VIEW_TO_EXT = {}
        for i, vc in enumerate(VIEW_COLUMN_ORDER):
            fk = f"field_{i+1}"
            ext_name = field_key_to_name.get(fk)
            if ext_name:
                VIEW_TO_EXT[vc] = ext_name
        VIEW_TO_EXT["LINE_ITEM_COUNT"] = "line_item_count"
        VIEW_TO_EXT["COMPUTED_LINE_TOTAL"] = "computed_line_total"

        # ── Pre-save validation ───────────────────────────────────────
        validation_errors = []
        for idx in changed_rows:
            row = edited_df.iloc[idx]
            ref = _norm(row.get("INVOICE_NUMBER")) or _norm(row.get("FILE_NAME"))
            for vc, ext_name in VIEW_TO_EXT.items():
                ftype = VIEW_COL_TYPES.get(vc, "VARCHAR")
                raw_val = row.get(vc)
                if raw_val is None or (isinstance(raw_val, float) and pd.isna(raw_val)):
                    continue
                if ftype == "NUMBER":
                    try:
                        f = float(raw_val)
                        if f > 9999999999.99 or f < -9999999999.99:
                            validation_errors.append(
                                f"**{ref}** — {vc.replace('_', ' ').title()}: "
                                f"value {raw_val} exceeds NUMBER(12,2) range "
                                f"(±9,999,999,999.99)"
                            )
                    except (ValueError, TypeError):
                        validation_errors.append(
                            f"**{ref}** — {vc.replace('_', ' ').title()}: "
                            f"'{raw_val}' is not a valid number"
                        )
                elif ftype == "DATE":
                    raw_str = str(raw_val).strip()
                    if raw_str:
                        try:
                            pd.to_datetime(raw_str)
                        except (ValueError, TypeError):
                            validation_errors.append(
                                f"**{ref}** — {vc.replace('_', ' ').title()}: "
                                f"'{raw_str}' is not a valid date "
                                f"(expected YYYY-MM-DD)"
                            )

        if validation_errors:
            st.error("**Validation failed — changes not saved:**")
            for err in validation_errors:
                st.markdown(f"- {err}")
            st.stop()

        saved = 0
        saved_ids = []
        for idx in changed_rows:
            row = edited_df.iloc[idx]
            record_id = int(row["RECORD_ID"])
            file_name = str(row["FILE_NAME"])
            status = str(row["REVIEW_STATUS"]) if row["REVIEW_STATUS"] else "CORRECTED"

            # Build corrections VARIANT JSON using config-driven field mapping
            corrections_dict = {}
            for vc, ext_name in VIEW_TO_EXT.items():
                ftype = VIEW_COL_TYPES.get(vc, "VARCHAR")
                if ftype == "NUMBER":
                    val = _safe_num(row.get(vc))
                elif ftype == "DATE":
                    val = _safe_date(row.get(vc))
                else:
                    val = _safe_str(row.get(vc))
                if val is not None:
                    corrections_dict[ext_name] = val

            corrections_json = json.dumps(corrections_dict)

            try:
                session.sql(
                    f"""
                    INSERT INTO {DB}.INVOICE_REVIEW (
                        record_id, file_name, review_status,
                        corrected_vendor_name, corrected_invoice_number,
                        corrected_po_number, corrected_invoice_date,
                        corrected_due_date, corrected_payment_terms,
                        corrected_recipient, corrected_subtotal,
                        corrected_tax_amount, corrected_total,
                        reviewer_notes, corrections
                    ) SELECT
                        ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, PARSE_JSON(?)
                    """,
                    params=[_to_native(p) for p in [
                        record_id,
                        file_name,
                        status,
                        _safe_str(row.get("VENDOR_NAME")),
                        _safe_str(row.get("INVOICE_NUMBER")),
                        _safe_str(row.get("PO_NUMBER")),
                        _safe_date(row.get("INVOICE_DATE")),
                        _safe_date(row.get("DUE_DATE")),
                        _safe_str(row.get("PAYMENT_TERMS")),
                        _safe_str(row.get("RECIPIENT")),
                        _safe_num(row.get("SUBTOTAL")),
                        _safe_num(row.get("TAX_AMOUNT")),
                        _safe_num(row.get("TOTAL_AMOUNT")),
                        _safe_str(row.get("REVIEWER_NOTES")),
                        corrections_json,
                    ]],
                ).collect()
                saved += 1
                saved_ids.append(record_id)
            except Exception as e:
                st.error(f"Could not save record {record_id}: {e}")

        # Store result and clear snapshot so next load re-fetches
        st.session_state.save_result = {"count": saved, "record_ids": saved_ids}
        st.session_state.orig_snapshot_key = None
        st.rerun()
else:
    st.caption("No pending changes — edit any cell above, then save")

# ── Line Item Review ─────────────────────────────────────────────────────────
st.divider()
st.subheader("Line Item Review")
st.caption("Select a document above, then edit its line items below")

if st.session_state.line_save_result:
    result = st.session_state.line_save_result
    st.success(f"Saved {result['count']} line item correction(s) — audit rows appended to LINE_ITEM_REVIEW")
    if st.button("Continue Editing Line Items"):
        st.session_state.line_save_result = None
        st.rerun()

doc_options_df = original_df[["RECORD_ID", "FILE_NAME", "INVOICE_NUMBER", "VENDOR_NAME"]].copy()
doc_choices = []
for _, r in doc_options_df.iterrows():
    label = r["INVOICE_NUMBER"] or r["FILE_NAME"]
    vendor = r["VENDOR_NAME"] or ""
    doc_choices.append(f"{label} — {vendor}" if vendor else str(label))

if doc_choices:
    selected_line_doc = st.selectbox("Select document for line item review", doc_choices, key="line_item_doc_select")
    selected_idx = doc_choices.index(selected_line_doc)
    selected_file = str(doc_options_df.iloc[selected_idx]["FILE_NAME"])
    selected_record_id = int(doc_options_df.iloc[selected_idx]["RECORD_ID"])

    try:
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
            params=[selected_file],
        ).to_pandas()
    except Exception as e:
        st.error(f"Could not load line items: {e}")
        line_items = pd.DataFrame()

    if len(line_items) > 0:
        line_filter_key = f"review_lines|{selected_file}"
        if "review_line_orig_key" not in st.session_state or st.session_state.review_line_orig_key != line_filter_key:
            st.session_state.review_line_orig_snapshot = line_items.copy()
            st.session_state.review_line_orig_key = line_filter_key
        line_orig = st.session_state.review_line_orig_snapshot

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
            key=f"review_line_editor_{selected_file}",
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

            if st.button(f"Save {len(line_changes)} Line Item Change(s)", type="primary", key="save_line_items"):
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

                def _li_safe_str(v):
                    if v is None:
                        return None
                    if isinstance(v, float) and pd.isna(v):
                        return None
                    s = str(v).strip()
                    return s if s else None

                def _li_safe_num(v):
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
                                f = float(raw_val)
                                if col == "QUANTITY" and f < 0:
                                    validation_errors.append(f"Line #{ln} — Quantity: negative value ({f})")
                                if col in ("UNIT_PRICE", "LINE_TOTAL") and (f > 9999999999.99 or f < -9999999999.99):
                                    validation_errors.append(f"Line #{ln} — {col.replace('_', ' ').title()}: value {f} exceeds allowed range (±9,999,999,999.99)")
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
                    corrections_dict = {}
                    for disp_col, ext_col in COL_MAP.items():
                        val = row.get(disp_col)
                        if ext_col in ("col_3", "col_4", "col_5"):
                            cval = _li_safe_num(val)
                        else:
                            cval = _li_safe_str(val)
                        if cval is not None:
                            corrections_dict[ext_col] = cval

                    try:
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
                                str(selected_file),
                                int(selected_record_id),
                                _li_safe_str(row.get("DESCRIPTION")),
                                _li_safe_str(row.get("CATEGORY")),
                                _li_safe_num(row.get("QUANTITY")),
                                _li_safe_num(row.get("UNIT_PRICE")),
                                _li_safe_num(row.get("LINE_TOTAL")),
                                json.dumps(corrections_dict),
                            ]],
                        ).collect()
                        saved += 1
                    except Exception as e:
                        st.error(f"Could not save line item #{line_id}: {e}")

                st.session_state.line_save_result = {"count": saved, "file": selected_file}
                st.session_state.review_line_orig_key = None
                st.rerun()
        else:
            st.caption("No pending line item changes — edit any cell above, then save")
    else:
        st.info("No line items found for this document.")
