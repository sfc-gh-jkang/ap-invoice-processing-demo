"""
Page 4: Admin — Manage document type configurations.

No-JSON interface for non-technical users. Guided field builder auto-generates
extraction prompts, field labels, review fields, and table schemas from simple
form inputs. Supports re-extraction and test extraction.
"""

import json
import streamlit as st
import pandas as pd
from config import (
    DB, get_session, get_all_doc_type_configs, get_doc_type_config,
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
                st.markdown("**Test Extraction**")
                st.caption(
                    "Upload a single test document to verify your extraction prompt works correctly."
                )
                test_file = st.file_uploader(
                    "Upload a test document (PDF)",
                    type=["pdf"],
                    key=f"test_upload_{selected_type}",
                )
                if test_file and st.button("Run Test Extraction"):
                    with st.spinner("Running extraction on test document..."):
                        try:
                            # Upload to stage
                            test_fname = f"_test_{selected_type}_{test_file.name}"
                            session.file.put_stream(
                                test_file, f"@{DB}.DOCUMENT_STAGE/{test_fname}",
                                auto_compress=False, overwrite=True,
                            )
                            # Build a simple extraction call
                            prompt = cfg.get("extraction_prompt", "")
                            if prompt:
                                # Parse field names from prompt
                                import re
                                prompt_for_parsing = re.split(r'\.\s*FORMATTING\s+RULES', prompt, maxsplit=1)[0]
                                match = re.search(r':\s*(.+)$', prompt_for_parsing)
                                if match:
                                    fnames = [f.strip().rstrip('.') for f in match.group(1).split(',')]
                                    prompt_parts = []
                                    for fn in fnames:
                                        lbl = fn.replace('_', ' ').title()
                                        prompt_parts.append(f"'{fn}': 'What is the {lbl.lower()}?'")
                                    prompt_obj = '{' + ', '.join(prompt_parts) + '}'

                                    result = session.sql(f"""
                                        SELECT AI_EXTRACT(
                                            TO_FILE('@{DB}.DOCUMENT_STAGE', '{test_fname}'),
                                            {prompt_obj}
                                        ) AS extraction
                                    """).collect()

                                    if result:
                                        ext = result[0]['EXTRACTION']
                                        ext_json = json.loads(ext) if isinstance(ext, str) else ext
                                        response = ext_json.get('response', ext_json)
                                        st.success("Extraction successful!")
                                        st.json(response)
                                    else:
                                        st.warning("No extraction result returned.")
                            # Clean up test file
                            session.sql(
                                f"REMOVE @{DB}.DOCUMENT_STAGE/{test_fname}"
                            ).collect()
                        except Exception as e:
                            st.error(f"Test extraction failed: {e}")

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
