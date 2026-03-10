"""
Page 0: Dashboard — KPI cards, recent documents, extraction metrics.
"""

import streamlit as st
from config import DB, get_session, get_doc_type_labels, get_doc_types, inject_custom_css, sidebar_branding

st.title("Document Processing Dashboard")
st.caption("Powered by Snowflake Cortex AI_EXTRACT")

inject_custom_css()
with st.sidebar:
    sidebar_branding()

session = get_session()

# --- Document Type Filter ---
doc_types = get_doc_types(session)
selected_type = st.selectbox("Document Type", ["ALL"] + doc_types, index=0)

labels = get_doc_type_labels(session, selected_type if selected_type != "ALL" else "INVOICE")

type_clause = ""
type_params = []
if selected_type != "ALL":
    type_clause = "WHERE rd.doc_type = ?"
    type_params = [selected_type]


# --- KPI Cards ---
kpis = session.sql(
    f"""
    SELECT
        COUNT(*)                                                     AS total_documents,
        SUM(ef.field_10)                                             AS total_amount,
        COUNT(DISTINCT ef.field_1)                                   AS unique_senders,
        COUNT(CASE WHEN ef.field_5 IS NOT NULL
                    AND ef.field_5 < CURRENT_DATE() THEN 1 END)     AS overdue_count,
        SUM(CASE WHEN ef.field_5 IS NOT NULL
                  AND ef.field_5 < CURRENT_DATE() THEN ef.field_10 ELSE 0 END) AS overdue_amount
    FROM {DB}.EXTRACTED_FIELDS ef
        JOIN {DB}.RAW_DOCUMENTS rd ON ef.file_name = rd.file_name
    {type_clause}
    """,
    params=type_params,
).to_pandas()

if len(kpis) > 0:
    k = kpis.iloc[0]

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Documents", f"{int(k['TOTAL_DOCUMENTS']):,}")

    with col2:
        total = k["TOTAL_AMOUNT"] or 0
        st.metric(labels.get("amount_label", "Total Amount"), f"${total:,.0f}")

    with col3:
        st.metric(f"Unique {labels.get('sender_label', 'Senders')}", f"{int(k['UNIQUE_SENDERS']):,}")

    with col4:
        overdue = k["OVERDUE_AMOUNT"] or 0
        st.metric(
            "Overdue",
            f"${overdue:,.0f}",
            delta=f"{int(k['OVERDUE_COUNT'])} documents" if k["OVERDUE_COUNT"] else "0",
            delta_color="inverse",
        )

st.divider()

# --- Extraction Pipeline Status ---
status = session.sql(f"SELECT * FROM {DB}.V_EXTRACTION_STATUS").to_pandas()

if len(status) > 0:
    s = status.iloc[0]
    col5, col6, col7 = st.columns(3)
    with col5:
        st.metric(
            "Pipeline Progress",
            f"{int(s['EXTRACTED_FILES'])}/{int(s['TOTAL_FILES'])} processed",
        )
    with col6:
        st.metric("Pending", f"{int(s['PENDING_FILES'])}")
    with col7:
        st.metric("Failed", f"{int(s['FAILED_FILES'])}")

st.divider()

# --- Recent Documents ---
st.subheader("Recently Extracted Documents")

recent = session.sql(
    f"""
    SELECT
        ef.field_2       AS document_number,
        ef.field_1       AS sender,
        ef.field_4       AS document_date,
        ef.field_5       AS due_date,
        ef.field_10      AS total_amount,
        ef.status,
        ef.extracted_at
    FROM {DB}.EXTRACTED_FIELDS ef
        JOIN {DB}.RAW_DOCUMENTS rd ON ef.file_name = rd.file_name
    {type_clause}
    ORDER BY ef.extracted_at DESC NULLS LAST
    LIMIT 15
    """,
    params=type_params,
).to_pandas()

if len(recent) > 0:
    st.dataframe(
        recent,
        column_config={
            "DOCUMENT_NUMBER": labels.get("reference_label", "Document #"),
            "SENDER": labels.get("sender_label", "Sender"),
            "DOCUMENT_DATE": st.column_config.DateColumn(labels.get("date_label", "Date")),
            "DUE_DATE": st.column_config.DateColumn("Due Date"),
            "TOTAL_AMOUNT": st.column_config.NumberColumn(labels.get("amount_label", "Amount"), format="$%.2f"),
            "STATUS": "Status",
            "EXTRACTED_AT": "Extracted At",
        },
        hide_index=True,
        use_container_width=True,
    )
else:
    st.info("No documents extracted yet. Run the extraction pipeline first (sql/04_batch_extract.sql).")
