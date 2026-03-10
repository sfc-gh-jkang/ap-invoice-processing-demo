"""
Page 2: Analytics — Spend/volume by sender, monthly trends, top line items.
"""

import streamlit as st
import plotly.express as px
from config import DB, get_session, get_doc_type_labels, get_doc_types, inject_custom_css, sidebar_branding

st.set_page_config(page_title="Analytics", page_icon="📊", layout="wide")

inject_custom_css()
with st.sidebar:
    sidebar_branding()

session = get_session()

st.title("Document Analytics")
st.caption("Insights from your extracted document data")

# --- Document Type Filter ---
doc_types = get_doc_types(session)
selected_type = st.selectbox("Document Type", ["ALL"] + doc_types, index=0)

labels = get_doc_type_labels(session, selected_type if selected_type != "ALL" else "INVOICE")

type_and_clause = ""
type_where_clause = ""
type_params = []
if selected_type != "ALL":
    type_and_clause = "AND rd.doc_type = ?"
    type_where_clause = "WHERE rd.doc_type = ?"
    type_params = [selected_type]


# --- Summary by Sender (horizontal bar) ---
st.subheader(f"Amount by {labels.get('sender_label', 'Sender')}")

vendor_df = session.sql(
    f"""
    SELECT
        ef.field_1          AS vendor_name,
        COUNT(*)            AS document_count,
        SUM(ef.field_10)    AS total_amount,
        AVG(ef.field_10)    AS avg_amount
    FROM {DB}.EXTRACTED_FIELDS ef
        JOIN {DB}.RAW_DOCUMENTS rd ON ef.file_name = rd.file_name
    WHERE ef.field_1 IS NOT NULL {type_and_clause}
    GROUP BY ef.field_1
    ORDER BY total_amount DESC
    LIMIT 15
    """,
    params=type_params,
).to_pandas()

if len(vendor_df) > 0:
    fig_vendor = px.bar(
        vendor_df,
        x="TOTAL_AMOUNT",
        y="VENDOR_NAME",
        orientation="h",
        color="TOTAL_AMOUNT",
        color_continuous_scale="Blues",
        labels={"TOTAL_AMOUNT": f"{labels.get('amount_label', 'Total Amount')} ($)", "VENDOR_NAME": labels.get("sender_label", "Sender")},
        text_auto="$.2s",
    )
    fig_vendor.update_layout(
        showlegend=False,
        coloraxis_showscale=False,
        yaxis=dict(autorange="reversed"),
        height=400,
        margin=dict(l=10, r=10, t=10, b=10),
    )
    st.plotly_chart(fig_vendor, use_container_width=True)
else:
    st.info("No data available. Run extraction first.")

st.divider()

# --- Monthly Trend + Aging side by side ---
col1, col2 = st.columns(2)

with col1:
    st.subheader("Monthly Trend")
    monthly_df = session.sql(
        f"""
        SELECT
            DATE_TRUNC('month', ef.field_4)  AS month,
            COUNT(*)                           AS document_count,
            SUM(ef.field_10)                   AS total_amount
        FROM {DB}.EXTRACTED_FIELDS ef
            JOIN {DB}.RAW_DOCUMENTS rd ON ef.file_name = rd.file_name
        WHERE ef.field_4 IS NOT NULL {type_and_clause}
        GROUP BY DATE_TRUNC('month', ef.field_4)
        ORDER BY month
        """,
        params=type_params,
    ).to_pandas()

    if len(monthly_df) > 0:
        fig_monthly = px.area(
            monthly_df,
            x="MONTH",
            y="TOTAL_AMOUNT",
            labels={"TOTAL_AMOUNT": f"{labels.get('amount_label', 'Total Amount')} ($)", "MONTH": "Month"},
            color_discrete_sequence=["#1a237e"],
        )
        fig_monthly.update_layout(
            height=350,
            margin=dict(l=10, r=10, t=10, b=10),
        )
        st.plotly_chart(fig_monthly, use_container_width=True)

        total = monthly_df["TOTAL_AMOUNT"].sum()
        avg_monthly = monthly_df["TOTAL_AMOUNT"].mean()
        st.caption(f"Total: ${total:,.0f} | Monthly Avg: ${avg_monthly:,.0f}")

with col2:
    st.subheader("Aging Distribution")
    # Query V_DOCUMENT_LEDGER directly (instead of V_AGING_SUMMARY) so we
    # can filter by doc_type.
    aging_df = session.sql(
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
        WHERE aging_bucket != 'N/A'
            {"AND doc_type = ?" if type_params else ""}
        GROUP BY aging_bucket, sort_order
        ORDER BY sort_order
        """,
        params=type_params,
    ).to_pandas()

    if len(aging_df) > 0:
        color_map = {
            "Current": "#4caf50",
            "1-30 Days": "#ff9800",
            "31-60 Days": "#f44336",
            "61-90 Days": "#b71c1c",
            "90+ Days": "#4a0000",
        }
        fig_aging = px.bar(
            aging_df,
            x="AGING_BUCKET",
            y="TOTAL_AMOUNT",
            color="AGING_BUCKET",
            color_discrete_map=color_map,
            labels={"TOTAL_AMOUNT": "Amount ($)", "AGING_BUCKET": "Aging Bucket"},
            text_auto="$.2s",
        )
        fig_aging.update_layout(
            showlegend=False,
            height=350,
            margin=dict(l=10, r=10, t=10, b=10),
        )
        st.plotly_chart(fig_aging, use_container_width=True)

st.divider()

# --- Top Line Items ---
st.subheader("Top 20 Items by Amount")

# Join EXTRACTED_TABLE_DATA with RAW_DOCUMENTS so we can filter by doc_type.
top_items_df = session.sql(
    f"""
    SELECT
        etd.col_1               AS item_description,
        etd.col_2               AS category,
        COUNT(*)                AS appearance_count,
        SUM(etd.col_3)          AS total_quantity,
        AVG(etd.col_4)          AS avg_unit_price,
        SUM(etd.col_5)          AS total_spend
    FROM {DB}.EXTRACTED_TABLE_DATA etd
        JOIN {DB}.RAW_DOCUMENTS rd ON etd.file_name = rd.file_name
    WHERE etd.col_1 IS NOT NULL
        {"AND rd.doc_type = ?" if type_params else ""}
    GROUP BY etd.col_1, etd.col_2
    ORDER BY total_spend DESC
    LIMIT 20
    """,
    params=type_params,
).to_pandas()

if len(top_items_df) > 0:
    st.dataframe(
        top_items_df,
        column_config={
            "ITEM_DESCRIPTION": "Item",
            "CATEGORY": "Category",
            "APPEARANCE_COUNT": "Occurrences",
            "TOTAL_QUANTITY": st.column_config.NumberColumn("Total Qty", format="%.0f"),
            "AVG_UNIT_PRICE": st.column_config.NumberColumn("Avg Price", format="$%.2f"),
            "TOTAL_SPEND": st.column_config.NumberColumn("Total Amount", format="$%.2f"),
        },
        hide_index=True,
        use_container_width=True,
    )
