"""
AI_EXTRACT Document Processing — POC Dashboard
Landing page with pipeline status and overview.
"""

import streamlit as st
from config import DB, get_session

st.set_page_config(
    page_title="AI_EXTRACT POC",
    page_icon="📄",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("AI-Powered Document Extraction")
st.markdown(
    "**Extract structured data from your documents using Snowflake Cortex AI_EXTRACT — "
    "no external services, no API keys, no infrastructure to manage.**"
)

st.divider()

# --- Live Pipeline Stats ---
session = get_session()

status = session.sql(
    f"SELECT * FROM {DB}.V_EXTRACTION_STATUS"
).to_pandas()

if len(status) > 0:
    s = status.iloc[0]
    st.header("Pipeline Status")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Files Staged", f"{int(s['TOTAL_FILES']):,}")
    with col2:
        st.metric("Successfully Extracted", f"{int(s['EXTRACTED_FILES']):,}")
    with col3:
        st.metric("Pending", f"{int(s['PENDING_FILES']):,}")
    with col4:
        st.metric("Failed", f"{int(s['FAILED_FILES']):,}")

    if s["LAST_EXTRACTION"]:
        st.caption(f"Last extraction: {s['LAST_EXTRACTION']}")

st.divider()

# --- How It Works ---
st.header("How It Works")

st.graphviz_chart(
    """
    digraph architecture {
        rankdir=LR;
        bgcolor="transparent";
        node [shape=box, style="rounded,filled", fontname="Helvetica",
              fontsize=12, fillcolor="#e8f0fe", color="#4285f4"];
        edge [color="#5f6368", fontname="Helvetica", fontsize=10];

        subgraph cluster_ingest {
            label="Ingest";
            style="dashed"; color="#dadce0"; fontname="Helvetica";
            docs  [label="Your Documents\\n(PDFs, images, etc.)", fillcolor="#fce8e6", color="#ea4335"];
            stage [label="Snowflake\\nInternal Stage"];
        }

        subgraph cluster_process {
            label="Process";
            style="dashed"; color="#dadce0"; fontname="Helvetica";
            raw    [label="RAW_DOCUMENTS\\n(file tracking)"];
            stream [label="Stream\\n(change detection)"];
            task   [label="Scheduled Task\\n(every 5 min)"];
            ai     [label="Cortex\\nAI_EXTRACT", fillcolor="#e6f4ea", color="#34a853"];
        }

        subgraph cluster_serve {
            label="Serve";
            style="dashed"; color="#dadce0"; fontname="Helvetica";
            tables [label="EXTRACTED_FIELDS\\nEXTRACTED_TABLE_DATA"];
            views  [label="Analytical Views"];
            app    [label="This Dashboard", fillcolor="#fef7e0", color="#fbbc04"];
        }

        docs   -> stage [label="Upload"];
        stage  -> raw   [label="Register"];
        raw    -> stream;
        stream -> task [label="triggers"];
        task   -> ai    [label="calls SP"];
        ai     -> tables [label="structured\\noutput"];
        tables -> views;
        views  -> app;
        stage  -> app [label="PDF render", style=dashed];
    }
    """,
    use_container_width=True,
)

st.divider()

# --- Quick summary from extracted data ---
summary = session.sql(
    f"""
    SELECT
        (SELECT COUNT(*) FROM {DB}.EXTRACTED_FIELDS) AS documents,
        (SELECT COUNT(*) FROM {DB}.EXTRACTED_TABLE_DATA) AS line_items,
        (SELECT COUNT(DISTINCT field_1) FROM {DB}.EXTRACTED_FIELDS WHERE field_1 IS NOT NULL) AS unique_senders
    """
).to_pandas()

if len(summary) > 0:
    r = summary.iloc[0]
    st.header("Extraction Summary")
    m1, m2, m3 = st.columns(3)
    m1.metric("Documents Extracted", f"{int(r['DOCUMENTS']):,}")
    m2.metric("Line Items Parsed", f"{int(r['LINE_ITEMS']):,}")
    m3.metric("Unique Senders", f"{int(r['UNIQUE_SENDERS']):,}")

# --- Sidebar navigation guide ---
with st.sidebar:
    st.markdown("### Pages")
    st.markdown(
        """
**Dashboard** — KPI cards, document counts,
recent extractions

**Document Viewer** — Browse all documents,
view extracted fields alongside the source PDF

**Analytics** — Charts and breakdowns by
vendor, time period, and line items

**Review & Approve** — Review extracted invoices
and record approval decisions
"""
    )
