"""
Convenience Store Accounts Payable — Streamlit in Snowflake
Landing page: architecture overview and business value
"""

import streamlit as st
from config import DB, STAGE

st.set_page_config(
    page_title="AP Invoice Processing",
    page_icon="📄",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ──────────────────────────────────────────────
# Header
# ──────────────────────────────────────────────
st.title("AI-Powered Invoice Processing")
st.markdown(
    "**End-to-end accounts payable automation for convenience store chains, "
    "built entirely on Snowflake.**"
)

st.divider()

# ──────────────────────────────────────────────
# Business Value
# ──────────────────────────────────────────────
st.header("Why This Matters")

st.markdown(
    """
A typical convenience store chain manages **thousands of invoices per month**
from dozens of vendors — beverages, snacks, tobacco, dairy, frozen goods,
cleaning supplies, and more. The traditional AP workflow is:
"""
)

pain_col, value_col = st.columns(2)

with pain_col:
    st.subheader("The Problem")
    st.markdown(
        """
- **Manual data entry** — staff re-key vendor name, line items, totals
  from every PDF invoice into the ERP
- **Slow turnaround** — invoices sit in email inboxes for days before
  processing, missing early-pay discounts
- **Error-prone** — typos, missed line items, and duplicate payments
  cost 1-3% of total AP spend
- **No real-time visibility** — managers cannot see outstanding
  liabilities or cash-flow impact until month-end close
- **Audit headaches** — matching a payment back to the source PDF
  requires digging through file cabinets or shared drives
"""
    )

with value_col:
    st.subheader("The Solution")
    st.markdown(
        """
- **Zero manual entry** — Snowflake Cortex AI_EXTRACT reads every PDF
  and outputs structured fields: vendor, dates, terms, line items, totals
- **Minutes, not days** — a scheduled Task processes new invoices
  automatically as they land on the stage
- **Built-in accuracy** — AI extraction eliminates transcription errors;
  totals are validated against line-item sums
- **Live dashboards** — AP clerks and managers see real-time KPIs,
  aging buckets, and spend breakdowns the moment an invoice is processed
- **Source-linked audit trail** — every extracted record links back to
  its original PDF, viewable inline in the app
"""
    )

st.divider()

# ──────────────────────────────────────────────
# Architecture Flow
# ──────────────────────────────────────────────
st.header("Architecture")

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
            pdf  [label="PDF Invoices\\n(vendor emails, scans)", fillcolor="#fce8e6", color="#ea4335"];
            stage [label="Snowflake\\nInternal Stage\\n(@INVOICE_STAGE)"];
        }

        subgraph cluster_process {
            label="Process (automated)";
            style="dashed"; color="#dadce0"; fontname="Helvetica";
            raw    [label="RAW_INVOICES\\n(file metadata)"];
            stream [label="Stream\\n(change tracking)"];
            task   [label="Scheduled Task\\n(every 5 min)"];
            ai     [label="Cortex\\nAI_EXTRACT", fillcolor="#e6f4ea", color="#34a853"];
        }

        subgraph cluster_serve {
            label="Serve";
            style="dashed"; color="#dadce0"; fontname="Helvetica";
            tables [label="EXTRACTED_INVOICES\\nEXTRACTED_LINE_ITEMS\\nVENDORS"];
            views  [label="8 Analytical Views\\n(ledger, aging, spend,\\ntrends, categories)"];
            app    [label="Streamlit App\\n(Container Runtime)", fillcolor="#fef7e0", color="#fbbc04"];
        }

        pdf   -> stage [label="PUT / upload"];
        stage -> raw   [label="DIRECTORY()"];
        raw   -> stream;
        stream -> task [label="triggers"];
        task  -> ai    [label="calls SP"];
        ai    -> tables [label="structured\\noutput"];
        tables -> views;
        views -> app;
        stage -> app [label="PDF render\\n(session.file.get)", style=dashed];
    }
    """,
    use_container_width=True,
)

st.divider()

# ──────────────────────────────────────────────
# Key Technologies
# ──────────────────────────────────────────────
st.header("Key Technologies")

t1, t2, t3 = st.columns(3)

with t1:
    st.subheader("Cortex AI_EXTRACT")
    st.markdown(
        """
Snowflake's built-in document intelligence function.
Reads unstructured PDF invoices and returns structured
JSON — vendor name, invoice number, dates, payment terms,
line items with quantities and prices — with **no model
training or external API calls** required.
"""
    )

    st.subheader("Streams + Tasks")
    st.markdown(
        """
**Streams** track new rows in `RAW_INVOICES` via
change data capture. A **scheduled Task** (every 5 minutes)
checks the stream and calls the extraction stored procedure
automatically — true event-driven processing with zero
external orchestration.
"""
    )

with t2:
    st.subheader("Streamlit Container Runtime")
    st.markdown(
        """
The dashboard runs as a **Streamlit in Snowflake** app on
Container Runtime (`SYSTEM$ST_CONTAINER_RUNTIME_PY3_11`),
enabling custom Python packages like `pypdfium2` and `plotly`
that aren't available in the standard warehouse runtime.
"""
    )

    st.subheader("Inline PDF Rendering")
    st.markdown(
        """
Source PDFs are downloaded from the internal stage via
`session.file.get()`, rendered page-by-page with
**pypdfium2**, and displayed as images — no iframes,
no presigned URLs, no external network calls. Works
behind any corporate firewall.
"""
    )

with t3:
    st.subheader("Analytical Views")
    st.markdown(
        """
Eight pre-built views power every dashboard chart:
AP Ledger with aging buckets, aging summary, spend by
vendor, spend by category, monthly trends, top line items,
vendor payment terms, and extraction pipeline status.
"""
    )

    st.subheader("PDF Generation (UDTF)")
    st.markdown(
        """
A Python **UDTF** (`GENERATE_INVOICE_PDF`) creates
realistic multi-vendor invoices using `fpdf`, writing
them directly to the internal stage — enabling a
self-contained demo with no external data dependencies.
"""
    )

st.divider()

# ──────────────────────────────────────────────
# Quick Stats (live from the database)
# ──────────────────────────────────────────────
conn = st.connection("snowflake")

stats = conn.query(
    f"""
    SELECT
        (SELECT COUNT(*) FROM {DB}.EXTRACTED_INVOICES) AS invoices,
        (SELECT COUNT(*) FROM {DB}.EXTRACTED_LINE_ITEMS) AS line_items,
        (SELECT COUNT(*) FROM {DB}.VENDORS) AS vendors,
        (SELECT COUNT(*) FROM DIRECTORY(@{STAGE})) AS pdfs
    """,
    ttl=60,
)

if len(stats) > 0:
    s = stats.iloc[0]
    st.markdown("### Live Pipeline Stats")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Invoices Extracted", f"{int(s['INVOICES']):,}")
    m2.metric("Line Items Parsed", f"{int(s['LINE_ITEMS']):,}")
    m3.metric("Vendors Identified", f"{int(s['VENDORS']):,}")
    m4.metric("Source PDFs on Stage", f"{int(s['PDFS']):,}")

# ──────────────────────────────────────────────
# Sidebar
# ──────────────────────────────────────────────
with st.sidebar:
    st.markdown("### Navigation Guide")
    st.markdown(
        """
**Dashboard** — KPI cards, outstanding balances,
recent invoices

**AP Ledger** — Full invoice list with inline PDF
viewer and extracted field details

**Analytics** — Spend breakdowns, aging analysis,
monthly trends, category insights

**Process New** — Watch the pipeline in action:
generate invoices, trigger extraction, see results

**AI Extract Lab** — Interactive prompt builder for
AI_EXTRACT: templates, visual builder, raw JSON editor
"""
    )
