"""
Page 5: Cost Observability — AI_EXTRACT credit consumption from first-party
billing (CORTEX_AI_FUNCTIONS_USAGE_HISTORY). All metrics in CREDITS.
Optional USD estimate via configurable credit rate in sidebar.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from config import DB, get_session, inject_custom_css, sidebar_branding

st.set_page_config(page_title="Cost Observability", page_icon="💰", layout="wide")

inject_custom_css()
with st.sidebar:
    sidebar_branding()
    st.divider()
    credit_rate = st.number_input(
        "Credit Rate (USD)", min_value=0.00, value=0.00, step=0.50,
        help="Set your contract's $/credit rate to see USD estimates. Leave 0 to show credits only."
    )

show_usd = credit_rate > 0

st.title("Cost Observability")
st.caption("AI_EXTRACT credit consumption from Snowflake first-party billing")

session = get_session()

day_range = st.selectbox("Time Range", [7, 14, 30, 90], index=2, format_func=lambda d: f"Last {d} days")

# --- Methodology & Assumptions ---
with st.expander("Cost Methodology & Assumptions", expanded=False):
    st.markdown("""
**What is "Credits/Doc"?**

This is **only** the AI_EXTRACT function credit cost — the Snowflake Cortex AI credits
consumed when calling `AI_EXTRACT()` to process a single document. It does **not** include
warehouse compute, Streamlit hosting, or any other infrastructure costs.

**Credit sources in this pipeline (shown separately below):**

| Source | What it covers | Billing view |
|--------|---------------|--------------|
| **AI_EXTRACT credits** | Token-based cost of the Cortex AI_EXTRACT function per call | `CORTEX_AI_FUNCTIONS_USAGE_HISTORY` |
| **Warehouse credits** | `AI_EXTRACT_WH` compute — includes idle time, query execution, auto-suspend gaps | `WAREHOUSE_METERING_HISTORY` |
| **SPCS credits** | `AI_EXTRACT_POC_POOL` compute pool — hosts the Streamlit container runtime | `SNOWPARK_CONTAINER_SERVICES_HISTORY` |

**How AI_EXTRACT credits work:**

- Credits are **token-based** — cost scales with the number of tokens in the document
- Token count depends on document length, complexity, and content density
- Each `AI_EXTRACT()` call = 1 document extraction = 1 credit charge
- Re-extracting the same document costs the same credits again (it's a new API call)
- See the "Credit Cost by Token Range" section for how credits vary across token sizes

**Getting cost per page:**

Page count is now tracked in `RAW_DOCUMENTS.PAGE_COUNT` (populated via `pypdfium2`).
Credits/page = credits / page_count. See the "Cost Drivers" section below.

**What is NOT included in Credits/Doc:**

- Warehouse idle time and auto-suspend credits
- Streamlit container runtime (SPCS compute pool)
- Stage storage for PDFs
- Any other queries run on `AI_EXTRACT_WH`

**USD estimates** (when enabled in sidebar) use your configured $/credit rate.
Actual cost depends on your Snowflake contract. Credits are the source of truth.
    """)

try:
    summary = session.sql(f"SELECT * FROM {DB}.V_AI_EXTRACT_COST_SUMMARY").to_pandas()
except Exception as e:
    st.warning(f"Cost summary unavailable: {e}")
    st.info("Run `sql/12_cost_views.sql` to create cost views. Requires ACCOUNT_USAGE access.")
    st.stop()

if len(summary) > 0:
    row = summary.iloc[0]
    credits_per_doc = float(row['AVG_CREDITS_PER_DOC'])

    st.subheader("AI_EXTRACT Cost (per document)")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("AI Credits (7d)", f"{row['AI_CREDITS_LAST_7D']:.4f}")
    c2.metric("AI Credits (30d)", f"{row['AI_CREDITS_LAST_30D']:.4f}")
    c3.metric("Docs Processed", f"{int(row['TOTAL_CALLS']):,}")
    if show_usd:
        c4.metric("Credits/Doc", f"{credits_per_doc:.6f}", delta=f"${credits_per_doc * credit_rate:.4f} USD")
    else:
        c4.metric("Credits/Doc", f"{credits_per_doc:.6f}")
    c5.metric("Unique Documents", f"{int(row['UNIQUE_DOCS'])}")

    st.caption(
        f"**Credits/Doc = AI_EXTRACT credits / total extractions** (1 call = 1 doc).  "
        f"This is *only* the Cortex AI function cost — warehouse and SPCS credits shown separately below."
    )

st.divider()

# --- Credit Cost by Document Complexity (token buckets) ---
st.subheader("Credit Cost by Token Range")
st.caption("AI_EXTRACT credits are token-based — larger documents consume more tokens and cost more credits")
try:
    token_dist = session.sql(f"""
        SELECT
            CASE
                WHEN PARSE_JSON(metrics[0]:value)::INT < 1500 THEN '< 1.5K tokens'
                WHEN PARSE_JSON(metrics[0]:value)::INT < 3000 THEN '1.5K - 3K'
                WHEN PARSE_JSON(metrics[0]:value)::INT < 6000 THEN '3K - 6K'
                WHEN PARSE_JSON(metrics[0]:value)::INT < 15000 THEN '6K - 15K'
                ELSE '15K+ tokens'
            END AS token_range,
            CASE
                WHEN PARSE_JSON(metrics[0]:value)::INT < 1500 THEN 1
                WHEN PARSE_JSON(metrics[0]:value)::INT < 3000 THEN 2
                WHEN PARSE_JSON(metrics[0]:value)::INT < 6000 THEN 3
                WHEN PARSE_JSON(metrics[0]:value)::INT < 15000 THEN 4
                ELSE 5
            END AS sort_order,
            COUNT(*) AS calls,
            ROUND(AVG(credits), 6) AS avg_credits,
            ROUND(MIN(credits), 6) AS min_credits,
            ROUND(MAX(credits), 6) AS max_credits,
            ROUND(AVG(PARSE_JSON(metrics[0]:value)::INT), 0) AS avg_tokens
        FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AI_FUNCTIONS_USAGE_HISTORY
        WHERE function_name = 'AI_EXTRACT'
          AND start_time >= DATEADD('day', -{day_range}, CURRENT_TIMESTAMP())
        GROUP BY 1, 2 ORDER BY 2
    """).to_pandas()
    if len(token_dist) > 0:
        col_chart, col_table = st.columns([1, 1])
        with col_chart:
            fig_td = px.bar(
                token_dist, x="TOKEN_RANGE", y="AVG_CREDITS",
                text="CALLS",
                labels={"TOKEN_RANGE": "Token Range", "AVG_CREDITS": "Avg Credits/Doc", "CALLS": "Calls"},
                color_discrete_sequence=["#29B5E8"],
            )
            fig_td.update_layout(height=300, margin=dict(l=20, r=20, t=10, b=20))
            st.plotly_chart(fig_td, use_container_width=True)
        with col_table:
            display_td = token_dist[["TOKEN_RANGE", "CALLS", "AVG_CREDITS", "MIN_CREDITS", "MAX_CREDITS", "AVG_TOKENS"]].copy()
            display_td.columns = ["Token Range", "Calls", "Avg Credits", "Min Credits", "Max Credits", "Avg Tokens"]
            if show_usd:
                display_td["Avg USD"] = display_td["Avg Credits"] * credit_rate
            st.dataframe(
                display_td, hide_index=True, use_container_width=True,
                column_config={
                    "Avg Credits": st.column_config.NumberColumn(format="%.6f"),
                    "Min Credits": st.column_config.NumberColumn(format="%.6f"),
                    "Max Credits": st.column_config.NumberColumn(format="%.6f"),
                    "Avg Tokens": st.column_config.NumberColumn(format="%d"),
                    **({"Avg USD": st.column_config.NumberColumn(format="$%.4f")} if show_usd else {}),
                },
            )
    else:
        st.info("No AI_EXTRACT calls in this period.")
except Exception as e:
    st.warning(f"Could not load token distribution: {e}")

st.divider()

# --- Cost Drivers: Per-PDF Attribution ---
st.subheader("Cost Drivers — What Makes a PDF Cost More?")
st.caption("Per-PDF cost attribution: page count, file size, tokens, and fields extracted")
try:
    drivers = session.sql(f"SELECT * FROM {DB}.V_AI_EXTRACT_COST_DRIVERS").to_pandas()
    if len(drivers) > 0:
        d1, d2, d3, d4 = st.columns(4)
        avg_cpp = drivers["AVG_CREDITS_PER_PAGE"].mean()
        avg_tpp = drivers["AVG_TOKENS_PER_PAGE"].mean()
        tc = drivers["TOKEN_CREDIT_CORRELATION"].dropna().mean()
        ptc = drivers["PAGE_TOKEN_CORRELATION"].dropna().mean()
        d1.metric("Avg Credits/Page", f"{avg_cpp:.6f}")
        d2.metric("Avg Tokens/Page", f"{avg_tpp:,.0f}")
        d3.metric("Token↔Credit Corr.", f"{tc:.2f}" if not pd.isna(tc) else "N/A")
        d4.metric("Page↔Token Corr.", f"{ptc:.2f}" if not pd.isna(ptc) else "N/A")

        st.markdown("""
**What drives AI_EXTRACT credit variation:**
- **Page count** — more pages = more tokens to process (correlation shown above)
- **Content density** — text-heavy pages with tables, line items consume more tokens than simple forms
- **Fields extracted** — more fields in the extraction schema = larger prompt + response
- **Token↔Credit** correlation of ~1.0 confirms credits are purely token-based (no fixed overhead)
""")

        display_drivers = drivers[["DOC_TYPE", "TOTAL_CALLS", "AVG_PAGES", "AVG_FILE_SIZE",
                                    "AVG_TOKENS", "AVG_CREDITS", "AVG_CREDITS_PER_PAGE",
                                    "AVG_TOKENS_PER_PAGE", "FIELDS_EXTRACTED"]].copy()
        display_drivers.columns = ["Doc Type", "Calls", "Avg Pages", "Avg Size (B)",
                                    "Avg Tokens", "Avg Credits", "Credits/Page",
                                    "Tokens/Page", "Fields"]
        if show_usd:
            display_drivers["Credits/Page USD"] = display_drivers["Credits/Page"] * credit_rate
        st.dataframe(
            display_drivers, hide_index=True, use_container_width=True,
            column_config={
                "Avg Credits": st.column_config.NumberColumn(format="%.6f"),
                "Credits/Page": st.column_config.NumberColumn(format="%.6f"),
                "Avg Size (B)": st.column_config.NumberColumn(format="%d"),
                "Avg Tokens": st.column_config.NumberColumn(format="%d"),
                "Tokens/Page": st.column_config.NumberColumn(format="%d"),
                "Fields": st.column_config.NumberColumn(format="%d"),
                **({"Credits/Page USD": st.column_config.NumberColumn(format="$%.6f")} if show_usd else {}),
            },
        )

        col_scatter1, col_scatter2 = st.columns(2)
        with col_scatter1:
            per_pdf = session.sql(
                f"SELECT file_name, doc_type, page_count, tokens, ai_credits, "
                f"file_size_bytes, field_count, credits_per_page "
                f"FROM {DB}.V_AI_EXTRACT_COST_PER_PDF "
                f"WHERE start_time >= DATEADD('day', -{day_range}, CURRENT_TIMESTAMP()) "
                f"LIMIT 500"
            ).to_pandas()
            if len(per_pdf) > 0:
                fig_s1 = px.scatter(
                    per_pdf, x="PAGE_COUNT", y="AI_CREDITS", color="DOC_TYPE",
                    size="TOKENS", hover_data=["FILE_NAME", "TOKENS", "FILE_SIZE_BYTES"],
                    labels={"PAGE_COUNT": "Pages", "AI_CREDITS": "Credits", "DOC_TYPE": "Doc Type"},
                    title="Pages vs Credits",
                    color_discrete_sequence=["#29B5E8", "#11567F", "#FF4B4B", "#FFA726", "#66BB6A"],
                )
                fig_s1.update_layout(height=350, margin=dict(l=20, r=20, t=40, b=20))
                st.plotly_chart(fig_s1, use_container_width=True)
        with col_scatter2:
            if len(per_pdf) > 0:
                fig_s2 = px.scatter(
                    per_pdf, x="TOKENS", y="AI_CREDITS", color="DOC_TYPE",
                    hover_data=["FILE_NAME", "PAGE_COUNT", "FILE_SIZE_BYTES"],
                    labels={"TOKENS": "Tokens", "AI_CREDITS": "Credits", "DOC_TYPE": "Doc Type"},
                    title="Tokens vs Credits",
                    color_discrete_sequence=["#29B5E8", "#11567F", "#FF4B4B", "#FFA726", "#66BB6A"],
                )
                fig_s2.update_layout(height=350, margin=dict(l=20, r=20, t=40, b=20))
                st.plotly_chart(fig_s2, use_container_width=True)

        with st.expander("Per-PDF Detail (recent extractions)"):
            if len(per_pdf) > 0:
                display_pdf = per_pdf.copy()
                display_pdf.columns = ["File", "Doc Type", "Pages", "Tokens", "Credits",
                                        "Size (B)", "Fields", "Credits/Page"]
                if show_usd:
                    display_pdf["USD"] = display_pdf["Credits"] * credit_rate
                st.dataframe(
                    display_pdf, hide_index=True, use_container_width=True,
                    column_config={
                        "Credits": st.column_config.NumberColumn(format="%.6f"),
                        "Credits/Page": st.column_config.NumberColumn(format="%.6f"),
                        "Size (B)": st.column_config.NumberColumn(format="%d"),
                        **({"USD": st.column_config.NumberColumn(format="$%.6f")} if show_usd else {}),
                    },
                )
    else:
        st.info("No cost attribution data. Run `sql/13_cost_attribution.sql` to set up per-PDF tracking.")
except Exception as e:
    st.warning(f"Could not load cost drivers: {e}")

st.divider()

col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Daily AI_EXTRACT Credits")
    try:
        daily = session.sql(
            f"SELECT * FROM {DB}.V_AI_EXTRACT_COST_DAILY "
            f"WHERE usage_date >= DATEADD('day', -{day_range}, CURRENT_DATE()) "
            f"ORDER BY usage_date"
        ).to_pandas()
        if len(daily) > 0:
            fig = px.area(
                daily, x="USAGE_DATE", y="AI_EXTRACT_CREDITS",
                labels={"USAGE_DATE": "Date", "AI_EXTRACT_CREDITS": "Credits"},
                color_discrete_sequence=["#29B5E8"],
            )
            fig.update_layout(height=300, margin=dict(l=20, r=20, t=10, b=20))
            st.plotly_chart(fig, use_container_width=True)

            display_daily = daily[["USAGE_DATE", "AI_EXTRACT_CREDITS", "AI_EXTRACT_CALLS",
                                   "TOTAL_TOKENS", "DOCS_EXTRACTED"]].copy()
            display_daily.columns = ["Date", "AI Credits", "Extractions", "Tokens", "Unique Docs"]
            if show_usd:
                display_daily["Est. USD"] = display_daily["AI Credits"] * credit_rate
            st.dataframe(
                display_daily, hide_index=True, use_container_width=True,
                column_config={
                    "AI Credits": st.column_config.NumberColumn(format="%.4f"),
                    "Tokens": st.column_config.NumberColumn(format="%d"),
                    **({"Est. USD": st.column_config.NumberColumn(format="$%.4f")} if show_usd else {}),
                },
            )
        else:
            st.info("No AI_EXTRACT usage data in this period.")
    except Exception as e:
        st.warning(f"Could not load daily costs: {e}")

with col_right:
    st.subheader("Credits by Document Type")
    try:
        by_type = session.sql(
            f"SELECT doc_type, SUM(ai_extract_credits) AS total_credits, "
            f"SUM(call_count) AS calls, SUM(total_tokens) AS tokens "
            f"FROM {DB}.V_AI_EXTRACT_COST_BY_DOC_TYPE "
            f"WHERE usage_date >= DATEADD('day', -{day_range}, CURRENT_DATE()) "
            f"GROUP BY doc_type ORDER BY total_credits DESC"
        ).to_pandas()
        if len(by_type) > 0:
            fig2 = px.bar(
                by_type, x="DOC_TYPE", y="TOTAL_CREDITS",
                text="CALLS",
                labels={"DOC_TYPE": "Document Type", "TOTAL_CREDITS": "AI Credits", "CALLS": "Calls"},
                color_discrete_sequence=["#11567F"],
            )
            fig2.update_layout(height=300, margin=dict(l=20, r=20, t=10, b=20))
            st.plotly_chart(fig2, use_container_width=True)

            display_type = by_type.copy()
            display_type.columns = ["Doc Type", "AI Credits", "Calls", "Tokens"]
            if show_usd:
                display_type["Est. USD"] = display_type["AI Credits"] * credit_rate
            st.dataframe(
                display_type, hide_index=True, use_container_width=True,
                column_config={
                    "AI Credits": st.column_config.NumberColumn(format="%.4f"),
                    "Tokens": st.column_config.NumberColumn(format="%d"),
                    **({"Est. USD": st.column_config.NumberColumn(format="$%.4f")} if show_usd else {}),
                },
            )
        else:
            st.info("No doc-type data available. This view joins query_tag metadata.")
    except Exception as e:
        st.warning(f"Could not load credits by doc type: {e}")

st.divider()

st.subheader("Per-Document Credit Trend")
try:
    per_doc = session.sql(
        f"SELECT * FROM {DB}.V_AI_EXTRACT_COST_PER_DOCUMENT "
        f"WHERE usage_date >= DATEADD('day', -{day_range}, CURRENT_DATE()) "
        f"ORDER BY usage_date"
    ).to_pandas()
    if len(per_doc) > 0:
        fig3 = px.line(
            per_doc, x="USAGE_DATE", y="CREDITS_PER_DOC",
            markers=True,
            labels={"USAGE_DATE": "Date", "CREDITS_PER_DOC": "Credits/Doc"},
            color_discrete_sequence=["#FF4B4B"],
        )
        fig3.update_layout(height=280, margin=dict(l=20, r=20, t=10, b=20))
        st.plotly_chart(fig3, use_container_width=True)

        display_perdoc = per_doc[["USAGE_DATE", "AI_EXTRACT_CREDITS", "AI_EXTRACT_CALLS",
                                   "TOTAL_TOKENS", "DOCS_EXTRACTED",
                                   "CREDITS_PER_DOC"]].copy()
        display_perdoc.columns = ["Date", "AI Credits", "Extractions", "Tokens",
                                   "Unique Docs", "Credits/Doc"]
        if show_usd:
            display_perdoc["USD/Doc"] = display_perdoc["Credits/Doc"] * credit_rate
        st.dataframe(
            display_perdoc, hide_index=True, use_container_width=True,
            column_config={
                "AI Credits": st.column_config.NumberColumn(format="%.4f"),
                "Credits/Doc": st.column_config.NumberColumn(format="%.6f"),
                "Tokens": st.column_config.NumberColumn(format="%d"),
                **({"USD/Doc": st.column_config.NumberColumn(format="$%.4f")} if show_usd else {}),
            },
        )
    else:
        st.info("No per-document credit data available yet.")
except Exception as e:
    st.warning(f"Could not load per-document credits: {e}")

st.divider()

st.subheader("Recent AI_EXTRACT Calls")
try:
    queries = session.sql(
        f"SELECT query_id, start_time, doc_type, ai_credits, tokens, elapsed_sec, rows_produced "
        f"FROM {DB}.V_AI_EXTRACT_QUERY_LOG "
        f"WHERE start_time >= DATEADD('day', -{day_range}, CURRENT_TIMESTAMP()) "
        f"ORDER BY start_time DESC LIMIT 100"
    ).to_pandas()
    if len(queries) > 0:
        if show_usd:
            queries["EST_USD"] = queries["AI_CREDITS"] * credit_rate
        st.dataframe(
            queries, use_container_width=True, hide_index=True,
            column_config={
                "AI_CREDITS": st.column_config.NumberColumn("AI Credits", format="%.6f"),
                "TOKENS": st.column_config.NumberColumn("Tokens", format="%d"),
                "ELAPSED_SEC": st.column_config.NumberColumn("Duration (s)", format="%.1f"),
                **({"EST_USD": st.column_config.NumberColumn("Est. USD", format="$%.4f")} if show_usd else {}),
            },
        )
    else:
        st.info("No AI_EXTRACT calls found in this period.")
except Exception as e:
    st.warning(f"Could not load query log: {e}")

st.divider()

# --- Infrastructure Credits (separate from AI_EXTRACT) ---
st.subheader("Infrastructure Credits (not included in Credits/Doc)")
st.caption("These are the other credit costs for running the pipeline — warehouse compute, Streamlit hosting, etc.")

infra_col1, infra_col2 = st.columns(2)

with infra_col1:
    try:
        wh_credits = session.sql(f"""
            SELECT
                DATE_TRUNC('day', start_time)::DATE AS usage_date,
                SUM(credits_used) AS warehouse_credits
            FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
            WHERE warehouse_name = 'AI_EXTRACT_WH'
              AND start_time >= DATEADD('day', -{day_range}, CURRENT_TIMESTAMP())
            GROUP BY 1 ORDER BY 1
        """).to_pandas()
        if len(wh_credits) > 0:
            total_wh = wh_credits["WAREHOUSE_CREDITS"].sum()
            st.metric("Warehouse Credits (AI_EXTRACT_WH)", f"{total_wh:.4f}",
                      delta=f"${total_wh * credit_rate:.2f} USD" if show_usd else None)
            fig_wh = px.bar(
                wh_credits, x="USAGE_DATE", y="WAREHOUSE_CREDITS",
                labels={"USAGE_DATE": "Date", "WAREHOUSE_CREDITS": "Credits"},
                color_discrete_sequence=["#11567F"],
            )
            fig_wh.update_layout(height=250, margin=dict(l=20, r=20, t=10, b=20))
            st.plotly_chart(fig_wh, use_container_width=True)
            st.caption("Includes idle time, auto-suspend gaps, all queries (not just AI_EXTRACT)")
    except Exception as e:
        st.warning(f"Could not load warehouse credits: {e}")

with infra_col2:
    try:
        spcs_credits = session.sql(f"""
            SELECT
                DATE_TRUNC('day', start_time)::DATE AS usage_date,
                SUM(credits_used) AS spcs_credits
            FROM SNOWFLAKE.ACCOUNT_USAGE.SNOWPARK_CONTAINER_SERVICES_HISTORY
            WHERE compute_pool_name = 'AI_EXTRACT_POC_POOL'
              AND start_time >= DATEADD('day', -{day_range}, CURRENT_TIMESTAMP())
            GROUP BY 1 ORDER BY 1
        """).to_pandas()
        if len(spcs_credits) > 0:
            total_spcs = spcs_credits["SPCS_CREDITS"].sum()
            st.metric("SPCS Credits (Streamlit Hosting)", f"{total_spcs:.4f}",
                      delta=f"${total_spcs * credit_rate:.2f} USD" if show_usd else None)
            fig_spcs = px.bar(
                spcs_credits, x="USAGE_DATE", y="SPCS_CREDITS",
                labels={"USAGE_DATE": "Date", "SPCS_CREDITS": "Credits"},
                color_discrete_sequence=["#FF4B4B"],
            )
            fig_spcs.update_layout(height=250, margin=dict(l=20, r=20, t=10, b=20))
            st.plotly_chart(fig_spcs, use_container_width=True)
            st.caption("AI_EXTRACT_POC_POOL — hosts the Streamlit container runtime (UI only)")
        else:
            st.info("No SPCS usage for AI_EXTRACT_POC_POOL in this period.")
    except Exception as e:
        st.warning(f"Could not load SPCS credits: {e}")

with st.expander("Full Account Credit Breakdown (all service types)"):
    try:
        breakdown = session.sql(
            f"SELECT * FROM {DB}.V_AI_EXTRACT_COST_BREAKDOWN "
            f"WHERE usage_date >= DATEADD('day', -{day_range}, CURRENT_DATE()) "
            f"ORDER BY usage_date DESC, service_type"
        ).to_pandas()
        if len(breakdown) > 0:
            fig4 = px.bar(
                breakdown, x="USAGE_DATE", y="CREDITS_USED", color="SERVICE_TYPE",
                labels={"USAGE_DATE": "Date", "CREDITS_USED": "Credits", "SERVICE_TYPE": "Service"},
                color_discrete_map={
                    "AI_SERVICES": "#29B5E8",
                    "WAREHOUSE_METERING": "#11567F",
                    "SNOWPARK_CONTAINER_SERVICES": "#FF4B4B",
                },
                barmode="stack",
            )
            fig4.update_layout(height=350, margin=dict(l=20, r=20, t=10, b=20))
            st.plotly_chart(fig4, use_container_width=True)

            if show_usd:
                breakdown["EST_USD"] = breakdown["CREDITS_USED"] * credit_rate
            st.dataframe(
                breakdown, hide_index=True, use_container_width=True,
                column_config={
                    "CREDITS_USED": st.column_config.NumberColumn("Credits Used", format="%.4f"),
                    "CREDITS_BILLED": st.column_config.NumberColumn("Credits Billed", format="%.4f"),
                    **({"EST_USD": st.column_config.NumberColumn("Est. USD", format="$%.4f")} if show_usd else {}),
                },
            )
        else:
            st.info("No metering data in this period.")
    except Exception as e:
        st.warning(f"Could not load credit breakdown: {e}")

with st.expander("Resource Monitor Status"):
    try:
        rm = session.sql("SHOW RESOURCE MONITORS LIKE 'AI_EXTRACT_MONITOR'").to_pandas()
        if len(rm) > 0:
            r = rm.iloc[0]
            rc1, rc2, rc3 = st.columns(3)
            rc1.metric("Credit Quota", r.get("credit_quota", "N/A"))
            rc2.metric("Credits Used", r.get("used_credits", "N/A"))
            rc3.metric("Remaining", r.get("remaining_credits", "N/A"))
        else:
            st.info("No resource monitor configured. See `sql/10_harden.sql`.")
    except Exception:
        st.info("Resource monitor info requires MONITOR privilege on the warehouse.")
