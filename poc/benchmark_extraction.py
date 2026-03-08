#!/usr/bin/env python3
"""Performance benchmark for AI_EXTRACT extraction pipeline.

Measures wall-clock time for key operations at various document counts.
Produces a JSON report and prints a summary table.

Usage:
    # Run against current data (no new docs generated)
    POC_CONNECTION=default python benchmark_extraction.py

    # Generate N additional invoices first, then benchmark
    POC_CONNECTION=default python benchmark_extraction.py --generate 370

Environment variables:
    POC_CONNECTION  Snowflake connection name (required)
    POC_DB          Database name     (default: AI_EXTRACT_POC)
    POC_SCHEMA      Schema name       (default: DOCUMENTS)
    POC_WH          Warehouse name    (default: AI_EXTRACT_WH)
    POC_ROLE        Role name         (default: AI_EXTRACT_APP)
"""

import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import snowflake.connector

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
CONNECTION = os.environ.get("POC_CONNECTION", "default")
DB = os.environ.get("POC_DB", "AI_EXTRACT_POC")
SCHEMA = os.environ.get("POC_SCHEMA", "DOCUMENTS")
WH = os.environ.get("POC_WH", "AI_EXTRACT_WH")
ROLE = os.environ.get("POC_ROLE", "AI_EXTRACT_APP")
FQ = f"{DB}.{SCHEMA}"

STAGE = f"@{FQ}.DOCUMENT_STAGE"
POC_DIR = Path(__file__).resolve().parent


def get_connection():
    conn = snowflake.connector.connect(connection_name=CONNECTION)
    cur = conn.cursor()
    cur.execute(f"USE ROLE {ROLE}")
    cur.execute(f"USE DATABASE {DB}")
    cur.execute(f"USE SCHEMA {SCHEMA}")
    cur.execute(f"USE WAREHOUSE {WH}")
    return conn, cur


def timed(label, func, *args, **kwargs):
    """Run func, print elapsed time, return (result, elapsed_seconds)."""
    print(f"  {label} ...", end="", flush=True)
    start = time.perf_counter()
    result = func(*args, **kwargs)
    elapsed = time.perf_counter() - start
    print(f" {elapsed:.1f}s")
    return result, elapsed


# ---------------------------------------------------------------------------
# Benchmark operations
# ---------------------------------------------------------------------------
def count_documents(cur):
    cur.execute(f"SELECT COUNT(*) FROM {FQ}.RAW_DOCUMENTS")
    return cur.fetchone()[0]


def count_extracted(cur):
    cur.execute(f"SELECT COUNT(*) FROM {FQ}.EXTRACTED_FIELDS")
    return cur.fetchone()[0]


def count_table_data(cur):
    cur.execute(f"SELECT COUNT(*) FROM {FQ}.EXTRACTED_TABLE_DATA")
    return cur.fetchone()[0]


def bench_view_query(cur):
    """Time V_INVOICE_SUMMARY full scan."""
    cur.execute(f"SELECT COUNT(*) FROM {FQ}.V_INVOICE_SUMMARY")
    return cur.fetchone()[0]


def bench_view_with_filter(cur):
    """Time V_INVOICE_SUMMARY with a WHERE filter (simulates dashboard)."""
    cur.execute(
        f"SELECT * FROM {FQ}.V_INVOICE_SUMMARY "
        f"WHERE doc_type = 'INVOICE' LIMIT 50"
    )
    return len(cur.fetchall())


def bench_extracted_fields_scan(cur):
    """Full scan of EXTRACTED_FIELDS."""
    cur.execute(f"SELECT * FROM {FQ}.EXTRACTED_FIELDS")
    return len(cur.fetchall())


def bench_table_data_scan(cur):
    """Full scan of EXTRACTED_TABLE_DATA."""
    cur.execute(f"SELECT * FROM {FQ}.EXTRACTED_TABLE_DATA")
    return len(cur.fetchall())


def bench_doc_type_summary(cur):
    """Aggregate query by doc_type (joined from RAW_DOCUMENTS)."""
    cur.execute(
        f"SELECT r.doc_type, COUNT(*) AS cnt "
        f"FROM {FQ}.RAW_DOCUMENTS r "
        f"JOIN {FQ}.EXTRACTED_FIELDS e ON r.file_name = e.file_name "
        f"GROUP BY r.doc_type"
    )
    return cur.fetchall()


def bench_extraction_sp(cur, doc_type="ALL"):
    """Call SP_EXTRACT_BY_DOC_TYPE and time the full extraction."""
    cur.execute(f"CALL {FQ}.SP_EXTRACT_BY_DOC_TYPE('{doc_type}')")
    return cur.fetchone()[0]


def bench_stage_list(cur):
    """List files on the document stage."""
    cur.execute(f"LIST {STAGE}")
    return len(cur.fetchall())


# ---------------------------------------------------------------------------
# Generate additional invoices
# ---------------------------------------------------------------------------
def generate_invoices(n, cur):
    """Generate n additional invoice PDFs and upload to stage."""
    import random
    import string

    # Use the existing generator as a subprocess
    gen_script = POC_DIR / "generate_sample_docs.py"
    if not gen_script.exists():
        print(f"  WARNING: {gen_script} not found — skipping generation")
        return 0

    # Create temp dir for new invoices
    tmp_dir = POC_DIR / "sample_documents" / "_benchmark_tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    # Generate simple invoices using reportlab directly
    try:
        from reportlab.lib.pagesizes import letter
        from reportlab.pdfgen import canvas
    except ImportError:
        print("  WARNING: reportlab not installed — skipping generation")
        return 0

    vendors = [
        "Acme Supply Co", "Metro Distributors", "Northeast Foods",
        "Garden State Wholesale", "Empire Logistics", "Hudson Trading",
        "Liberty Supply Chain", "Atlantic Imports", "Tri-State Products",
        "Keystone Distribution",
    ]

    uploaded = 0
    for i in range(n):
        fname = f"bench_invoice_{i+1:04d}.pdf"
        fpath = tmp_dir / fname
        inv_num = f"BENCH-{i+1:05d}"
        vendor = vendors[i % len(vendors)]
        amount = round(random.uniform(100, 50000), 2)
        date_str = f"2025-{random.randint(1,12):02d}-{random.randint(1,28):02d}"

        c = canvas.Canvas(str(fpath), pagesize=letter)
        c.setFont("Helvetica-Bold", 16)
        c.drawString(72, 720, "INVOICE")
        c.setFont("Helvetica", 11)
        c.drawString(72, 700, f"Invoice Number: {inv_num}")
        c.drawString(72, 685, f"Date: {date_str}")
        c.drawString(72, 670, f"Vendor: {vendor}")
        c.drawString(72, 640, f"Bill To: Benchmark Test Corp")
        c.drawString(72, 610, f"Description: Monthly supply order #{i+1}")

        # Simple line items
        y = 570
        c.setFont("Helvetica-Bold", 10)
        c.drawString(72, y, "Item")
        c.drawString(300, y, "Qty")
        c.drawString(370, y, "Unit Price")
        c.drawString(470, y, "Total")
        y -= 20
        c.setFont("Helvetica", 10)
        num_items = random.randint(2, 6)
        subtotal = 0
        for j in range(num_items):
            item_name = f"Product {string.ascii_uppercase[j]}"
            qty = random.randint(1, 100)
            unit_price = round(random.uniform(5, 500), 2)
            line_total = round(qty * unit_price, 2)
            subtotal += line_total
            c.drawString(72, y, item_name)
            c.drawString(300, y, str(qty))
            c.drawString(370, y, f"${unit_price:,.2f}")
            c.drawString(470, y, f"${line_total:,.2f}")
            y -= 18

        tax = round(subtotal * 0.08875, 2)
        total = round(subtotal + tax, 2)
        y -= 10
        c.drawString(370, y, f"Subtotal: ${subtotal:,.2f}")
        y -= 18
        c.drawString(370, y, f"Tax (8.875%): ${tax:,.2f}")
        y -= 18
        c.setFont("Helvetica-Bold", 11)
        c.drawString(370, y, f"Total Due: ${total:,.2f}")

        c.save()

    # Upload all to stage
    print(f"  Uploading {n} benchmark invoices to stage ...", end="", flush=True)
    start = time.perf_counter()
    cur.execute(
        f"PUT 'file://{tmp_dir}/*.pdf' {STAGE}/invoices/ "
        f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
    )
    upload_time = time.perf_counter() - start
    print(f" {upload_time:.1f}s")

    # Register in RAW_DOCUMENTS
    print(f"  Registering documents ...", end="", flush=True)
    start = time.perf_counter()
    cur.execute(f"ALTER STAGE {FQ}.DOCUMENT_STAGE REFRESH")
    cur.execute(f"""
        INSERT INTO {FQ}.RAW_DOCUMENTS (file_name, file_path, doc_type)
        SELECT
            RELATIVE_PATH,
            BUILD_SCOPED_FILE_URL({STAGE}, RELATIVE_PATH),
            'INVOICE'
        FROM DIRECTORY({STAGE})
        WHERE RELATIVE_PATH LIKE 'invoices/bench_invoice_%'
          AND RELATIVE_PATH NOT IN (
              SELECT file_name FROM {FQ}.RAW_DOCUMENTS
          )
    """)
    reg_time = time.perf_counter() - start
    uploaded = cur.rowcount or 0
    print(f" {reg_time:.1f}s ({uploaded} new rows)")

    return uploaded


def cleanup_benchmark_docs(cur):
    """Remove benchmark-generated documents."""
    bench_pattern = '%bench_invoice_%'
    cur.execute(
        f"DELETE FROM {FQ}.EXTRACTED_TABLE_DATA "
        f"WHERE file_name LIKE '{bench_pattern}'"
    )
    td_deleted = cur.rowcount or 0
    cur.execute(
        f"DELETE FROM {FQ}.EXTRACTED_FIELDS "
        f"WHERE file_name LIKE '{bench_pattern}'"
    )
    ef_deleted = cur.rowcount or 0
    cur.execute(
        f"DELETE FROM {FQ}.RAW_DOCUMENTS "
        f"WHERE file_name LIKE '{bench_pattern}'"
    )
    rd_deleted = cur.rowcount or 0
    print(f"  Cleaned up {rd_deleted} RAW_DOCUMENTS, "
          f"{ef_deleted} EXTRACTED_FIELDS, {td_deleted} TABLE_DATA rows")

    # Clean up temp files
    tmp_dir = POC_DIR / "sample_documents" / "_benchmark_tmp"
    if tmp_dir.exists():
        import shutil
        shutil.rmtree(tmp_dir)
        print(f"  Removed temp directory {tmp_dir}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def run_benchmark(generate_count=0, cleanup=False):
    conn, cur = get_connection()
    results = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "connection": CONNECTION,
        "database": DB,
        "warehouse": WH,
        "benchmarks": {},
    }

    try:
        # Suspend the background task to avoid interference
        print("Suspending EXTRACT_NEW_DOCUMENTS_TASK ...")
        try:
            cur.execute(
                f"ALTER TASK {FQ}.EXTRACT_NEW_DOCUMENTS_TASK SUSPEND"
            )
        except Exception:
            pass  # Task may not exist in all deployments

        # Pre-benchmark counts
        doc_count, _ = timed("Count RAW_DOCUMENTS", count_documents, cur)
        ef_count, _ = timed("Count EXTRACTED_FIELDS", count_extracted, cur)
        td_count, _ = timed("Count EXTRACTED_TABLE_DATA", count_table_data, cur)

        results["pre_counts"] = {
            "raw_documents": doc_count,
            "extracted_fields": ef_count,
            "extracted_table_data": td_count,
        }

        print(f"\n--- Pre-benchmark: {doc_count} docs, "
              f"{ef_count} extracted, {td_count} table rows ---\n")

        # Generate additional docs if requested
        if generate_count > 0:
            print(f"Generating {generate_count} additional invoices ...")
            uploaded = generate_invoices(generate_count, cur)
            doc_count, _ = timed("Recount RAW_DOCUMENTS", count_documents, cur)
            results["generated"] = uploaded
            print()

        # --- Read benchmarks ---
        print("Read benchmarks:")
        _, t = timed("V_INVOICE_SUMMARY full count", bench_view_query, cur)
        results["benchmarks"]["view_full_count"] = t

        _, t = timed("V_INVOICE_SUMMARY filtered query", bench_view_with_filter, cur)
        results["benchmarks"]["view_filtered"] = t

        _, t = timed("EXTRACTED_FIELDS full scan", bench_extracted_fields_scan, cur)
        results["benchmarks"]["ef_full_scan"] = t

        _, t = timed("EXTRACTED_TABLE_DATA full scan", bench_table_data_scan, cur)
        results["benchmarks"]["td_full_scan"] = t

        _, t = timed("Doc type summary aggregate", bench_doc_type_summary, cur)
        results["benchmarks"]["doc_type_aggregate"] = t

        _, t = timed("Stage LIST", bench_stage_list, cur)
        results["benchmarks"]["stage_list"] = t

        # --- Extraction benchmark (only if there are unextracted docs) ---
        cur.execute(
            f"SELECT COUNT(*) FROM {FQ}.RAW_DOCUMENTS "
            f"WHERE EXTRACTED = FALSE OR EXTRACTED IS NULL"
        )
        unextracted = cur.fetchone()[0]
        if unextracted > 0:
            print(f"\nExtraction benchmark ({unextracted} unextracted docs):")
            _, t = timed(
                f"SP_EXTRACT_BY_DOC_TYPE('ALL') — {unextracted} docs",
                bench_extraction_sp, cur, "ALL"
            )
            results["benchmarks"]["extraction_all"] = t
            results["benchmarks"]["extraction_doc_count"] = unextracted
            results["benchmarks"]["extraction_per_doc"] = round(
                t / unextracted, 2
            ) if unextracted > 0 else None
        else:
            print("\nNo unextracted documents — skipping extraction benchmark.")

        # Post-benchmark counts
        doc_count_post, _ = timed("\nPost-count RAW_DOCUMENTS", count_documents, cur)
        ef_count_post, _ = timed("Post-count EXTRACTED_FIELDS", count_extracted, cur)
        td_count_post, _ = timed("Post-count EXTRACTED_TABLE_DATA", count_table_data, cur)

        results["post_counts"] = {
            "raw_documents": doc_count_post,
            "extracted_fields": ef_count_post,
            "extracted_table_data": td_count_post,
        }

        # Cleanup if requested
        if cleanup:
            print("\nCleaning up benchmark data ...")
            cleanup_benchmark_docs(cur)

        # Resume task
        print("\nResuming EXTRACT_NEW_DOCUMENTS_TASK ...")
        try:
            cur.execute(
                f"ALTER TASK {FQ}.EXTRACT_NEW_DOCUMENTS_TASK RESUME"
            )
        except Exception:
            pass

    finally:
        cur.close()
        conn.close()

    # Save report
    report_path = POC_DIR / "benchmark_results.json"
    with open(report_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nReport saved to {report_path}")

    # Print summary table
    print("\n" + "=" * 60)
    print(f"BENCHMARK SUMMARY  ({results['timestamp']})")
    print(f"Database: {DB}  Warehouse: {WH}")
    print(f"Documents: {results.get('post_counts', results['pre_counts'])['raw_documents']}")
    print("=" * 60)
    print(f"{'Operation':<40} {'Time (s)':>10}")
    print("-" * 60)
    for key, val in results["benchmarks"].items():
        if isinstance(val, (int, float)):
            print(f"  {key:<38} {val:>10.2f}")
    print("=" * 60)

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AI_EXTRACT performance benchmark")
    parser.add_argument(
        "--generate", type=int, default=0,
        help="Number of additional invoices to generate before benchmarking"
    )
    parser.add_argument(
        "--cleanup", action="store_true",
        help="Remove benchmark-generated documents after the run"
    )
    args = parser.parse_args()
    run_benchmark(generate_count=args.generate, cleanup=args.cleanup)
