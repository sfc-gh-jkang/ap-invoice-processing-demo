#!/usr/bin/env python3
"""
generate_sample_docs.py — Generate 5 sample invoices for the AI_EXTRACT POC kit.

These are fictional convenience-store distributor invoices with realistic fields
(vendor, invoice number, PO number, dates, line items, totals). They let you
validate the entire POC pipeline without bringing your own documents first.

Usage:
    pip install reportlab   # or: uv add reportlab
    python generate_sample_docs.py

Output:
    poc/sample_documents/sample_invoice_01.pdf ... sample_invoice_05.pdf
"""

import os
import random
from datetime import datetime, timedelta
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.platypus import (
    SimpleDocTemplate,
    Table,
    TableStyle,
    Paragraph,
    Spacer,
)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_RIGHT, TA_CENTER

# ---------------------------------------------------------------------------
# Reference data — fictional vendors and products
# ---------------------------------------------------------------------------

VENDORS = [
    {
        "name": "McLane Company, Inc.",
        "address": "4747 McLane Parkway\nTemple, TX 76504",
        "terms": "Net 15",
    },
    {
        "name": "Core-Mark International",
        "address": "395 Oyster Point Blvd, Suite 415\nSouth San Francisco, CA 94080",
        "terms": "Net 30",
    },
    {
        "name": "Coca-Cola Bottling Co.",
        "address": "One Coca-Cola Plaza\nAtlanta, GA 30313",
        "terms": "Net 30",
    },
    {
        "name": "PepsiCo / Frito-Lay",
        "address": "7701 Legacy Drive\nPlano, TX 75024",
        "terms": "Net 30",
    },
    {
        "name": "Red Bull Distribution",
        "address": "1740 Stewart Street\nSanta Monica, CA 90404",
        "terms": "Net 15",
    },
]

PRODUCTS = [
    ("Beverages", "Coca-Cola Classic 20oz", 1.05, 1.35),
    ("Beverages", "Red Bull 8.4oz", 2.10, 2.60),
    ("Beverages", "Gatorade Fruit Punch 28oz", 1.25, 1.55),
    ("Beverages", "Dasani Water 20oz", 0.60, 0.85),
    ("Beverages", "Monster Energy 16oz", 1.80, 2.20),
    ("Snacks", "Doritos Nacho Cheese 2.75oz", 1.10, 1.45),
    ("Snacks", "Lay's Classic 2.625oz", 1.10, 1.40),
    ("Snacks", "Cheetos Crunchy 3.25oz", 1.15, 1.45),
    ("Snacks", "Takis Fuego 4oz", 1.30, 1.60),
    ("Candy & Gum", "Snickers Bar 1.86oz", 0.95, 1.20),
    ("Candy & Gum", "M&M's Peanut 1.74oz", 0.95, 1.20),
    ("Candy & Gum", "Skittles Original 2.17oz", 0.90, 1.15),
    ("Dairy & Refrigerated", "Fairlife Whole Milk 14oz", 1.80, 2.20),
    ("Dairy & Refrigerated", "Chobani Vanilla Greek Yogurt", 1.20, 1.50),
    ("General Merchandise", "Energizer AA 4-pack", 3.50, 4.20),
    ("General Merchandise", "BIC Classic Lighter 2-pack", 2.00, 2.50),
]


def _random_date(start: datetime, end: datetime) -> datetime:
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))


def _generate_invoice_data(invoice_num: int, vendor: dict, date_start: datetime, date_end: datetime) -> dict:
    invoice_date = _random_date(date_start, date_end)
    terms_days = int(vendor["terms"].split()[-1])
    due_date = invoice_date + timedelta(days=terms_days)

    num_items = random.randint(4, 10)
    selected = random.sample(PRODUCTS, min(num_items, len(PRODUCTS)))

    line_items = []
    for cat, name, price_low, price_high in selected:
        unit_price = round(random.uniform(price_low, price_high), 2)
        qty = random.choice([6, 12, 24, 36, 48])
        line_total = round(unit_price * qty, 2)
        line_items.append({
            "category": cat,
            "product": name,
            "quantity": qty,
            "unit_price": unit_price,
            "line_total": line_total,
        })

    subtotal = round(sum(li["line_total"] for li in line_items), 2)
    tax_rate = round(random.uniform(0.06, 0.09), 4)
    tax = round(subtotal * tax_rate, 2)
    total = round(subtotal + tax, 2)

    return {
        "vendor": vendor,
        "invoice_number": f"INV-{invoice_num:05d}",
        "po_number": f"PO-{random.randint(100000, 999999)}",
        "invoice_date": invoice_date,
        "due_date": due_date,
        "line_items": line_items,
        "subtotal": subtotal,
        "tax_rate": tax_rate,
        "tax": tax,
        "total": total,
    }


def _build_pdf(invoice_data: dict, output_path: str):
    doc = SimpleDocTemplate(
        output_path,
        pagesize=letter,
        leftMargin=0.75 * inch,
        rightMargin=0.75 * inch,
        topMargin=0.5 * inch,
        bottomMargin=0.5 * inch,
    )

    styles = getSampleStyleSheet()
    style_center = ParagraphStyle("Center", parent=styles["Normal"], alignment=TA_CENTER)
    style_title = ParagraphStyle(
        "InvoiceTitle", parent=styles["Heading1"],
        fontSize=20, textColor=colors.HexColor("#1a237e"),
    )
    style_vendor = ParagraphStyle(
        "VendorName", parent=styles["Heading2"],
        fontSize=14, textColor=colors.HexColor("#1a237e"),
    )

    elements = []
    v = invoice_data["vendor"]

    # Header
    elements.append(Paragraph(v["name"], style_vendor))
    elements.append(Paragraph(v["address"].replace("\n", "<br/>"), styles["Normal"]))
    elements.append(Spacer(1, 0.3 * inch))
    elements.append(Paragraph("INVOICE", style_title))
    elements.append(Spacer(1, 0.15 * inch))

    # Invoice details
    details_data = [
        ["Invoice Number:", invoice_data["invoice_number"]],
        ["PO Number:", invoice_data["po_number"]],
        ["Invoice Date:", invoice_data["invoice_date"].strftime("%B %d, %Y")],
        ["Due Date:", invoice_data["due_date"].strftime("%B %d, %Y")],
        ["Payment Terms:", v["terms"]],
    ]
    details_table = Table(details_data, colWidths=[1.8 * inch, 3 * inch])
    details_table.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 10),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
    ]))
    elements.append(details_table)
    elements.append(Spacer(1, 0.3 * inch))

    # Bill To
    store_num = random.randint(100, 999)
    street = random.choice(["Main", "Highway", "Commerce", "Oak", "Elm", "Pine"])
    suffix = random.choice(["St", "Rd", "Blvd", "Ave"])
    city = random.choice(["Springfield", "Riverside", "Fairview", "Madison", "Georgetown"])
    state = random.choice(["GA", "FL", "TX", "TN", "NC", "OH"])
    elements.append(Paragraph("<b>Bill To:</b>", styles["Normal"]))
    elements.append(Paragraph(
        f"QuickStop Convenience Store #{store_num}<br/>"
        f"{random.randint(100, 9999)} {street} {suffix}<br/>"
        f"{city}, {state} {random.randint(30000, 89999)}",
        styles["Normal"],
    ))
    elements.append(Spacer(1, 0.3 * inch))

    # Line items table
    header_row = ["#", "Product", "Category", "Qty", "Unit Price", "Total"]
    table_data = [header_row]
    for i, li in enumerate(invoice_data["line_items"], 1):
        table_data.append([
            str(i), li["product"], li["category"],
            str(li["quantity"]), f"${li['unit_price']:.2f}", f"${li['line_total']:.2f}",
        ])

    col_widths = [0.4 * inch, 2.5 * inch, 1.3 * inch, 0.6 * inch, 0.9 * inch, 0.9 * inch]
    line_table = Table(table_data, colWidths=col_widths)
    line_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1a237e")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 9),
        ("ALIGN", (0, 0), (-1, 0), "CENTER"),
        ("FONTSIZE", (0, 1), (-1, -1), 8),
        ("ALIGN", (0, 1), (0, -1), "CENTER"),
        ("ALIGN", (3, 1), (3, -1), "CENTER"),
        ("ALIGN", (4, 1), (5, -1), "RIGHT"),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f5f5f5")]),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
    ]))
    elements.append(line_table)
    elements.append(Spacer(1, 0.2 * inch))

    # Totals
    totals_data = [
        ["Subtotal:", f"${invoice_data['subtotal']:,.2f}"],
        [f"Tax ({invoice_data['tax_rate']*100:.1f}%):", f"${invoice_data['tax']:,.2f}"],
        ["TOTAL DUE:", f"${invoice_data['total']:,.2f}"],
    ]
    totals_table = Table(totals_data, colWidths=[1.5 * inch, 1.2 * inch])
    totals_table.setStyle(TableStyle([
        ("ALIGN", (0, 0), (0, -1), "RIGHT"),
        ("ALIGN", (1, 0), (1, -1), "RIGHT"),
        ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
        ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
        ("FONTSIZE", (0, -1), (-1, -1), 12),
        ("LINEABOVE", (0, -1), (-1, -1), 1, colors.black),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
    ]))
    totals_wrapper = Table([[None, totals_table]], colWidths=[4.0 * inch, 2.7 * inch])
    elements.append(totals_wrapper)

    elements.append(Spacer(1, 0.4 * inch))
    elements.append(Paragraph(
        f"<i>Payment Terms: {v['terms']} — Please remit by "
        f"{invoice_data['due_date'].strftime('%B %d, %Y')}</i>",
        style_center,
    ))

    doc.build(elements)


def main():
    script_dir = Path(__file__).parent
    output_dir = script_dir / "sample_documents"
    output_dir.mkdir(exist_ok=True)

    # Fixed seed for reproducible output
    random.seed(2025)

    # Date range: recent dates so aging buckets are meaningful
    date_end = datetime(2025, 6, 15)
    date_start = date_end - timedelta(days=90)

    print("Generating 5 sample invoices...")
    for i, vendor in enumerate(VENDORS):
        data = _generate_invoice_data(i + 1, vendor, date_start, date_end)
        path = output_dir / f"sample_invoice_{i + 1:02d}.pdf"
        _build_pdf(data, str(path))
        print(f"  {path.name} - {vendor['name']} - ${data['total']:,.2f}")

    print(f"\nDone! {len(list(output_dir.glob('*.pdf')))} sample invoices in {output_dir}/")
    print("Upload these to your Snowflake stage to test the POC pipeline.")


if __name__ == "__main__":
    main()
