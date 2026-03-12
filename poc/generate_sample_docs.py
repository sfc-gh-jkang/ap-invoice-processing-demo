#!/usr/bin/env python3
"""
generate_sample_docs.py — Generate sample documents for the AI_EXTRACT POC kit.

Generates fictional convenience-store distributor invoices and commercial lease
agreements with realistic fields. They let you validate the entire POC pipeline
without bringing your own documents first.

Usage:
    pip install reportlab   # or: uv add reportlab
    python generate_sample_docs.py

Output:
    poc/sample_documents/sample_invoice_01.pdf ... sample_invoice_05.pdf
    poc/sample_documents/lease_01.pdf ... lease_10.pdf
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


LANDLORDS = [
    {"name": "Apex Commercial Realty LLC", "address": "500 Commerce Blvd, Suite 200\nDallas, TX 75201"},
    {"name": "Harbor Point Properties Inc.", "address": "125 Waterfront Drive\nBoston, MA 02210"},
    {"name": "Pinnacle Asset Management", "address": "8900 Sunset Ave, Floor 12\nLos Angeles, CA 90069"},
    {"name": "Greenfield Development Group", "address": "2200 Innovation Way\nAustin, TX 78701"},
    {"name": "Metro Plaza Holdings Corp.", "address": "1 Penn Plaza, Suite 4500\nNew York, NY 10119"},
    {"name": "Sunbelt Property Partners", "address": "3300 Peachtree Rd NE\nAtlanta, GA 30326"},
    {"name": "Pacific Rim Commercial LLC", "address": "600 Pine Street, Suite 800\nSeattle, WA 98101"},
    {"name": "Crossroads Realty Trust", "address": "445 N Michigan Ave\nChicago, IL 60611"},
    {"name": "Brightstone Capital Properties", "address": "1200 17th Street, Suite 1000\nDenver, CO 80202"},
    {"name": "Riverstone Retail Partners", "address": "700 Lavaca Street\nAustin, TX 78701"},
]

TENANT_STORES = [
    "QuickStop Convenience #{num}",
    "EZ Mart #{num}",
    "Corner Express #{num}",
    "FastFuel & Go #{num}",
    "Daily Stop #{num}",
]


def _generate_lease_data(lease_idx: int, date_start: datetime, date_end: datetime) -> dict:
    landlord = LANDLORDS[lease_idx % len(LANDLORDS)]
    store_num = random.randint(100, 999)
    tenant = random.choice(TENANT_STORES).replace("{num}", str(store_num))

    start_date = _random_date(date_start, date_end)
    term_months = random.choice([12, 24, 36, 48, 60])
    end_date = start_date + timedelta(days=term_months * 30)

    monthly_rent = round(random.uniform(1500, 8500), 2)
    security_deposit = round(monthly_rent * random.choice([1, 1.5, 2]), 2)
    payment_due_day = random.choice([1, 5, 10, 15])
    late_fee = round(random.uniform(50, 250), 2)
    total_lease_value = round(monthly_rent * term_months, 2)

    street = random.choice(["Main", "Highway", "Commerce", "Oak", "Elm", "Industrial"])
    suffix = random.choice(["St", "Rd", "Blvd", "Ave", "Dr"])
    city = random.choice(["Springfield", "Riverside", "Fairview", "Madison", "Georgetown", "Lakewood"])
    state = random.choice(["GA", "FL", "TX", "TN", "NC", "OH", "CA", "NY", "IL", "CO"])

    return {
        "landlord": landlord,
        "tenant": tenant,
        "lease_number": f"LSE-{random.randint(10000, 99999)}",
        "property_address": f"{random.randint(100, 9999)} {street} {suffix}\n{city}, {state} {random.randint(30000, 89999)}",
        "lease_start_date": start_date,
        "lease_end_date": end_date,
        "lease_term_months": term_months,
        "monthly_rent": monthly_rent,
        "security_deposit": security_deposit,
        "payment_due_day": payment_due_day,
        "late_fee": late_fee,
        "total_lease_value": total_lease_value,
    }


def _build_lease_pdf(data: dict, output_path: str):
    doc_pdf = SimpleDocTemplate(
        output_path,
        pagesize=letter,
        leftMargin=0.75 * inch,
        rightMargin=0.75 * inch,
        topMargin=0.5 * inch,
        bottomMargin=0.5 * inch,
    )

    styles = getSampleStyleSheet()
    style_center = ParagraphStyle("LeaseCenter", parent=styles["Normal"], alignment=TA_CENTER)
    style_title = ParagraphStyle(
        "LeaseTitle", parent=styles["Heading1"],
        fontSize=22, textColor=colors.HexColor("#1b5e20"), alignment=TA_CENTER,
    )
    style_section = ParagraphStyle(
        "LeaseSection", parent=styles["Heading2"],
        fontSize=12, textColor=colors.HexColor("#1b5e20"), spaceBefore=12, spaceAfter=6,
    )

    elements = []

    elements.append(Paragraph("COMMERCIAL LEASE AGREEMENT", style_title))
    elements.append(Spacer(1, 0.15 * inch))
    elements.append(Paragraph(f"Lease Number: <b>{data['lease_number']}</b>", style_center))
    elements.append(Spacer(1, 0.3 * inch))

    elements.append(Paragraph("PARTIES", style_section))
    elements.append(Paragraph(
        f"<b>Landlord:</b> {data['landlord']['name']}<br/>"
        f"{data['landlord']['address'].replace(chr(10), '<br/>')}",
        styles["Normal"],
    ))
    elements.append(Spacer(1, 0.1 * inch))
    elements.append(Paragraph(f"<b>Tenant:</b> {data['tenant']}", styles["Normal"]))
    elements.append(Spacer(1, 0.2 * inch))

    elements.append(Paragraph("PROPERTY", style_section))
    elements.append(Paragraph(
        f"<b>Property Address:</b><br/>{data['property_address'].replace(chr(10), '<br/>')}",
        styles["Normal"],
    ))
    elements.append(Spacer(1, 0.2 * inch))

    elements.append(Paragraph("LEASE TERMS", style_section))
    terms_data = [
        ["Lease Start Date:", data["lease_start_date"].strftime("%B %d, %Y")],
        ["Lease End Date:", data["lease_end_date"].strftime("%B %d, %Y")],
        ["Lease Term:", f"{data['lease_term_months']} months"],
        ["Monthly Base Rent:", f"${data['monthly_rent']:,.2f}"],
        ["Security Deposit:", f"${data['security_deposit']:,.2f}"],
        ["Rent Due Day:", f"{data['payment_due_day']}{'st' if data['payment_due_day'] == 1 else 'th'} of each month"],
        ["Late Payment Fee:", f"${data['late_fee']:,.2f}"],
        ["Total Lease Value:", f"${data['total_lease_value']:,.2f}"],
    ]
    terms_table = Table(terms_data, colWidths=[2.0 * inch, 3.5 * inch])
    terms_table.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 10),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#cccccc")),
        ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#e8f5e9")),
    ]))
    elements.append(terms_table)
    elements.append(Spacer(1, 0.3 * inch))

    elements.append(Paragraph("RENT SCHEDULE", style_section))
    schedule_header = ["Month", "Base Rent", "Escalation", "Total"]
    schedule_rows = [schedule_header]
    escalation_pct = round(random.uniform(0.02, 0.05), 3)
    for yr in range(min(data["lease_term_months"] // 12, 5)):
        esc = round(data["monthly_rent"] * (escalation_pct * yr), 2)
        total_m = round(data["monthly_rent"] + esc, 2)
        label = f"Year {yr + 1}" if data["lease_term_months"] > 12 else f"Months 1-{data['lease_term_months']}"
        schedule_rows.append([label, f"${data['monthly_rent']:,.2f}", f"${esc:,.2f}", f"${total_m:,.2f}"])

    if len(schedule_rows) > 1:
        sched_table = Table(schedule_rows, colWidths=[1.5 * inch, 1.5 * inch, 1.5 * inch, 1.5 * inch])
        sched_table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1b5e20")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
            ("ALIGN", (1, 0), (-1, -1), "RIGHT"),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f1f8e9")]),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
        ]))
        elements.append(sched_table)

    elements.append(Spacer(1, 0.4 * inch))
    elements.append(Paragraph(
        "<i>This lease agreement is binding upon execution by both parties. "
        "Rent is due on the date specified above. A late fee will be assessed "
        "for payments received after a 5-day grace period.</i>",
        style_center,
    ))

    doc_pdf.build(elements)


def main():
    script_dir = Path(__file__).parent
    output_dir = script_dir / "sample_documents"
    output_dir.mkdir(exist_ok=True)

    # Fixed seed for reproducible output
    random.seed(2025)

    # Date range: spread across the last 90 days from today so aging buckets
    # (Current, 1-30, 31-60, 61-90) are always populated with meaningful data.
    date_end = datetime.now()
    date_start = date_end - timedelta(days=90)

    print("Generating 5 sample invoices...")
    for i, vendor in enumerate(VENDORS):
        data = _generate_invoice_data(i + 1, vendor, date_start, date_end)
        path = output_dir / f"sample_invoice_{i + 1:02d}.pdf"
        _build_pdf(data, str(path))
        print(f"  {path.name} - {vendor['name']} - ${data['total']:,.2f}")

    print("\nGenerating 10 sample leases...")
    for i in range(10):
        data = _generate_lease_data(i, date_start, date_end)
        path = output_dir / f"lease_{i + 1:02d}.pdf"
        _build_lease_pdf(data, str(path))
        print(f"  {path.name} - {data['landlord']['name']} - ${data['monthly_rent']:,.2f}/mo")

    all_pdfs = list(output_dir.glob("*.pdf"))
    print(f"\nDone! {len(all_pdfs)} sample documents in {output_dir}/")
    print("Upload these to your Snowflake stage to test the POC pipeline.")


if __name__ == "__main__":
    main()
