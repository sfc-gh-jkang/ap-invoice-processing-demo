"""
generate_utility_bills.py — Generate 10 sample electrical utility bills for
the AI_EXTRACT POC (NY/NJ providers).

Creates realistic PDFs with varied layouts to stress-test config-driven
extraction. Each bill's ground truth is printed to stdout as JSON for
validation after extraction.

Usage:
    python generate_utility_bills.py

Output:
    poc/sample_documents/utility_bill_01.pdf ... utility_bill_10.pdf
    Ground truth JSON printed to stdout
"""

import json
import os
import random
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
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
    HRFlowable,
)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_RIGHT, TA_CENTER

# ---------------------------------------------------------------------------
# Reference data — real NY/NJ electrical utility providers
# ---------------------------------------------------------------------------

PROVIDERS = [
    {
        "name": "Consolidated Edison",
        "short": "Con Edison",
        "address": "4 Irving Place\nNew York, NY 10003",
        "phone": "(800) 752-6633",
        "brand_color": "#0055A5",
        "rate_schedule": "SC1 - Residential",
        "service_territory": "NYC / Westchester",
    },
    {
        "name": "Public Service Electric and Gas",
        "short": "PSE&G",
        "address": "80 Park Plaza\nNewark, NJ 07102",
        "phone": "(800) 436-7734",
        "brand_color": "#00703C",
        "rate_schedule": "RS - Residential Service",
        "service_territory": "Northern / Central NJ",
    },
    {
        "name": "National Grid",
        "short": "National Grid",
        "address": "300 Erie Boulevard West\nSyracuse, NY 13202",
        "phone": "(800) 642-4272",
        "brand_color": "#003D6B",
        "rate_schedule": "SC1 - Residential",
        "service_territory": "Upstate NY / Brooklyn / Queens / Staten Island",
    },
    {
        "name": "Orange and Rockland Utilities",
        "short": "O&R",
        "address": "390 West Route 59\nSpring Valley, NY 10977",
        "phone": "(877) 434-4100",
        "brand_color": "#FF6600",
        "rate_schedule": "SC1 - Residential",
        "service_territory": "Orange / Rockland / Bergen Counties",
    },
    {
        "name": "Jersey Central Power & Light",
        "short": "JCP&L",
        "address": "300 Madison Avenue\nMorristown, NJ 07962",
        "phone": "(800) 662-3115",
        "brand_color": "#1E3A5F",
        "rate_schedule": "RS - Residential Service",
        "service_territory": "Central / Northern NJ",
    },
]

# NY/NJ addresses for service locations
SERVICE_ADDRESSES = [
    "142 W 72nd St, Apt 4B, New York, NY 10023",
    "87-15 Queens Blvd, Apt 12C, Elmhurst, NY 11373",
    "315 Atlantic Ave, Brooklyn, NY 11201",
    "4401 Bergenline Ave, Union City, NJ 07087",
    "22 Maple Avenue, Montclair, NJ 07042",
    "1560 Richmond Road, Staten Island, NY 10304",
    "89 Prospect St, Ridgewood, NJ 07450",
    "211 Main Street, White Plains, NY 10601",
    "47 Warwick Turnpike, West Milford, NJ 07480",
    "630 Grand Concourse, Apt 8F, Bronx, NY 10451",
]

# Tiered rate structures (realistic ConEd / PSE&G style)
RATE_TIERS_SUMMER = [
    {"tier": "First 250 kWh", "range": "0 - 250", "rate": 0.1045},
    {"tier": "Next 500 kWh", "range": "251 - 750", "rate": 0.1215},
    {"tier": "Over 750 kWh", "range": "751+", "rate": 0.1458},
]

RATE_TIERS_WINTER = [
    {"tier": "First 250 kWh", "range": "0 - 250", "rate": 0.0985},
    {"tier": "Next 500 kWh", "range": "251 - 750", "rate": 0.1105},
    {"tier": "Over 750 kWh", "range": "751+", "rate": 0.1290},
]


def _d(val):
    """Round to 2 decimal places."""
    return float(Decimal(str(val)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def _generate_bill_data(bill_num: int, provider: dict, address: str) -> dict:
    """Generate one utility bill's data with ground truth."""
    random.seed(2026_03 + bill_num)

    # Billing period — roughly monthly, recent
    period_end = datetime(2026, 2, 1) + timedelta(days=random.randint(0, 25))
    period_start = period_end - timedelta(days=random.randint(28, 32))

    due_date = period_end + timedelta(days=random.randint(18, 25))

    # Account/meter numbers
    account_number = f"{random.randint(10, 99)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}-{random.randint(10, 99)}"
    meter_number = f"{random.choice(['E','M','K'])}{random.randint(100000, 999999)}"

    # Usage
    is_summer = period_end.month in (6, 7, 8, 9)
    kwh_usage = random.randint(180, 1400)
    demand_kw = _d(random.uniform(1.5, 8.5)) if kwh_usage > 600 else 0.0

    # Calculate tiered charges
    tiers = RATE_TIERS_SUMMER if is_summer else RATE_TIERS_WINTER
    remaining = kwh_usage
    tier_details = []
    total_energy_charge = 0.0

    for t in tiers:
        if remaining <= 0:
            break
        if t["tier"].startswith("First"):
            tier_kwh = min(remaining, 250)
        elif t["tier"].startswith("Next"):
            tier_kwh = min(remaining, 500)
        else:
            tier_kwh = remaining

        charge = _d(tier_kwh * t["rate"])
        total_energy_charge += charge
        tier_details.append({
            "tier": t["tier"],
            "range": t["range"],
            "rate": f"${t['rate']:.4f}",
            "kwh": tier_kwh,
            "amount": charge,
        })
        remaining -= tier_kwh

    total_energy_charge = _d(total_energy_charge)

    # Additional charges
    delivery_charge = _d(random.uniform(15.0, 45.0))
    system_benefit = _d(random.uniform(2.50, 8.00))
    taxes_surcharges = _d(random.uniform(5.0, 22.0))

    current_charges = _d(total_energy_charge + delivery_charge + system_benefit + taxes_surcharges)
    previous_balance = _d(random.choice([0.0, 0.0, random.uniform(50.0, 250.0)]))
    total_due = _d(current_charges + previous_balance)

    return {
        "bill_num": bill_num,
        "provider": provider,
        "account_number": account_number,
        "meter_number": meter_number,
        "service_address": address,
        "billing_period_start": period_start,
        "billing_period_end": period_end,
        "rate_schedule": provider["rate_schedule"],
        "kwh_usage": kwh_usage,
        "demand_kw": demand_kw,
        "previous_balance": previous_balance,
        "current_charges": current_charges,
        "total_due": total_due,
        "due_date": due_date,
        "tier_details": tier_details,
        "delivery_charge": delivery_charge,
        "system_benefit": system_benefit,
        "taxes_surcharges": taxes_surcharges,
        "total_energy_charge": total_energy_charge,
    }


def _build_conedison_style(data: dict, output_path: str):
    """ConEdison-style bill layout — blue header, account summary box."""
    doc = SimpleDocTemplate(
        output_path, pagesize=letter,
        leftMargin=0.6 * inch, rightMargin=0.6 * inch,
        topMargin=0.5 * inch, bottomMargin=0.5 * inch,
    )
    styles = getSampleStyleSheet()
    bc = data["provider"]["brand_color"]
    s_title = ParagraphStyle("Title", parent=styles["Heading1"], fontSize=18,
                             textColor=colors.HexColor(bc))
    s_sub = ParagraphStyle("Sub", parent=styles["Heading3"], fontSize=11,
                           textColor=colors.HexColor(bc))
    s_right = ParagraphStyle("Right", parent=styles["Normal"], alignment=TA_RIGHT)
    s_small = ParagraphStyle("Small", parent=styles["Normal"], fontSize=8,
                             textColor=colors.grey)

    elements = []

    # Header
    header_data = [
        [Paragraph(f"<b>{data['provider']['name']}</b>", s_title),
         Paragraph(f"<b>ELECTRIC BILL</b>", s_title)],
        [Paragraph(data["provider"]["address"].replace("\n", "<br/>"), styles["Normal"]),
         Paragraph(f"Statement Date: {data['billing_period_end'].strftime('%B %d, %Y')}", s_right)],
    ]
    ht = Table(header_data, colWidths=[3.5 * inch, 3.5 * inch])
    ht.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
    ]))
    elements.append(ht)
    elements.append(HRFlowable(width="100%", thickness=2, color=colors.HexColor(bc)))
    elements.append(Spacer(1, 0.15 * inch))

    # Account info box
    elements.append(Paragraph("Account Information", s_sub))
    acct_data = [
        ["Account Number:", data["account_number"], "Meter Number:", data["meter_number"]],
        ["Service Address:", data["service_address"], "", ""],
        ["Billing Period:", f"{data['billing_period_start'].strftime('%m/%d/%Y')} - {data['billing_period_end'].strftime('%m/%d/%Y')}",
         "Rate Schedule:", data["rate_schedule"]],
    ]
    at = Table(acct_data, colWidths=[1.3 * inch, 2.5 * inch, 1.2 * inch, 2.0 * inch])
    at.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
        ("FONTNAME", (2, 0), (2, -1), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor(bc)),
        ("INNERGRID", (0, 0), (-1, -1), 0.25, colors.lightgrey),
        ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#f0f5ff")),
    ]))
    elements.append(at)
    elements.append(Spacer(1, 0.2 * inch))

    # Usage summary
    elements.append(Paragraph("Usage Summary", s_sub))
    usage_data = [
        ["Total kWh Used:", f"{data['kwh_usage']:,} kWh"],
    ]
    if data["demand_kw"] > 0:
        usage_data.append(["Peak Demand:", f"{data['demand_kw']:.1f} kW"])
    ut = Table(usage_data, colWidths=[1.8 * inch, 2.5 * inch])
    ut.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 10),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]))
    elements.append(ut)
    elements.append(Spacer(1, 0.15 * inch))

    # Rate tier table
    elements.append(Paragraph("Electricity Supply Charges", s_sub))
    tier_header = ["Tier", "kWh Range", "Rate per kWh", "Amount"]
    tier_rows = [tier_header]
    for td in data["tier_details"]:
        tier_rows.append([td["tier"], td["range"], td["rate"], f"${td['amount']:.2f}"])

    tt = Table(tier_rows, colWidths=[1.5 * inch, 1.5 * inch, 1.3 * inch, 1.2 * inch])
    tt.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor(bc)),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("ALIGN", (2, 0), (3, -1), "RIGHT"),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f5f5f5")]),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
    ]))
    elements.append(tt)
    elements.append(Spacer(1, 0.2 * inch))

    # Charges breakdown
    elements.append(Paragraph("Charges Summary", s_sub))
    charges_data = [
        ["Energy Supply Charges:", f"${data['total_energy_charge']:,.2f}"],
        ["Delivery Charges:", f"${data['delivery_charge']:,.2f}"],
        ["System Benefits Charge:", f"${data['system_benefit']:,.2f}"],
        ["Taxes & Surcharges:", f"${data['taxes_surcharges']:,.2f}"],
        ["Current Charges:", f"${data['current_charges']:,.2f}"],
    ]
    if data["previous_balance"] > 0:
        charges_data.append(["Previous Balance:", f"${data['previous_balance']:,.2f}"])
    charges_data.append(["TOTAL AMOUNT DUE:", f"${data['total_due']:,.2f}"])

    ct = Table(charges_data, colWidths=[2.5 * inch, 1.5 * inch])
    ct.setStyle(TableStyle([
        ("ALIGN", (0, 0), (0, -1), "RIGHT"),
        ("ALIGN", (1, 0), (1, -1), "RIGHT"),
        ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
        ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
        ("FONTSIZE", (0, -1), (-1, -1), 12),
        ("LINEABOVE", (0, -1), (-1, -1), 1.5, colors.HexColor(bc)),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
    ]))
    charges_wrapper = Table([[None, ct]], colWidths=[3.0 * inch, 4.0 * inch])
    elements.append(charges_wrapper)
    elements.append(Spacer(1, 0.3 * inch))

    # Due date
    elements.append(Paragraph(
        f"<b>Payment Due Date: {data['due_date'].strftime('%B %d, %Y')}</b>",
        ParagraphStyle("DueDate", parent=styles["Normal"], fontSize=11,
                       textColor=colors.HexColor("#CC0000")),
    ))
    elements.append(Spacer(1, 0.15 * inch))
    elements.append(Paragraph(
        f"For questions about your bill, call {data['provider']['phone']}",
        s_small,
    ))

    doc.build(elements)


def _build_pseg_style(data: dict, output_path: str):
    """PSE&G / JCP&L style — green/dark header, different field label wording."""
    doc = SimpleDocTemplate(
        output_path, pagesize=letter,
        leftMargin=0.7 * inch, rightMargin=0.7 * inch,
        topMargin=0.5 * inch, bottomMargin=0.5 * inch,
    )
    styles = getSampleStyleSheet()
    bc = data["provider"]["brand_color"]
    s_header = ParagraphStyle("Header", parent=styles["Heading1"], fontSize=16,
                              textColor=colors.white)
    s_sub = ParagraphStyle("Sub", parent=styles["Heading3"], fontSize=10,
                           textColor=colors.HexColor(bc))

    elements = []

    # Big colored header bar
    hdr_tbl = Table(
        [[Paragraph(f"{data['provider']['short']} — Monthly Electric Statement", s_header)]],
        colWidths=[7.0 * inch],
    )
    hdr_tbl.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor(bc)),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 8),
        ("LEFTPADDING", (0, 0), (-1, -1), 12),
    ]))
    elements.append(hdr_tbl)
    elements.append(Spacer(1, 0.15 * inch))

    # Two-column: customer info | bill summary
    left_data = [
        ["Customer Name:", "Residential Customer"],
        ["Account #:", data["account_number"]],
        ["Service Location:", data["service_address"]],
        ["Electric Meter #:", data["meter_number"]],
    ]
    lt = Table(left_data, colWidths=[1.4 * inch, 2.2 * inch])
    lt.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
    ]))

    right_data = [
        ["Bill Date:", data["billing_period_end"].strftime("%m/%d/%Y")],
        ["Service From:", data["billing_period_start"].strftime("%m/%d/%Y")],
        ["Service To:", data["billing_period_end"].strftime("%m/%d/%Y")],
        ["Rate Class:", data["rate_schedule"]],
        ["Due Date:", data["due_date"].strftime("%m/%d/%Y")],
    ]
    rt = Table(right_data, colWidths=[1.1 * inch, 1.5 * inch])
    rt.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
    ]))

    two_col = Table([[lt, rt]], colWidths=[3.8 * inch, 3.2 * inch])
    two_col.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP")]))
    elements.append(two_col)
    elements.append(Spacer(1, 0.15 * inch))
    elements.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor(bc)))
    elements.append(Spacer(1, 0.1 * inch))

    # Meter reading / usage
    elements.append(Paragraph("Electric Usage Detail", s_sub))
    elements.append(Spacer(1, 0.05 * inch))
    usage_line = f"Total Consumption: <b>{data['kwh_usage']:,} kWh</b>"
    if data["demand_kw"] > 0:
        usage_line += f"&nbsp;&nbsp;&nbsp;|&nbsp;&nbsp;&nbsp;Demand: <b>{data['demand_kw']:.1f} kW</b>"
    elements.append(Paragraph(usage_line, styles["Normal"]))
    elements.append(Spacer(1, 0.15 * inch))

    # Rate tiers — slightly different column headers
    elements.append(Paragraph("Supply Charge Breakdown", s_sub))
    tier_header = ["Rate Tier", "Usage Range (kWh)", "Price / kWh", "Charge"]
    tier_rows = [tier_header]
    for td in data["tier_details"]:
        tier_rows.append([td["tier"], td["range"], td["rate"], f"${td['amount']:.2f}"])

    tt = Table(tier_rows, colWidths=[1.5 * inch, 1.6 * inch, 1.2 * inch, 1.2 * inch])
    tt.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor(bc)),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("ALIGN", (2, 0), (3, -1), "RIGHT"),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
    ]))
    elements.append(tt)
    elements.append(Spacer(1, 0.2 * inch))

    # Bill summary
    elements.append(Paragraph("Bill Summary", s_sub))
    summary_data = [
        ["Electric Supply:", f"${data['total_energy_charge']:,.2f}"],
        ["Delivery & Infrastructure:", f"${data['delivery_charge']:,.2f}"],
        ["Societal Benefits Charge:", f"${data['system_benefit']:,.2f}"],
        ["State/Local Taxes:", f"${data['taxes_surcharges']:,.2f}"],
        ["Total New Charges:", f"${data['current_charges']:,.2f}"],
    ]
    if data["previous_balance"] > 0:
        summary_data.append(["Previous Amount Due:", f"${data['previous_balance']:,.2f}"])
    summary_data.append(["AMOUNT DUE:", f"${data['total_due']:,.2f}"])

    st = Table(summary_data, colWidths=[2.2 * inch, 1.3 * inch])
    st.setStyle(TableStyle([
        ("ALIGN", (0, 0), (0, -1), "RIGHT"),
        ("ALIGN", (1, 0), (1, -1), "RIGHT"),
        ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
        ("FONTSIZE", (0, -1), (-1, -1), 11),
        ("LINEABOVE", (0, -1), (-1, -1), 1.5, colors.HexColor(bc)),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
    ]))
    sw = Table([[None, st]], colWidths=[3.5 * inch, 3.5 * inch])
    elements.append(sw)
    elements.append(Spacer(1, 0.25 * inch))

    elements.append(Paragraph(
        f"<b>Please pay by {data['due_date'].strftime('%B %d, %Y')} to avoid late charges.</b>",
        ParagraphStyle("Due", parent=styles["Normal"], fontSize=10,
                       textColor=colors.HexColor("#CC0000")),
    ))

    doc.build(elements)


def _ground_truth(data: dict) -> dict:
    """Extract the ground truth values matching UTILITY_BILL config fields."""
    return {
        "file_name": f"utility_bill_{data['bill_num']:02d}.pdf",
        "utility_company": data["provider"]["name"],
        "account_number": data["account_number"],
        "meter_number": data["meter_number"],
        "service_address": data["service_address"],
        "billing_period_start": data["billing_period_start"].strftime("%Y-%m-%d"),
        "billing_period_end": data["billing_period_end"].strftime("%Y-%m-%d"),
        "rate_schedule": data["rate_schedule"],
        "kwh_usage": data["kwh_usage"],
        "demand_kw": data["demand_kw"],
        "previous_balance": data["previous_balance"],
        "current_charges": data["current_charges"],
        "total_due": data["total_due"],
        "due_date": data["due_date"].strftime("%Y-%m-%d"),
        "tier_count": len(data["tier_details"]),
    }


def main():
    script_dir = Path(__file__).parent
    output_dir = script_dir / "sample_documents"
    output_dir.mkdir(exist_ok=True)

    # Provider assignments:
    # Bills 1-5: ConEdison (varied addresses/usage)
    # Bill 6: PSE&G
    # Bill 7: National Grid
    # Bill 8: Orange & Rockland
    # Bill 9: JCP&L
    # Bill 10: ConEdison (one more for variety)
    provider_assignments = [
        PROVIDERS[0],  # ConEdison
        PROVIDERS[0],  # ConEdison
        PROVIDERS[0],  # ConEdison
        PROVIDERS[0],  # ConEdison
        PROVIDERS[0],  # ConEdison
        PROVIDERS[1],  # PSE&G
        PROVIDERS[2],  # National Grid
        PROVIDERS[3],  # O&R
        PROVIDERS[4],  # JCP&L
        PROVIDERS[0],  # ConEdison
    ]

    ground_truths = []

    print("Generating 10 sample utility bills (NY/NJ providers)...\n")
    for i in range(10):
        bill_num = i + 1
        provider = provider_assignments[i]
        address = SERVICE_ADDRESSES[i]
        data = _generate_bill_data(bill_num, provider, address)

        path = output_dir / f"utility_bill_{bill_num:02d}.pdf"

        # Use ConEdison style for ConEd/NatGrid/O&R, PSE&G style for PSE&G/JCP&L
        if provider["short"] in ("PSE&G", "JCP&L"):
            _build_pseg_style(data, str(path))
        else:
            _build_conedison_style(data, str(path))

        gt = _ground_truth(data)
        ground_truths.append(gt)
        print(f"  {path.name} — {provider['short']:15s} | "
              f"{data['kwh_usage']:>5,} kWh | "
              f"${data['total_due']:>8,.2f} | "
              f"{address[:40]}...")

    # Save ground truth
    gt_path = output_dir / "utility_bill_ground_truth.json"
    with open(gt_path, "w") as f:
        json.dump(ground_truths, f, indent=2)

    print(f"\nDone! {len(ground_truths)} utility bills generated.")
    print(f"Ground truth saved to: {gt_path}")
    print(f"Files in: {output_dir}/")


if __name__ == "__main__":
    main()
