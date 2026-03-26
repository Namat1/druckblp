import json
import html
from datetime import datetime
from typing import Dict, List, Tuple

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components


# ============================================================
# STREAMLIT PAGE CONFIG
# ============================================================
st.set_page_config(
    page_title="Sendeplan-Generator",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ============================================================
# 1) HARDCODED DATENQUELLEN
#    HIER DIE ECHTEN EXCEL-WERTE EINFÜGEN
# ============================================================
# WICHTIG:
# - Bitte die Listen unten direkt mit deinen echten Datensätzen füllen.
# - Die Schlüssel entsprechen bewusst den Excel-Spaltenbuchstaben.
# - Keine automatische Spaltenerkennung: nur diese Felder werden verwendet.

KUNDEN_DATA: List[Dict[str, str]] = [
    # --- Beispielzeilen ---
    {
        "A": "Max Mustermann",      # Fachberater
        "I": "2881",               # CSB-Nr
        "J": "1001",               # SAP-Nr (Key)
        "K": "Musterkunde Nord",   # Name
        "L": "Hafenstraße 1",      # Straße
        "M": "24534",              # PLZ
        "N": "Neumünster",         # Ort
    },
    {
        "A": "Erika Beispiel",
        "I": "1884",
        "J": "1002",
        "K": "Beispielmarkt Süd",
        "L": "Industrieweg 7",
        "M": "19370",
        "N": "Parchim",
    },
    # --------------------------------------------------------
    # HIER 500+ ZEILEN EINFÜGEN
    # {
    #     "A": "...",
    #     "I": "...",
    #     "J": "...",
    #     "K": "...",
    #     "L": "...",
    #     "M": "...",
    #     "N": "...",
    # },
]

SAP_DATA: List[Dict[str, str]] = [
    # A: SAP-Nr | O: Liefertyp_ID | I: Bestellzeitende | H: Bestelltag | Y: Rahmentour_Raw
    {"A": "1001", "O": "LT01", "I": "13:00", "H": "1", "Y": "M12345678-01"},
    {"A": "1001", "O": "LT02", "I": "17:00", "H": "3", "Y": "M12345678-02"},
    {"A": "1002", "O": "LT01", "I": "12:00", "H": "2", "Y": "N87654321-01"},
    # HIER 500+ ZEILEN EINFÜGEN
    # {"A": "...", "O": "...", "I": "...", "H": "...", "Y": "..."},
]

TRANSPORT_DATA: List[Dict[str, str]] = [
    # A: Liefertyp_ID | C: Liefertyp_Name
    {"A": "LT01", "C": "Lagerware TP 1001, 3001"},
    {"A": "LT02", "C": "Frische"},
    {"A": "LT03", "C": "Tiefkühl"},
    # HIER WEITERE ZEILEN EINFÜGEN
]

KISOFT_DATA: List[Dict[str, str]] = [
    # Mapping-Key: "SAP Rahmentour" = "00" + erste 8 Stellen von Rahmentour_Raw
    {
        "SAP Rahmentour": "00M1234567",
        "CSB Tournummer": "2881",
        "Verladetor": "Tor 2",
    },
    {
        "SAP Rahmentour": "00N8765432",
        "CSB Tournummer": "1884",
        "Verladetor": "Tor 5",
    },
    # HIER WEITERE ZEILEN EINFÜGEN
]

KOSTENSTELLEN_DATA: List[Dict[str, str]] = [
    # Range-Lookup über SAP-Nr
    # sap_von | sap_bis | tourengruppe | leiter
    {"sap_von": "1001", "sap_bis": "1046", "tourengruppe": "1001-1046", "leiter": "Leitung Nord"},
    {"sap_von": "2001", "sap_bis": "2046", "tourengruppe": "2001-2046", "leiter": "Leitung Süd"},
    # HIER WEITERE BEREICHE EINFÜGEN
]


# ============================================================
# 2) KONSTANTEN UND HILFSWERTE
# ============================================================
WOCHENTAGE = {
    1: "Montag",
    2: "Dienstag",
    3: "Mittwoch",
    4: "Donnerstag",
    5: "Freitag",
    6: "Samstag",
}

KATEGORIEN = ["Alle", "Malchow", "NMS", "MK", "Direkt"]

REQUIRED_KUNDEN_COLUMNS = ["A", "I", "J", "K", "L", "M", "N"]
REQUIRED_SAP_COLUMNS = ["A", "O", "I", "H", "Y"]
REQUIRED_TRANSPORT_COLUMNS = ["A", "C"]
REQUIRED_KISOFT_COLUMNS = ["SAP Rahmentour", "CSB Tournummer", "Verladetor"]
REQUIRED_KOSTENSTELLEN_COLUMNS = ["sap_von", "sap_bis", "tourengruppe", "leiter"]


# ============================================================
# 3) DATENLADUNG UND VALIDIERUNG
# ============================================================
@st.cache_data(show_spinner=False)
def load_raw_data() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df_kunden = pd.DataFrame(KUNDEN_DATA)
    df_sap = pd.DataFrame(SAP_DATA)
    df_transport = pd.DataFrame(TRANSPORT_DATA)
    df_kisoft = pd.DataFrame(KISOFT_DATA)
    df_kostenstellen = pd.DataFrame(KOSTENSTELLEN_DATA)

    validate_required_columns(df_kunden, REQUIRED_KUNDEN_COLUMNS, "df_kunden")
    validate_required_columns(df_sap, REQUIRED_SAP_COLUMNS, "df_sap")
    validate_required_columns(df_transport, REQUIRED_TRANSPORT_COLUMNS, "df_transport")
    validate_required_columns(df_kisoft, REQUIRED_KISOFT_COLUMNS, "df_kisoft")
    validate_required_columns(df_kostenstellen, REQUIRED_KOSTENSTELLEN_COLUMNS, "df_kostenstellen")

    return df_kunden, df_sap, df_transport, df_kisoft, df_kostenstellen


def validate_required_columns(df: pd.DataFrame, required_columns: List[str], name: str) -> None:
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"{name} fehlt Pflichtspalten: {', '.join(missing)}")


def normalize_text(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def normalize_digits(value) -> str:
    text = normalize_text(value)
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits if digits else text


def day_name_from_number(value) -> str:
    try:
        return WOCHENTAGE.get(int(value), "Unbekannt")
    except (TypeError, ValueError):
        return "Unbekannt"


def build_kisoft_key(rahmentour_raw: str) -> str:
    raw = normalize_text(rahmentour_raw)
    return f"00{raw[:8]}" if raw else ""


def is_mk_pattern(csb_nr: str) -> bool:
    csb = normalize_digits(csb_nr)
    return len(csb) == 4 and (csb.endswith("881") or csb.endswith("884"))


def classify_customer(rahmentour_raw: str, csb_nr: str) -> str:
    route = normalize_text(rahmentour_raw).upper()
    if "M" in route:
        return "Malchow"
    if "N" in route:
        return "NMS"
    if is_mk_pattern(csb_nr):
        return "MK"
    return "Direkt"


# ============================================================
# 4) RANGE LOOKUP KOSTENSTELLEN
# ============================================================
def apply_kostenstellen_lookup(df_base: pd.DataFrame, df_kostenstellen: pd.DataFrame) -> pd.DataFrame:
    table = df_kostenstellen.copy()
    table["sap_von_num"] = pd.to_numeric(table["sap_von"], errors="coerce")
    table["sap_bis_num"] = pd.to_numeric(table["sap_bis"], errors="coerce")

    def lookup_row(sap_nr: str) -> pd.Series:
        sap_num = pd.to_numeric(normalize_digits(sap_nr), errors="coerce")
        if pd.isna(sap_num):
            return pd.Series({"Tourengruppe": "", "Leiter": ""})

        match = table[(table["sap_von_num"] <= sap_num) & (table["sap_bis_num"] >= sap_num)]
        if match.empty:
            return pd.Series({"Tourengruppe": "", "Leiter": ""})

        row = match.iloc[0]
        return pd.Series({
            "Tourengruppe": normalize_text(row["tourengruppe"]),
            "Leiter": normalize_text(row["leiter"]),
        })

    result = df_base.copy()
    result[["Tourengruppe", "Leiter"]] = result["SAP_Nr"].apply(lookup_row)
    return result


# ============================================================
# 5) DATENAUFBEREITUNG
# ============================================================
@st.cache_data(show_spinner=False)
def prepare_data() -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, int]]:
    df_kunden_raw, df_sap_raw, df_transport_raw, df_kisoft_raw, df_kostenstellen_raw = load_raw_data()

    df_kunden = df_kunden_raw.rename(columns={
        "A": "Fachberater",
        "I": "CSB_Nr",
        "J": "SAP_Nr",
        "K": "Name",
        "L": "Strasse",
        "M": "PLZ",
        "N": "Ort",
    }).copy()

    df_sap = df_sap_raw.rename(columns={
        "A": "SAP_Nr",
        "O": "Liefertyp_ID",
        "I": "Bestellzeitende",
        "H": "Bestelltag",
        "Y": "Rahmentour_Raw",
    }).copy()

    df_transport = df_transport_raw.rename(columns={
        "A": "Liefertyp_ID",
        "C": "Liefertyp_Name",
    }).copy()

    df_kisoft = df_kisoft_raw.copy()
    df_kostenstellen = df_kostenstellen_raw.copy()

    for col in ["SAP_Nr", "CSB_Nr", "Name", "Strasse", "PLZ", "Ort", "Fachberater"]:
        df_kunden[col] = df_kunden[col].map(normalize_text)

    for col in ["SAP_Nr", "Liefertyp_ID", "Bestellzeitende", "Bestelltag", "Rahmentour_Raw"]:
        df_sap[col] = df_sap[col].map(normalize_text)

    df_transport["Liefertyp_ID"] = df_transport["Liefertyp_ID"].map(normalize_text)
    df_transport["Liefertyp_Name"] = df_transport["Liefertyp_Name"].map(normalize_text)

    df_kisoft["SAP Rahmentour"] = df_kisoft["SAP Rahmentour"].map(normalize_text)
    df_kisoft["CSB Tournummer"] = df_kisoft["CSB Tournummer"].map(normalize_text)
    df_kisoft["Verladetor"] = df_kisoft["Verladetor"].map(normalize_text)

    # SAP-Zeilen anreichern
    df_sap["Kisoft_Key"] = df_sap["Rahmentour_Raw"].map(build_kisoft_key)
    df_sap["Bestelltag_Name"] = df_sap["Bestelltag"].map(day_name_from_number)

    df_sap = df_sap.merge(df_transport, on="Liefertyp_ID", how="left")
    df_sap = df_sap.merge(
        df_kisoft[["SAP Rahmentour", "CSB Tournummer", "Verladetor"]],
        left_on="Kisoft_Key",
        right_on="SAP Rahmentour",
        how="left",
    )

    # Liefertag:
    # 1. bevorzugt aus erster Ziffer der Kisoft-CSB-Tournummer
    # 2. ansonsten aus erster Ziffer der Tourengruppe
    # 3. ansonsten Fallback auf Bestelltag
    def infer_liefertag(row: pd.Series) -> str:
        csb_tour = normalize_digits(row.get("CSB Tournummer", ""))
        if csb_tour and csb_tour[0].isdigit():
            day = int(csb_tour[0])
            if day in WOCHENTAGE:
                return WOCHENTAGE[day]
        return row.get("Bestelltag_Name", "Unbekannt")

    kunden_basis = df_kunden.merge(df_sap[["SAP_Nr", "Rahmentour_Raw"]].drop_duplicates(subset=["SAP_Nr"]), on="SAP_Nr", how="left")
    kunden_basis["Kategorie"] = kunden_basis.apply(
        lambda row: classify_customer(row.get("Rahmentour_Raw", ""), row.get("CSB_Nr", "")),
        axis=1,
    )

    kunden_basis = apply_kostenstellen_lookup(kunden_basis, df_kostenstellen)

    plan_rows = df_sap.merge(
        kunden_basis[[
            "SAP_Nr",
            "CSB_Nr",
            "Name",
            "Strasse",
            "PLZ",
            "Ort",
            "Fachberater",
            "Kategorie",
            "Tourengruppe",
            "Leiter",
        ]],
        on="SAP_Nr",
        how="left",
    )

    plan_rows["Liefertag"] = plan_rows.apply(infer_liefertag, axis=1)
    plan_rows["Sortiment"] = plan_rows["Liefertyp_Name"].fillna("")
    plan_rows["Bestellzeitende"] = plan_rows["Bestellzeitende"].fillna("")

    plan_rows["SortKey_Bestelltag"] = pd.to_numeric(plan_rows["Bestelltag"], errors="coerce").fillna(99)
    plan_rows["SortKey_Sortiment"] = plan_rows["Sortiment"].fillna("")

    counts = {cat: int((kunden_basis["Kategorie"] == cat).sum()) for cat in KATEGORIEN if cat != "Alle"}
    counts["Alle"] = int(len(kunden_basis))

    return kunden_basis, plan_rows, counts


# ============================================================
# 6) FILTER UND SUCHLOGIK
# ============================================================
def filter_customers(df_customers: pd.DataFrame, category: str, search_text: str) -> pd.DataFrame:
    result = df_customers.copy()

    if category != "Alle":
        result = result[result["Kategorie"] == category]

    search = normalize_text(search_text).lower()
    if search:
        result = result[
            result["SAP_Nr"].str.lower().str.contains(search, na=False)
            | result["Name"].str.lower().str.contains(search, na=False)
        ]

    result = result.sort_values(["Name", "SAP_Nr"], na_position="last").reset_index(drop=True)
    return result


# ============================================================
# 7) HTML / CSS / DRUCKLAYOUT
# ============================================================
def app_css() -> str:
    return """
    <style>
        :root {
            --paper-width: 210mm;
            --paper-min-height: 297mm;
            --border-color: #aaaaaa;
            --soft-bg: #f3f5f7;
            --muted: #5f6b76;
            --accent: #1f4e79;
        }

        .stApp {
            background: linear-gradient(180deg, #eef2f7 0%, #f7f9fb 100%);
        }

        section[data-testid="stSidebar"] {
            background: #f7f9fc;
            border-right: 1px solid #d9e0e7;
        }

        .paper {
            width: 100%;
            max-width: var(--paper-width);
            min-height: var(--paper-min-height);
            margin: 0 auto 1.5rem auto;
            background: white;
            box-shadow: 0 10px 30px rgba(0, 0, 0, 0.12);
            border-radius: 8px;
            padding: 12mm;
            color: #222;
        }

        .paper-header {
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            gap: 1rem;
            margin-bottom: 8mm;
            border-bottom: 1px solid #d8dde3;
            padding-bottom: 5mm;
        }

        .paper-title {
            font-size: 20pt;
            font-weight: 700;
            color: var(--accent);
            margin: 0;
        }

        .paper-subtitle {
            font-size: 10pt;
            color: var(--muted);
            margin-top: 2mm;
        }

        .meta-grid {
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 4mm 8mm;
            margin-bottom: 8mm;
        }

        .meta-card {
            border: 1px solid #e3e7ec;
            border-radius: 6px;
            padding: 3.5mm 4mm;
            background: #fbfcfd;
        }

        .meta-label {
            display: block;
            font-size: 8.5pt;
            color: var(--muted);
            text-transform: uppercase;
            letter-spacing: 0.04em;
            margin-bottom: 1mm;
        }

        .meta-value {
            display: block;
            font-size: 11pt;
            font-weight: 600;
        }

        .plan-table {
            width: 100%;
            border-collapse: collapse;
            border: 1.5px solid #aaa;
            font-size: 9pt;
            margin-top: 4mm;
        }

        .plan-table th,
        .plan-table td {
            border: 1px solid #aaa;
            padding: 2.5mm 2mm;
            text-align: left;
            vertical-align: top;
        }

        .plan-table th {
            background: #eef3f9;
            font-weight: 700;
        }

        .cover-page,
        .separator-page {
            width: 100%;
            max-width: var(--paper-width);
            min-height: var(--paper-min-height);
            margin: 0 auto 1.5rem auto;
            background: white;
            box-shadow: 0 10px 30px rgba(0, 0, 0, 0.12);
            border-radius: 8px;
            padding: 18mm 14mm;
            display: flex;
            flex-direction: column;
            justify-content: center;
            align-items: center;
            text-align: center;
            page-break-after: always;
        }

        .cover-page h1,
        .separator-page h1 {
            margin: 0 0 8mm 0;
            font-size: 26pt;
            color: var(--accent);
        }

        .cover-page h2,
        .separator-page h2 {
            margin: 0 0 4mm 0;
            font-size: 16pt;
            color: #2d3741;
        }

        .cover-page p,
        .separator-page p {
            font-size: 11pt;
            color: var(--muted);
            margin: 1.5mm 0;
        }

        .print-toolbar {
            display: flex;
            gap: 0.75rem;
            flex-wrap: wrap;
            margin: 0 0 1rem 0;
            justify-content: center;
        }

        .print-note {
            color: #51606f;
            font-size: 0.92rem;
            text-align: center;
            margin-bottom: 1rem;
        }

        .empty-state {
            width: 100%;
            max-width: var(--paper-width);
            margin: 0 auto;
            background: white;
            box-shadow: 0 10px 30px rgba(0, 0, 0, 0.12);
            border-radius: 8px;
            padding: 30px;
            text-align: center;
            color: #5f6b76;
        }

        @page {
            size: A4 portrait;
            margin: 10mm;
        }

        @media print {
            header,
            [data-testid="stToolbar"],
            [data-testid="stSidebar"],
            [data-testid="stHeader"],
            [data-testid="stDecoration"],
            [data-testid="stStatusWidget"],
            .stDeployButton,
            .print-toolbar,
            .print-note {
                display: none !important;
                visibility: hidden !important;
            }

            .main .block-container {
                padding: 0 !important;
                margin: 0 !important;
                max-width: none !important;
            }

            .paper,
            .cover-page,
            .separator-page {
                width: auto !important;
                min-height: auto !important;
                box-shadow: none !important;
                border-radius: 0 !important;
                margin: 0 !important;
                padding: 0 !important;
                page-break-after: always;
                break-after: page;
            }

            .paper:last-child,
            .separator-page:last-child,
            .cover-page:last-child {
                page-break-after: auto;
                break-after: auto;
            }
        }
    </style>
    """


def render_plan_table(rows: pd.DataFrame) -> str:
    if rows.empty:
        return "<p>Keine Planzeilen vorhanden.</p>"

    ordered = rows.sort_values(["SortKey_Bestelltag", "SortKey_Sortiment", "Bestellzeitende"])

    trs = []
    for _, row in ordered.iterrows():
        trs.append(
            f"""
            <tr>
                <td>{html.escape(normalize_text(row.get('Liefertag', '')))}</td>
                <td>{html.escape(normalize_text(row.get('Sortiment', '')))}</td>
                <td>{html.escape(normalize_text(row.get('Bestelltag_Name', '')))}</td>
                <td>{html.escape(normalize_text(row.get('Bestellzeitende', '')))}</td>
            </tr>
            """
        )

    return f"""
        <table class="plan-table">
            <thead>
                <tr>
                    <th>Liefertag</th>
                    <th>Sortiment</th>
                    <th>Bestelltag</th>
                    <th>Bestellzeitende</th>
                </tr>
            </thead>
            <tbody>
                {''.join(trs)}
            </tbody>
        </table>
    """


def render_customer_plan(customer: pd.Series, customer_rows: pd.DataFrame) -> str:
    sap_nr = normalize_text(customer.get("SAP_Nr", ""))
    name = normalize_text(customer.get("Name", ""))
    address = " ".join(filter(None, [normalize_text(customer.get("Strasse", ""))]))
    plz_ort = " ".join(filter(None, [normalize_text(customer.get("PLZ", "")), normalize_text(customer.get("Ort", ""))]))
    category = normalize_text(customer.get("Kategorie", ""))
    csb_nr = normalize_text(customer.get("CSB_Nr", ""))
    fachberater = normalize_text(customer.get("Fachberater", ""))
    tourengruppe = normalize_text(customer.get("Tourengruppe", ""))
    leiter = normalize_text(customer.get("Leiter", ""))

    verladetor = normalize_text(customer_rows["Verladetor"].dropna().iloc[0]) if not customer_rows.empty and customer_rows["Verladetor"].dropna().any() else ""
    rahmentour = normalize_text(customer_rows["Rahmentour_Raw"].dropna().iloc[0]) if not customer_rows.empty and customer_rows["Rahmentour_Raw"].dropna().any() else normalize_text(customer.get("Rahmentour_Raw", ""))

    return f"""
    <div class="paper">
        <div class="paper-header">
            <div>
                <h1 class="paper-title">Sendeplan</h1>
                <div class="paper-subtitle">Generiert am {datetime.now().strftime('%d.%m.%Y %H:%M')} Uhr</div>
            </div>
            <div style="text-align:right; font-size:10pt; color:#5f6b76;">
                <div><strong>Kategorie:</strong> {html.escape(category)}</div>
                <div><strong>Rahmentour:</strong> {html.escape(rahmentour)}</div>
                <div><strong>Verladetor:</strong> {html.escape(verladetor)}</div>
            </div>
        </div>

        <div class="meta-grid">
            <div class="meta-card">
                <span class="meta-label">SAP-Nummer</span>
                <span class="meta-value">{html.escape(sap_nr)}</span>
            </div>
            <div class="meta-card">
                <span class="meta-label">CSB-Nummer</span>
                <span class="meta-value">{html.escape(csb_nr)}</span>
            </div>
            <div class="meta-card">
                <span class="meta-label">Kunde</span>
                <span class="meta-value">{html.escape(name)}</span>
            </div>
            <div class="meta-card">
                <span class="meta-label">Fachberater</span>
                <span class="meta-value">{html.escape(fachberater)}</span>
            </div>
            <div class="meta-card">
                <span class="meta-label">Adresse</span>
                <span class="meta-value">{html.escape(address)}</span>
            </div>
            <div class="meta-card">
                <span class="meta-label">PLZ / Ort</span>
                <span class="meta-value">{html.escape(plz_ort)}</span>
            </div>
            <div class="meta-card">
                <span class="meta-label">Tourengruppe</span>
                <span class="meta-value">{html.escape(tourengruppe)}</span>
            </div>
            <div class="meta-card">
                <span class="meta-label">Leiter</span>
                <span class="meta-value">{html.escape(leiter)}</span>
            </div>
        </div>

        {render_plan_table(customer_rows)}
    </div>
    """


def render_cover_page(title: str, subtitle: str, lines: List[str]) -> str:
    content = "".join(f"<p>{html.escape(normalize_text(line))}</p>" for line in lines)
    return f"""
    <div class="cover-page">
        <h1>{html.escape(title)}</h1>
        <h2>{html.escape(subtitle)}</h2>
        {content}
    </div>
    """


def render_separator_page(customer: pd.Series) -> str:
    return f"""
    <div class="separator-page">
        <h1>{html.escape(normalize_text(customer.get('Name', '')))}</h1>
        <h2>SAP {html.escape(normalize_text(customer.get('SAP_Nr', '')))}</h2>
        <p>CSB {html.escape(normalize_text(customer.get('CSB_Nr', '')))}</p>
        <p>{html.escape(normalize_text(customer.get('PLZ', '')))} {html.escape(normalize_text(customer.get('Ort', '')))}</p>
        <p>Kategorie: {html.escape(normalize_text(customer.get('Kategorie', '')))}</p>
    </div>
    """


def build_full_document_html(customers: pd.DataFrame, plan_rows: pd.DataFrame, include_separators: bool = True) -> str:
    docs: List[str] = []

    docs.append(render_cover_page(
        title="Sendeplan-Generator",
        subtitle="Gesamtplan",
        lines=[
            f"Erstellt am {datetime.now().strftime('%d.%m.%Y %H:%M')} Uhr",
            f"Kundenanzahl: {len(customers)}",
            "Enthält Deckblatt, Zwischenseiten und Kundenseiten.",
        ],
    ))

    for idx, (_, customer) in enumerate(customers.iterrows(), start=1):
        rows = plan_rows[plan_rows["SAP_Nr"] == customer["SAP_Nr"]].copy()
        if include_separators:
            docs.append(render_separator_page(customer))
        docs.append(render_customer_plan(customer, rows))

    return f"""
    <!DOCTYPE html>
    <html lang="de">
    <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1.0" />
        <title>Sendeplan-Export</title>
        {app_css()}
    </head>
    <body>
        {''.join(docs)}
    </body>
    </html>
    """


def build_single_document_html(customer: pd.Series, customer_rows: pd.DataFrame) -> str:
    return f"""
    <!DOCTYPE html>
    <html lang="de">
    <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1.0" />
        <title>Sendeplan {html.escape(normalize_text(customer.get('SAP_Nr', '')))}</title>
        {app_css()}
    </head>
    <body>
        {render_customer_plan(customer, customer_rows)}
    </body>
    </html>
    """


def render_print_buttons(single_html: str, bulk_html: str) -> None:
    single_html_json = json.dumps(single_html)
    bulk_html_json = json.dumps(bulk_html)

    components.html(
        f"""
        <div class="print-toolbar">
            <button onclick='printDocument({single_html_json})'
                    style="padding:10px 16px; border:none; border-radius:8px; background:#1f4e79; color:white; font-weight:600; cursor:pointer;">
                Einzeldruck
            </button>
            <button onclick='printDocument({bulk_html_json})'
                    style="padding:10px 16px; border:none; border-radius:8px; background:#2a7f62; color:white; font-weight:600; cursor:pointer;">
                Alle drucken
            </button>
        </div>
        <script>
            function printDocument(fullHtml) {{
                const w = window.open('', '_blank');
                w.document.open();
                w.document.write(fullHtml);
                w.document.close();
                w.focus();
                setTimeout(() => w.print(), 250);
            }}
        </script>
        """,
        height=70,
    )


# ============================================================
# 8) STREAMLIT UI
# ============================================================
def init_session_state() -> None:
    if "category_filter" not in st.session_state:
        st.session_state.category_filter = "Alle"

    if "selected_sap" not in st.session_state:
        st.session_state.selected_sap = ""


def set_category(category: str) -> None:
    st.session_state.category_filter = category


@st.cache_data(show_spinner=False)
def build_option_labels(df_customers: pd.DataFrame) -> Dict[str, str]:
    return {
        row["SAP_Nr"]: f"{row['SAP_Nr']} | {row['Name']} | {row['Ort']}"
        for _, row in df_customers.iterrows()
    }


# ============================================================
# 9) APP START
# ============================================================
def main() -> None:
    init_session_state()
    st.markdown(app_css(), unsafe_allow_html=True)

    customers_df, plan_rows_df, counts = prepare_data()

    st.title("📦 Sendeplan-Generator")
    st.caption("Hardgecodete Streamlit-App für Kunden-, SAP-, Transport-, Kisoft- und Kostenstellenlogik.")

    with st.sidebar:
        st.header("Filter")
        st.text_input(
            "Suche nach SAP-Nummer oder Name",
            key="search_text",
            placeholder="z. B. 1001 oder Musterkunde",
        )

        st.markdown("**Bereiche**")
        col1, col2 = st.columns(2)
        with col1:
            st.button(f"Alle ({counts['Alle']})", use_container_width=True, on_click=set_category, args=("Alle",))
            st.button(f"Malchow ({counts['Malchow']})", use_container_width=True, on_click=set_category, args=("Malchow",))
            st.button(f"MK ({counts['MK']})", use_container_width=True, on_click=set_category, args=("MK",))
        with col2:
            st.button(f"NMS ({counts['NMS']})", use_container_width=True, on_click=set_category, args=("NMS",))
            st.button(f"Direkt ({counts['Direkt']})", use_container_width=True, on_click=set_category, args=("Direkt",))

        st.info(f"Aktiver Filter: **{st.session_state.category_filter}**")

        filtered_customers = filter_customers(
            customers_df,
            st.session_state.category_filter,
            st.session_state.get("search_text", ""),
        )

        st.markdown(f"**Treffer:** {len(filtered_customers)}")

        option_labels = build_option_labels(filtered_customers)
        options = filtered_customers["SAP_Nr"].tolist()

        if options:
            if st.session_state.selected_sap not in options:
                st.session_state.selected_sap = options[0]

            selected_sap = st.selectbox(
                "Kunde auswählen",
                options=options,
                format_func=lambda sap: option_labels.get(sap, sap),
                index=options.index(st.session_state.selected_sap) if st.session_state.selected_sap in options else 0,
            )
            st.session_state.selected_sap = selected_sap
        else:
            st.session_state.selected_sap = ""
            selected_sap = ""

        st.divider()
        st.subheader("Download")

        export_df = filtered_customers.copy()
        export_html = build_full_document_html(export_df, plan_rows_df, include_separators=True)
        filename_suffix = normalize_text(st.session_state.category_filter).lower() or "alle"

        st.download_button(
            label="HTML-Export herunterladen",
            data=export_html,
            file_name=f"sendeplan_{filename_suffix}.html",
            mime="text/html",
            use_container_width=True,
        )

    main_col, info_col = st.columns([5, 2], gap="large")

    with info_col:
        st.subheader("Datenstatus")
        st.metric("Kunden", len(customers_df))
        st.metric("Planzeilen", len(plan_rows_df))
        st.metric("Treffer im Filter", len(filtered_customers))

        st.markdown("### Mapping-Regeln")
        st.markdown(
            """
            - **Kunden**: J = SAP, I = CSB, K = Name, L = Straße, M = PLZ, N = Ort, A = Fachberater
            - **SAP**: A = SAP, O = Liefertyp, I = Bestellzeitende, H = Bestelltag, Y = Rahmentour
            - **Transport**: A = Liefertyp-ID, C = Klartext für Sortiment
            - **Kisoft**: `00 + erste 8 Stellen von Rahmentour_Raw` → `SAP Rahmentour`
            - **Kostenstellen**: Range-Lookup über SAP-Bereiche
            """
        )

    with main_col:
        if not filtered_customers.empty and st.session_state.selected_sap:
            selected_customer = filtered_customers[filtered_customers["SAP_Nr"] == st.session_state.selected_sap].iloc[0]
            customer_rows = plan_rows_df[plan_rows_df["SAP_Nr"] == st.session_state.selected_sap].copy()

            single_html = build_single_document_html(selected_customer, customer_rows)
            bulk_html = build_full_document_html(filtered_customers, plan_rows_df, include_separators=True)

            st.markdown("<div class='print-note'>Druckfunktionen öffnen ein separates HTML-Dokument mit dem A4-Layout und starten dort den Browser-Druck.</div>", unsafe_allow_html=True)
            render_print_buttons(single_html, bulk_html)
            st.markdown(render_customer_plan(selected_customer, customer_rows), unsafe_allow_html=True)
        else:
            st.markdown(
                """
                <div class="empty-state">
                    <h3>Keine Kunden im aktuellen Filter</h3>
                    <p>Bitte Suchbegriff oder Kategorie anpassen.</p>
                </div>
                """,
                unsafe_allow_html=True,
            )

        with st.expander("Code-Hinweise für die Datenpflege", expanded=False):
            st.code(
                """
# 1) KUNDEN_DATA mit echten Excel-Zeilen füllen
KUNDEN_DATA = [
    {"A": "Fachberater", "I": "CSB", "J": "SAP", "K": "Name", "L": "Straße", "M": "PLZ", "N": "Ort"},
]

# 2) SAP_DATA mit echten SAP-Zeilen füllen
SAP_DATA = [
    {"A": "SAP", "O": "Liefertyp_ID", "I": "Bestellzeitende", "H": "Bestelltag", "Y": "Rahmentour_Raw"},
]

# 3) TRANSPORT_DATA füllen
TRANSPORT_DATA = [
    {"A": "Liefertyp_ID", "C": "Liefertyp_Name"},
]

# 4) KISOFT_DATA füllen
KISOFT_DATA = [
    {"SAP Rahmentour": "00XXXXXXXX", "CSB Tournummer": "2881", "Verladetor": "Tor 1"},
]

# 5) KOSTENSTELLEN_DATA füllen
KOSTENSTELLEN_DATA = [
    {"sap_von": "1001", "sap_bis": "1046", "tourengruppe": "1001-1046", "leiter": "Leitung Nord"},
]
                """,
                language="python",
            )


if __name__ == "__main__":
    main()
