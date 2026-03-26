import html
import io
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components


st.set_page_config(
    page_title="Sendeplan-Generator",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded",
)


WOCHENTAGE = {
    1: "Montag",
    2: "Dienstag",
    3: "Mittwoch",
    4: "Donnerstag",
    5: "Freitag",
    6: "Samstag",
}

KATEGORIEN = ["Alle", "Malchow", "NMS", "MK", "Direkt"]

UPLOAD_CONFIG = {
    "kunden": {
        "label": "Kundenliste hochladen",
        "help": "Verwendet feste Excel-Spalten: A, I, J, K, L, M, N",
        "mapping": {
            "Fachberater": "A",
            "CSB_Nr": "I",
            "SAP_Nr": "J",
            "Name": "K",
            "Strasse": "L",
            "PLZ": "M",
            "Ort": "N",
        },
        "required": ["Fachberater", "CSB_Nr", "SAP_Nr", "Name", "Strasse", "PLZ", "Ort"],
        "key": "SAP_Nr",
    },
    "sap": {
        "label": "SAP-Datei hochladen",
        "help": "Verwendet feste Excel-Spalten: A, H, I, O, Y",
        "mapping": {
            "SAP_Nr": "A",
            "Bestelltag": "H",
            "Bestellzeitende": "I",
            "Liefertyp_ID": "O",
            "Rahmentour_Raw": "Y",
        },
        "required": ["SAP_Nr", "Bestelltag", "Bestellzeitende", "Liefertyp_ID", "Rahmentour_Raw"],
        "key": "SAP_Nr",
    },
    "transport": {
        "label": "Transportgruppen hochladen",
        "help": "Verwendet feste Excel-Spalten: A, C",
        "mapping": {
            "Liefertyp_ID": "A",
            "Liefertyp_Name": "C",
        },
        "required": ["Liefertyp_ID", "Liefertyp_Name"],
        "key": "Liefertyp_ID",
    },
}

KISOFT_REQUIRED_COLUMNS = ["SAP Rahmentour", "CSB Tournummer", "Verladetor"]
KOSTENSTELLEN_REQUIRED_COLUMNS = ["sap_von", "sap_bis", "tourengruppe", "leiter"]


# ============================================================
# HILFSFUNKTIONEN
# ============================================================
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
        return WOCHENTAGE.get(int(str(value).strip()), "Unbekannt")
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


def validate_required_columns(df: pd.DataFrame, required_columns: List[str], name: str) -> None:
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"{name} fehlt Pflichtspalten: {', '.join(missing)}")


def excel_column_to_index(column_letter: str) -> int:
    result = 0
    for char in column_letter.upper():
        if not char.isalpha():
            raise ValueError(f"Ungültiger Spaltenbuchstabe: {column_letter}")
        result = result * 26 + (ord(char) - ord("A") + 1)
    return result - 1


def read_upload_to_raw_dataframe(file_bytes: bytes, filename: str, csv_separator: str) -> pd.DataFrame:
    suffix = Path(filename).suffix.lower()

    if suffix == ".csv":
        errors = []
        for encoding in ["utf-8-sig", "utf-8", "latin1", "cp1252"]:
            try:
                return pd.read_csv(
                    io.BytesIO(file_bytes),
                    sep=csv_separator,
                    header=None,
                    dtype=str,
                    encoding=encoding,
                    keep_default_na=False,
                )
            except Exception as exc:  # pragma: no cover
                errors.append(f"{encoding}: {exc}")
        raise ValueError(f"CSV konnte nicht gelesen werden. Versuchte Encodings: {' | '.join(errors)}")

    if suffix in {".xlsx", ".xls", ".xlsm"}:
        return pd.read_excel(
            io.BytesIO(file_bytes),
            header=None,
            dtype=str,
        )

    raise ValueError(f"Nicht unterstütztes Dateiformat: {suffix}")


def extract_columns_by_letter(raw_df: pd.DataFrame, mapping: Dict[str, str], dataset_name: str) -> pd.DataFrame:
    extracted: Dict[str, pd.Series] = {}
    for target_name, column_letter in mapping.items():
        column_index = excel_column_to_index(column_letter)
        if column_index >= raw_df.shape[1]:
            raise ValueError(
                f"{dataset_name}: benötigte Spalte {column_letter} ist nicht vorhanden. "
                f"Gefundene Spaltenanzahl: {raw_df.shape[1]}"
            )
        extracted[target_name] = raw_df.iloc[:, column_index]
    return pd.DataFrame(extracted)


def cleanup_dataframe(df: pd.DataFrame, key_column: str) -> pd.DataFrame:
    result = df.copy()
    for column in result.columns:
        result[column] = result[column].map(normalize_text)

    result = result.replace("", pd.NA).dropna(how="all").fillna("")
    result = result[result[key_column].map(normalize_text) != ""].copy()

    if not result.empty:
        first_key = normalize_text(result.iloc[0][key_column]).lower()
        header_like_tokens = {
            key_column.lower(),
            key_column.lower().replace("_", " "),
            "sap",
            "sap-nr",
            "sap nr",
            "liefertyp_id",
            "liefertyp id",
            "sap rahmentour",
            "sap_von",
            "sap von",
            "csb tournummer",
        }
        if first_key in header_like_tokens:
            result = result.iloc[1:].copy()

    return result.reset_index(drop=True)


@st.cache_data(show_spinner=False)
def load_structured_upload(file_bytes: bytes, filename: str, csv_separator: str, dataset_key: str) -> pd.DataFrame:
    config = UPLOAD_CONFIG[dataset_key]
    raw_df = read_upload_to_raw_dataframe(file_bytes, filename, csv_separator)
    structured_df = extract_columns_by_letter(raw_df, config["mapping"], config["label"])
    structured_df = cleanup_dataframe(structured_df, config["key"])
    validate_required_columns(structured_df, config["required"], config["label"])
    return structured_df


@st.cache_data(show_spinner=False)
def load_kisoft_upload(file_bytes: bytes, filename: str, csv_separator: str) -> pd.DataFrame:
    raw_df = read_upload_to_raw_dataframe(file_bytes, filename, csv_separator)

    if raw_df.shape[1] >= 3:
        df = raw_df.iloc[:, :3].copy()
        df.columns = KISOFT_REQUIRED_COLUMNS
        df = cleanup_dataframe(df, "SAP Rahmentour")
        first_row_values = {normalize_text(value).lower() for value in df.head(1).iloc[0].tolist()} if not df.empty else set()
        if first_row_values & {"sap rahmentour", "csb tournummer", "verladetor"}:
            df = df.iloc[1:].reset_index(drop=True)
    else:
        raise ValueError("Kisoft-Datei muss mindestens 3 Spalten enthalten.")

    for column in KISOFT_REQUIRED_COLUMNS:
        df[column] = df[column].map(normalize_text)

    validate_required_columns(df, KISOFT_REQUIRED_COLUMNS, "Kisoft-Datei")
    return df


@st.cache_data(show_spinner=False)
def load_kostenstellen_upload(file_bytes: bytes, filename: str, csv_separator: str) -> pd.DataFrame:
    suffix = Path(filename).suffix.lower()

    if suffix == ".csv":
        for encoding in ["utf-8-sig", "utf-8", "latin1", "cp1252"]:
            try:
                df = pd.read_csv(
                    io.BytesIO(file_bytes),
                    sep=csv_separator,
                    dtype=str,
                    encoding=encoding,
                    keep_default_na=False,
                )
                break
            except Exception:
                df = None
        if df is None:
            raise ValueError("Kostenstellen-CSV konnte nicht gelesen werden.")
    else:
        df = pd.read_excel(io.BytesIO(file_bytes), dtype=str)

    df.columns = [normalize_text(column) for column in df.columns]
    missing = [col for col in KOSTENSTELLEN_REQUIRED_COLUMNS if col not in df.columns]

    if missing:
        raw_df = read_upload_to_raw_dataframe(file_bytes, filename, csv_separator)
        if raw_df.shape[1] < 4:
            raise ValueError("Kostenstellen-Datei benötigt mindestens 4 Spalten.")
        df = raw_df.iloc[:, :4].copy()
        df.columns = KOSTENSTELLEN_REQUIRED_COLUMNS

    df = cleanup_dataframe(df, "sap_von")
    for column in KOSTENSTELLEN_REQUIRED_COLUMNS:
        df[column] = df[column].map(normalize_text)

    validate_required_columns(df, KOSTENSTELLEN_REQUIRED_COLUMNS, "Kostenstellen-Datei")
    return df


# ============================================================
# LOOKUP UND AUFBEREITUNG
# ============================================================
def apply_kostenstellen_lookup(df_base: pd.DataFrame, df_kostenstellen: pd.DataFrame) -> pd.DataFrame:
    table = df_kostenstellen.copy()
    table["sap_von_num"] = pd.to_numeric(table["sap_von"].map(normalize_digits), errors="coerce")
    table["sap_bis_num"] = pd.to_numeric(table["sap_bis"].map(normalize_digits), errors="coerce")

    def lookup_row(sap_nr: str) -> pd.Series:
        sap_num = pd.to_numeric(normalize_digits(sap_nr), errors="coerce")
        if pd.isna(sap_num):
            return pd.Series({"Tourengruppe": "", "Leiter": ""})

        match = table[(table["sap_von_num"] <= sap_num) & (table["sap_bis_num"] >= sap_num)]
        if match.empty:
            return pd.Series({"Tourengruppe": "", "Leiter": ""})

        row = match.iloc[0]
        return pd.Series(
            {
                "Tourengruppe": normalize_text(row["tourengruppe"]),
                "Leiter": normalize_text(row["leiter"]),
            }
        )

    result = df_base.copy()
    result[["Tourengruppe", "Leiter"]] = result["SAP_Nr"].apply(lookup_row)
    return result


@st.cache_data(show_spinner=False)
def prepare_dataframes(
    kunden_bytes: bytes,
    kunden_name: str,
    sap_bytes: bytes,
    sap_name: str,
    transport_bytes: bytes,
    transport_name: str,
    kisoft_bytes: bytes,
    kisoft_name: str,
    kostenstellen_bytes: bytes,
    kostenstellen_name: str,
    csv_separator: str,
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, int]]:
    df_kunden = load_structured_upload(kunden_bytes, kunden_name, csv_separator, "kunden")
    df_sap = load_structured_upload(sap_bytes, sap_name, csv_separator, "sap")
    df_transport = load_structured_upload(transport_bytes, transport_name, csv_separator, "transport")
    df_kisoft = load_kisoft_upload(kisoft_bytes, kisoft_name, csv_separator)
    df_kostenstellen = load_kostenstellen_upload(kostenstellen_bytes, kostenstellen_name, csv_separator)

    for column in df_kunden.columns:
        df_kunden[column] = df_kunden[column].map(normalize_text)

    for column in df_sap.columns:
        df_sap[column] = df_sap[column].map(normalize_text)

    for column in df_transport.columns:
        df_transport[column] = df_transport[column].map(normalize_text)

    for column in df_kisoft.columns:
        df_kisoft[column] = df_kisoft[column].map(normalize_text)

    df_sap["Kisoft_Key"] = df_sap["Rahmentour_Raw"].map(build_kisoft_key)
    df_sap["Bestelltag_Name"] = df_sap["Bestelltag"].map(day_name_from_number)

    df_sap = df_sap.merge(df_transport, on="Liefertyp_ID", how="left")
    df_sap = df_sap.merge(
        df_kisoft[["SAP Rahmentour", "CSB Tournummer", "Verladetor"]],
        left_on="Kisoft_Key",
        right_on="SAP Rahmentour",
        how="left",
    )

    def infer_liefertag(row: pd.Series) -> str:
        csb_tour = normalize_digits(row.get("CSB Tournummer", ""))
        if csb_tour and csb_tour[0].isdigit():
            day = int(csb_tour[0])
            if day in WOCHENTAGE:
                return WOCHENTAGE[day]
        return row.get("Bestelltag_Name", "Unbekannt")

    kunden_basis = df_kunden.merge(
        df_sap[["SAP_Nr", "Rahmentour_Raw"]].drop_duplicates(subset=["SAP_Nr"]),
        on="SAP_Nr",
        how="left",
    )
    kunden_basis["Kategorie"] = kunden_basis.apply(
        lambda row: classify_customer(row.get("Rahmentour_Raw", ""), row.get("CSB_Nr", "")),
        axis=1,
    )
    kunden_basis = apply_kostenstellen_lookup(kunden_basis, df_kostenstellen)

    plan_rows = df_sap.merge(
        kunden_basis[
            [
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
            ]
        ],
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
# FILTER
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

    return result.sort_values(["Name", "SAP_Nr"], na_position="last").reset_index(drop=True)


# ============================================================
# HTML UND CSS
# ============================================================
def app_css() -> str:
    return """
    <style>
        :root {
            --paper-width: 210mm;
            --paper-min-height: 297mm;
            --border-color: #aaaaaa;
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

        .export-search-toolbar {
            width: 100%;
            max-width: var(--paper-width);
            margin: 0 auto 1rem auto;
            background: white;
            box-shadow: 0 10px 30px rgba(0, 0, 0, 0.12);
            border-radius: 8px;
            padding: 14px 16px;
            color: #1f2933;
        }

        .export-search-title {
            margin: 0 0 10px 0;
            font-size: 16px;
            font-weight: 700;
            color: var(--accent);
        }

        .export-search-grid {
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 12px;
        }

        .export-search-field label {
            display: block;
            font-size: 12px;
            font-weight: 700;
            color: #44515d;
            margin-bottom: 6px;
        }

        .export-search-field input {
            width: 100%;
            box-sizing: border-box;
            border: 1px solid #cbd5e1;
            border-radius: 8px;
            padding: 10px 12px;
            font-size: 14px;
            color: #111827;
            background: #ffffff;
        }

        .export-search-actions {
            display: flex;
            flex-wrap: wrap;
            align-items: center;
            gap: 12px;
            margin-top: 12px;
        }

        .export-search-actions button {
            border: 1px solid #9fb4c8;
            border-radius: 8px;
            padding: 9px 14px;
            background: #f8fbff;
            color: #184b6b;
            font-weight: 700;
            cursor: pointer;
        }

        .export-search-actions button:hover {
            background: #eef5fb;
        }

        .export-search-results {
            font-size: 13px;
            color: #526173;
        }

        .export-empty-results {
            display: none;
            margin-top: 12px;
            border: 1px solid #d7dde5;
            border-radius: 8px;
            padding: 12px;
            background: #fafcfe;
            color: #526173;
        }

        .customer-entry {
            display: block;
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
            .print-note,
            .export-search-toolbar,
            .export-empty-results {
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
    body_rows = []

    for _, row in ordered.iterrows():
        body_rows.append(
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
            {''.join(body_rows)}
        </tbody>
    </table>
    """


def render_customer_plan(customer: pd.Series, customer_rows: pd.DataFrame) -> str:
    sap_nr = normalize_text(customer.get("SAP_Nr", ""))
    name = normalize_text(customer.get("Name", ""))
    address = normalize_text(customer.get("Strasse", ""))
    plz_ort = " ".join(filter(None, [normalize_text(customer.get("PLZ", "")), normalize_text(customer.get("Ort", ""))]))
    category = normalize_text(customer.get("Kategorie", ""))
    csb_nr = normalize_text(customer.get("CSB_Nr", ""))
    fachberater = normalize_text(customer.get("Fachberater", ""))
    tourengruppe = normalize_text(customer.get("Tourengruppe", ""))
    leiter = normalize_text(customer.get("Leiter", ""))

    verladetor = ""
    rahmentour = normalize_text(customer.get("Rahmentour_Raw", ""))

    if not customer_rows.empty:
        verladetor_series = customer_rows["Verladetor"].dropna().astype(str).str.strip()
        rahmentour_series = customer_rows["Rahmentour_Raw"].dropna().astype(str).str.strip()
        if not verladetor_series.empty:
            verladetor = normalize_text(verladetor_series.iloc[0])
        if not rahmentour_series.empty:
            rahmentour = normalize_text(rahmentour_series.iloc[0])

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
            <div class="meta-card"><span class="meta-label">SAP-Nummer</span><span class="meta-value">{html.escape(sap_nr)}</span></div>
            <div class="meta-card"><span class="meta-label">CSB-Nummer</span><span class="meta-value">{html.escape(csb_nr)}</span></div>
            <div class="meta-card"><span class="meta-label">Kunde</span><span class="meta-value">{html.escape(name)}</span></div>
            <div class="meta-card"><span class="meta-label">Fachberater</span><span class="meta-value">{html.escape(fachberater)}</span></div>
            <div class="meta-card"><span class="meta-label">Adresse</span><span class="meta-value">{html.escape(address)}</span></div>
            <div class="meta-card"><span class="meta-label">PLZ / Ort</span><span class="meta-value">{html.escape(plz_ort)}</span></div>
            <div class="meta-card"><span class="meta-label">Tourengruppe</span><span class="meta-value">{html.escape(tourengruppe)}</span></div>
            <div class="meta-card"><span class="meta-label">Leiter</span><span class="meta-value">{html.escape(leiter)}</span></div>
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


def render_export_search_toolbar() -> str:
    return """
    <div class="export-search-toolbar">
        <div class="export-search-title">Suche im exportierten Sendeplan</div>
        <div class="export-search-grid">
            <div class="export-search-field">
                <label for="search-sap">SAP-Nummer</label>
                <input id="search-sap" type="text" placeholder="zum Beispiel 211393" />
            </div>
            <div class="export-search-field">
                <label for="search-csb">CSB-Nummer oder CSB-Tour</label>
                <input id="search-csb" type="text" placeholder="zum Beispiel 2881 oder 22221" />
            </div>
        </div>
        <div class="export-search-actions">
            <button type="button" onclick="resetExportSearch()">Suche zurücksetzen</button>
            <div id="export-search-results" class="export-search-results"></div>
        </div>
        <div id="export-empty-results" class="export-empty-results">
            Keine Treffer für die aktuelle SAP- oder CSB-Suche.
        </div>
    </div>
    """



def build_full_document_html(customers: pd.DataFrame, plan_rows: pd.DataFrame, include_separators: bool = True) -> str:
    docs: List[str] = [
        render_cover_page(
            title="Sendeplan-Generator",
            subtitle="Gesamtplan",
            lines=[
                f"Erstellt am {datetime.now().strftime('%d.%m.%Y %H:%M')} Uhr",
                f"Kundenanzahl: {len(customers)}",
                "Diese HTML-Datei ist vollständig eigenständig und direkt im Browser durchsuchbar.",
            ],
        )
    ]

    entry_count = 0
    for _, customer in customers.iterrows():
        rows = plan_rows[plan_rows["SAP_Nr"] == customer["SAP_Nr"]].copy()
        sap = normalize_text(customer.get("SAP_Nr", ""))
        csb_nr = normalize_text(customer.get("CSB_Nr", ""))
        csb_touren = sorted({
            normalize_text(value)
            for value in rows.get("CSB Tournummer", pd.Series(dtype=str)).tolist()
            if normalize_text(value)
        })
        search_blob = " ".join(
            part for part in [sap, csb_nr, " ".join(csb_touren), normalize_text(customer.get("Name", ""))]
            if part
        ).lower()

        entry_parts: List[str] = []
        if include_separators:
            entry_parts.append(render_separator_page(customer))
        entry_parts.append(render_customer_plan(customer, rows))

        csb_search = " ".join([part for part in [csb_nr, *csb_touren] if part]).lower()
        docs.append(
            (
                f'<section class="customer-entry" '
                f'data-sap="{html.escape(sap.lower())}" '
                f'data-csb="{html.escape(csb_search)}" '
                f'data-search="{html.escape(search_blob)}">'
                f'{"".join(entry_parts)}'
                f'</section>'
            )
        )
        entry_count += 1

    search_script = f"""
    <script>
        function normalizeSearchValue(value) {{
            return (value || '').toLowerCase().trim();
        }}

        function applyExportSearch() {{
            const sapValue = normalizeSearchValue(document.getElementById('search-sap').value);
            const csbValue = normalizeSearchValue(document.getElementById('search-csb').value);
            const entries = Array.from(document.querySelectorAll('.customer-entry'));
            let visibleCount = 0;

            entries.forEach((entry) => {{
                const sap = normalizeSearchValue(entry.getAttribute('data-sap'));
                const csb = normalizeSearchValue(entry.getAttribute('data-csb'));
                const sapOk = !sapValue || sap.includes(sapValue);
                const csbOk = !csbValue || csb.includes(csbValue);
                const show = sapOk && csbOk;
                entry.style.display = show ? '' : 'none';
                if (show) {{
                    visibleCount += 1;
                }}
            }});

            const resultLabel = document.getElementById('export-search-results');
            const emptyState = document.getElementById('export-empty-results');
            resultLabel.textContent = `Treffer: ${{visibleCount}} / {entry_count}`;
            emptyState.style.display = visibleCount === 0 ? 'block' : 'none';
        }}

        function resetExportSearch() {{
            document.getElementById('search-sap').value = '';
            document.getElementById('search-csb').value = '';
            applyExportSearch();
        }}

        document.addEventListener('DOMContentLoaded', function () {{
            document.getElementById('search-sap').addEventListener('input', applyExportSearch);
            document.getElementById('search-csb').addEventListener('input', applyExportSearch);
            applyExportSearch();
        }});
    </script>
    """

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
        {render_export_search_toolbar()}
        {''.join(docs)}
        {search_script}
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


@st.cache_data(show_spinner=False)
def build_option_labels(df_customers: pd.DataFrame) -> Dict[str, str]:
    return {
        row["SAP_Nr"]: f"{row['SAP_Nr']} | {row['Name']} | {row['Ort']}"
        for _, row in df_customers.iterrows()
    }


def init_session_state() -> None:
    if "category_filter" not in st.session_state:
        st.session_state.category_filter = "Alle"
    if "selected_sap" not in st.session_state:
        st.session_state.selected_sap = ""


def set_category(category: str) -> None:
    st.session_state.category_filter = category


def all_required_uploads_present(upload_map: Dict[str, Optional[st.runtime.uploaded_file_manager.UploadedFile]]) -> bool:
    return all(upload_map.values())


def main() -> None:
    init_session_state()
    st.markdown(app_css(), unsafe_allow_html=True)

    st.title("📦 Sendeplan-Generator")
    st.caption("Dateien hochladen, Sendeplan erzeugen und HTML zum Download bereitstellen.")

    with st.sidebar:
        st.header("Quelldateien")
        csv_separator = st.text_input("CSV-Trennzeichen", value=";", max_chars=1)

        kunden_file = st.file_uploader(
            "Kundenliste",
            type=["xlsx", "xls", "xlsm", "csv"],
            help="Feste Spalten: A, I, J, K, L, M, N",
        )
        sap_file = st.file_uploader(
            "SAP-Datei",
            type=["xlsx", "xls", "xlsm", "csv"],
            help="Feste Spalten: A, H, I, O, Y",
        )
        transport_file = st.file_uploader(
            "Transportgruppen",
            type=["xlsx", "xls", "xlsm", "csv"],
            help="Feste Spalten: A, C",
        )
        kisoft_file = st.file_uploader(
            "Kisoft-Datei",
            type=["csv", "xlsx", "xls", "xlsm"],
            help="Benötigte Felder: SAP Rahmentour, CSB Tournummer, Verladetor",
        )
        kostenstellen_file = st.file_uploader(
            "Kostenstellen-Datei",
            type=["xlsx", "xls", "xlsm", "csv"],
            help="Benötigte Felder: sap_von, sap_bis, tourengruppe, leiter",
        )

        upload_map = {
            "kunden": kunden_file,
            "sap": sap_file,
            "transport": transport_file,
            "kisoft": kisoft_file,
            "kostenstellen": kostenstellen_file,
        }

        if not all_required_uploads_present(upload_map):
            st.info("Bitte alle fünf Quelldateien hochladen. Danach werden Vorschau, Druck und HTML-Download freigeschaltet.")
            st.markdown("**Verwendete Regeln**")
            st.markdown(
                """
                - Kundenliste über feste Spalten **A, I, J, K, L, M, N**
                - SAP über feste Spalten **A, H, I, O, Y**
                - Transportgruppen über feste Spalten **A, C**
                - Kisoft über **SAP Rahmentour**, **CSB Tournummer**, **Verladetor**
                - Kostenstellen über **sap_von**, **sap_bis**, **tourengruppe**, **leiter**
                """
            )
            st.stop()

    try:
        customers_df, plan_rows_df, counts = prepare_dataframes(
            kunden_file.getvalue(),
            kunden_file.name,
            sap_file.getvalue(),
            sap_file.name,
            transport_file.getvalue(),
            transport_file.name,
            kisoft_file.getvalue(),
            kisoft_file.name,
            kostenstellen_file.getvalue(),
            kostenstellen_file.name,
            csv_separator or ";",
        )
    except Exception as exc:
        st.error(f"Die hochgeladenen Dateien konnten nicht verarbeitet werden: {exc}")
        st.stop()

    with st.sidebar:
        st.divider()
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

        st.divider()
        st.subheader("HTML-Export")
        export_html = build_full_document_html(filtered_customers, plan_rows_df, include_separators=True)
        filename_suffix = normalize_text(st.session_state.category_filter).lower() or "alle"

        st.download_button(
            label="Standalone-HTML mit Suche herunterladen",
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

        st.markdown("### Uploads")
        st.markdown(
            f"""
            - Kundenliste: **{kunden_file.name}**
            - SAP: **{sap_file.name}**
            - Transport: **{transport_file.name}**
            - Kisoft: **{kisoft_file.name}**
            - Kostenstellen: **{kostenstellen_file.name}**
            """
        )

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

            st.markdown(
                "<div class='print-note'>Einzeldruck und Massendruck öffnen ein separates HTML-Dokument mit A4-Layout und starten dort den Browser-Druck.</div>",
                unsafe_allow_html=True,
            )
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


if __name__ == "__main__":
    main()
