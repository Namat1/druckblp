
from __future__ import annotations

import html
import io
import json
import re
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

WOCHENTAGE_KURZ = {
    "Mo": "Montag",
    "Mon": "Montag",
    "Die": "Dienstag",
    "Di": "Dienstag",
    "Dienstag": "Dienstag",
    "Mi": "Mittwoch",
    "Mitt": "Mittwoch",
    "Mittwoch": "Mittwoch",
    "Do": "Donnerstag",
    "Don": "Donnerstag",
    "Donnerstag": "Donnerstag",
    "Fr": "Freitag",
    "Freitag": "Freitag",
    "Sa": "Samstag",
    "Samstag": "Samstag",
    "So": "Sonntag",
    "Sonntag": "Sonntag",
}

KATEGORIEN = ["Alle", "Malchow", "NMS", "MK", "Direkt"]

UPLOAD_SPECS = {
    "kunden": {
        "label": "Kundenliste",
        "sheet_default": "Liste m.Form",
        "mapping": {
            "Fachberater": "A",
            "CSB_Nr": "I",
            "SAP_Nr": "J",
            "Name": "K",
            "Strasse": "L",
            "PLZ": "M",
            "Ort": "N",
        },
        "key": "SAP_Nr",
    },
    "sap": {
        "label": "SAP-Datei",
        "sheet_default": "Rohdaten",
        "mapping": {
            "SAP_Nr": "A",
            "Bestelltag": "H",
            "Bestellzeitende": "I",
            "Liefertyp_ID": "O",
            "Rahmentour_Raw": "Y",
        },
        "key": "SAP_Nr",
    },
    "transport": {
        "label": "Transportgruppen",
        "sheet_default": "Tabelle1",
        "mapping": {
            "Liefertyp_ID": "A",
            "Liefertyp_Name": "C",
        },
        "key": "Liefertyp_ID",
    },
}

KISOFT_REQUIRED_COLUMNS = [
    "SAP Rahmentour",
    "CSB Tournummer",
    "Wochentag",
    "Bereitstelldatum und -zeit",
    "Verladetor",
]

KOSTENPLAN_LIEFERANTEN = {
    "Lagerware": (4, 5, 6),
    "AVO": (7, 8, 9),
    "Werbemittel Sonder": (10, 11, 12),
    "Werbemittel": (13, 14, 15),
    "Hamburger Jungs": (16, 17, 18),
}


def normalize_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).replace("\xa0", " ").strip()


def normalize_digits(value: object) -> str:
    return "".join(ch for ch in normalize_text(value) if ch.isdigit())


def day_name_from_number(value: object) -> str:
    try:
        return WOCHENTAGE[int(float(normalize_text(value)))]
    except Exception:
        return normalize_text(value)


def day_name_from_short(value: object) -> str:
    text = normalize_text(value)
    if not text:
        return ""
    return WOCHENTAGE_KURZ.get(text, text)


def excel_column_to_index(letter: str) -> int:
    result = 0
    for char in letter.upper():
        if not ("A" <= char <= "Z"):
            raise ValueError(f"Ungültiger Spaltenbuchstabe: {letter}")
        result = result * 26 + (ord(char) - ord("A") + 1)
    return result - 1


def parse_range_text(range_text: object) -> List[Tuple[int, int]]:
    text = normalize_text(range_text)
    if not text:
        return []

    parts = [part.strip() for part in text.split("+")]
    ranges: List[Tuple[int, int]] = []
    prefix_base: Optional[int] = None

    for part in parts:
        numbers = re.findall(r"\d+", part)
        if not numbers:
            continue

        if "-" in part and len(numbers) >= 2:
            start = int(numbers[0])
            end = int(numbers[1])
            ranges.append((start, end))
            if start >= 1000:
                prefix_base = (start // 1000) * 1000
        else:
            value = int(numbers[0])
            if prefix_base is not None and value < 100:
                value = prefix_base + value
            ranges.append((value, value))

    return ranges


def build_kisoft_key(rahmentour_raw: object) -> str:
    digits = normalize_digits(rahmentour_raw)
    if not digits:
        return ""
    return f"00{digits[:8]}"


def format_clock(value: object) -> str:
    text = normalize_text(value)
    if not text:
        return ""
    if ":" in text:
        return text.replace(" ", "")
    digits = re.sub(r"\D", "", text)
    if len(digits) == 4:
        return f"{digits[:2]}:{digits[2:]}"
    if len(digits) == 3:
        return f"0{digits[0]}:{digits[1:]}"
    if len(digits) == 2:
        return f"{digits}:00"
    return text


def classify_customer(rahmentour_raw: object, csb_nr: object) -> str:
    rahmentour_text = normalize_text(rahmentour_raw).upper()
    csb_text = normalize_digits(csb_nr)

    if "M" in rahmentour_text:
        return "Malchow"
    if "N" in rahmentour_text:
        return "NMS"
    if re.fullmatch(r"\d884", csb_text) or re.fullmatch(r"\d881", csb_text):
        return "MK"
    return "Direkt"


@st.cache_data(show_spinner=False)
def list_excel_sheets(file_bytes: bytes) -> List[str]:
    excel_file = pd.ExcelFile(io.BytesIO(file_bytes))
    return list(excel_file.sheet_names)


@st.cache_data(show_spinner=False)
def read_excel_raw(file_bytes: bytes, sheet_name: str) -> pd.DataFrame:
    return pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet_name, header=None, dtype=str)


@st.cache_data(show_spinner=False)
def read_csv_table(file_bytes: bytes, filename: str, separator: str) -> pd.DataFrame:
    errors: List[str] = []
    for encoding in ["utf-8-sig", "utf-8", "latin1", "cp1252"]:
        try:
            return pd.read_csv(
                io.BytesIO(file_bytes),
                sep=separator,
                dtype=str,
                encoding=encoding,
            )
        except Exception as exc:
            errors.append(f"{encoding}: {exc}")
    raise ValueError(f"{filename} konnte nicht gelesen werden: {' | '.join(errors)}")


def cleanup_dataframe(df: pd.DataFrame, key_column: Optional[str] = None) -> pd.DataFrame:
    result = df.copy()
    for column in result.columns:
        result[column] = result[column].map(normalize_text)

    result = result.replace("", pd.NA).dropna(how="all").fillna("").reset_index(drop=True)
    if key_column and key_column in result.columns:
        result = result[result[key_column].map(normalize_text) != ""].reset_index(drop=True)
    return result


def drop_header_row_if_needed(df: pd.DataFrame, key_column: str) -> pd.DataFrame:
    if df.empty:
        return df
    first_row = " | ".join(normalize_text(v).lower() for v in df.iloc[0].tolist())
    first_key = normalize_text(df.iloc[0].get(key_column, "")).lower()
    tokens = [
        "sap-nr",
        "sap nr",
        "debitoren",
        "kundennummer",
        "marktname",
        "bestellzeit",
        "liefertyp",
        "transport",
        "ort",
        "strasse",
    ]
    if any(token in first_row or token in first_key for token in tokens):
        return df.iloc[1:].reset_index(drop=True)
    return df


def extract_fixed_columns(raw_df: pd.DataFrame, mapping: Dict[str, str], label: str) -> pd.DataFrame:
    data: Dict[str, pd.Series] = {}
    for target_name, column_letter in mapping.items():
        column_index = excel_column_to_index(column_letter)
        if column_index >= raw_df.shape[1]:
            raise ValueError(f"{label}: Spalte {column_letter} fehlt.")
        data[target_name] = raw_df.iloc[:, column_index]
    return pd.DataFrame(data)


@st.cache_data(show_spinner=False)
def load_fixed_excel_dataset(file_bytes: bytes, dataset_key: str, sheet_name: str) -> pd.DataFrame:
    spec = UPLOAD_SPECS[dataset_key]
    raw_df = read_excel_raw(file_bytes, sheet_name)
    df = extract_fixed_columns(raw_df, spec["mapping"], spec["label"])
    df = cleanup_dataframe(df, spec["key"])
    df = drop_header_row_if_needed(df, spec["key"])
    return df.reset_index(drop=True)


@st.cache_data(show_spinner=False)
def load_kisoft_dataset(file_bytes: bytes, filename: str, separator: str) -> pd.DataFrame:
    df = read_csv_table(file_bytes, filename, separator)
    rename_map: Dict[str, str] = {}
    for column in df.columns:
        clean_col = normalize_text(column).replace('"', "")
        rename_map[column] = clean_col
    df = df.rename(columns=rename_map)

    missing = [col for col in KISOFT_REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Kisoft-Datei: Pflichtspalten fehlen: {', '.join(missing)}")

    df = cleanup_dataframe(df[KISOFT_REQUIRED_COLUMNS].copy(), "SAP Rahmentour")
    return df


@st.cache_data(show_spinner=False)
def load_kostenplan_dataset(file_bytes: bytes, sheet_name: str) -> pd.DataFrame:
    raw_df = read_excel_raw(file_bytes, sheet_name).fillna("")
    raw_df = raw_df.apply(lambda col: col.map(normalize_text))

    records: List[Dict[str, str]] = []
    for row_idx in range(len(raw_df)):
        day_name = normalize_text(raw_df.iloc[row_idx, 3] if raw_df.shape[1] > 3 else "")
        if day_name not in WOCHENTAGE.values():
            continue

        next_idx = row_idx + 1
        while next_idx < len(raw_df):
            row = raw_df.iloc[next_idx]
            label = normalize_text(row.iloc[0] if raw_df.shape[1] > 0 else "")
            range_text = normalize_text(row.iloc[1] if raw_df.shape[1] > 1 else "")

            if not label and not range_text:
                break
            if label == "Tourengruppen":
                break
            if "Kostenst." in normalize_text(row.iloc[2] if raw_df.shape[1] > 2 else ""):
                next_idx += 1
                continue

            record: Dict[str, str] = {
                "Liefertag": day_name,
                "Tourgruppenname": label,
                "Bereich": range_text,
                "Kostenstelle": normalize_text(row.iloc[2] if raw_df.shape[1] > 2 else ""),
                "Leiter": normalize_text(row.iloc[3] if raw_df.shape[1] > 3 else ""),
            }

            for supplier_name, (idx_time, idx_lead, idx_day) in KOSTENPLAN_LIEFERANTEN.items():
                record[f"{supplier_name}_Zeit"] = normalize_text(row.iloc[idx_time] if raw_df.shape[1] > idx_time else "")
                record[f"{supplier_name}_Vorlauf"] = normalize_text(row.iloc[idx_lead] if raw_df.shape[1] > idx_lead else "")
                record[f"{supplier_name}_Bestelltag"] = normalize_text(row.iloc[idx_day] if raw_df.shape[1] > idx_day else "")
            records.append(record)
            next_idx += 1

    if not records:
        raise ValueError("Im Kostenstellenplan konnten keine Tourgruppenblöcke gelesen werden.")

    return pd.DataFrame(records)


def lookup_kostenplan_row(csb_tournummer: object, df_kostenplan: pd.DataFrame) -> Optional[pd.Series]:
    digits = normalize_digits(csb_tournummer)
    if not digits:
        return None

    number = int(digits)
    for _, row in df_kostenplan.iterrows():
        for start, end in parse_range_text(row.get("Bereich", "")):
            if start <= number <= end:
                return row
    return None


def summarize_cost_schedules(customer_rows: pd.DataFrame, df_kostenplan: pd.DataFrame) -> List[Dict[str, object]]:
    if customer_rows.empty:
        return []

    unique_tours = (
        customer_rows[["CSB Tournummer", "Wochentag"]]
        .drop_duplicates()
        .sort_values(["CSB Tournummer", "Wochentag"], na_position="last")
    )

    schedules: List[Dict[str, object]] = []
    for _, tour_row in unique_tours.iterrows():
        csb_tour = normalize_text(tour_row.get("CSB Tournummer", ""))
        lookup = lookup_kostenplan_row(csb_tour, df_kostenplan)
        if lookup is None:
            schedules.append(
                {
                    "csb_tour": csb_tour,
                    "liefertag": day_name_from_short(tour_row.get("Wochentag", "")),
                    "tourengruppe": "",
                    "kostenstelle": "",
                    "leiter": "",
                    "supplier_rows": [],
                    "hinweis": "Keine Zuordnung im Kostenstellenplan gefunden.",
                }
            )
            continue

        supplier_rows: List[Dict[str, str]] = []
        for supplier_name in KOSTENPLAN_LIEFERANTEN:
            zeit = format_clock(lookup.get(f"{supplier_name}_Zeit", ""))
            vorlauf = normalize_text(lookup.get(f"{supplier_name}_Vorlauf", ""))
            bestelltag = day_name_from_short(lookup.get(f"{supplier_name}_Bestelltag", ""))
            if zeit or vorlauf or bestelltag:
                supplier_rows.append(
                    {
                        "lieferant": supplier_name,
                        "zeit": zeit,
                        "vorlauf": vorlauf,
                        "bestelltag": bestelltag,
                    }
                )

        schedules.append(
            {
                "csb_tour": csb_tour,
                "liefertag": normalize_text(lookup.get("Liefertag", "")) or day_name_from_short(tour_row.get("Wochentag", "")),
                "tourengruppe": normalize_text(lookup.get("Tourgruppenname", "")),
                "bereich": normalize_text(lookup.get("Bereich", "")),
                "kostenstelle": normalize_text(lookup.get("Kostenstelle", "")),
                "leiter": normalize_text(lookup.get("Leiter", "")),
                "supplier_rows": supplier_rows,
                "hinweis": "",
            }
        )

    return schedules


@st.cache_data(show_spinner=False)
def prepare_dataframes(
    kunden_bytes: bytes,
    sap_bytes: bytes,
    transport_bytes: bytes,
    kisoft_bytes: bytes,
    kostenplan_bytes: bytes,
    csv_separator: str,
    kunden_sheet: str,
    sap_sheet: str,
    transport_sheet: str,
    kostenplan_sheet: str,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df_kunden = load_fixed_excel_dataset(kunden_bytes, "kunden", kunden_sheet)
    df_sap = load_fixed_excel_dataset(sap_bytes, "sap", sap_sheet)
    df_transport = load_fixed_excel_dataset(transport_bytes, "transport", transport_sheet)
    df_kisoft = load_kisoft_dataset(kisoft_bytes, "Kisoft.csv", csv_separator)
    df_kostenplan = load_kostenplan_dataset(kostenplan_bytes, kostenplan_sheet)

    df_sap["Bestelltag_Name"] = df_sap["Bestelltag"].map(day_name_from_number)
    df_sap["Kisoft_Key"] = df_sap["Rahmentour_Raw"].map(build_kisoft_key)

    df_sap = df_sap.merge(df_transport, on="Liefertyp_ID", how="left")
    df_sap = df_sap.merge(
        df_kisoft[KISOFT_REQUIRED_COLUMNS],
        left_on="Kisoft_Key",
        right_on="SAP Rahmentour",
        how="left",
    )

    df_sap["Wochentag"] = df_sap["Wochentag"].map(day_name_from_short)
    df_sap["CSB Tournummer"] = df_sap["CSB Tournummer"].map(normalize_text)
    df_sap["Verladetor"] = df_sap["Verladetor"].map(normalize_text)

    # Doppelte Planzeilen entfernen:
    # gleiche SAP-Nummer + gleicher Bestelltag + gleicher Liefertyp nur einmal.
    df_sap = (
        df_sap.sort_values(["SAP_Nr", "Bestelltag", "Liefertyp_ID", "Bestellzeitende", "Rahmentour_Raw"])
        .drop_duplicates(subset=["SAP_Nr", "Bestelltag", "Liefertyp_ID"], keep="first")
        .reset_index(drop=True)
    )

    kunden_basis = df_kunden.copy()
    kunden_basis = kunden_basis.merge(
        df_sap[["SAP_Nr", "Rahmentour_Raw"]].drop_duplicates("SAP_Nr"),
        on="SAP_Nr",
        how="left",
    )
    kunden_basis["Kategorie"] = kunden_basis.apply(
        lambda row: classify_customer(row.get("Rahmentour_Raw", ""), row.get("CSB_Nr", "")),
        axis=1,
    )

    plan_rows = df_sap.merge(
        kunden_basis[["SAP_Nr", "CSB_Nr", "Name", "Strasse", "PLZ", "Ort", "Fachberater", "Kategorie"]],
        on="SAP_Nr",
        how="left",
    )

    def infer_liefertag(row: pd.Series) -> str:
        weekday = normalize_text(row.get("Wochentag", ""))
        if weekday:
            return weekday
        tour_digits = normalize_digits(row.get("CSB Tournummer", ""))
        if tour_digits and tour_digits[0].isdigit():
            day_num = int(tour_digits[0])
            if day_num in WOCHENTAGE:
                return WOCHENTAGE[day_num]
        return normalize_text(row.get("Bestelltag_Name", ""))

    plan_rows["Liefertag"] = plan_rows.apply(infer_liefertag, axis=1)
    plan_rows["Sortiment"] = plan_rows["Liefertyp_Name"].map(normalize_text)
    plan_rows["Bestellzeitende"] = plan_rows["Bestellzeitende"].map(normalize_text)
    plan_rows["SortKey_Liefertag"] = plan_rows["Liefertag"].map({name: i for i, name in WOCHENTAGE.items()}).fillna(99)
    plan_rows["SortKey_Bestelltag"] = pd.to_numeric(plan_rows["Bestelltag"], errors="coerce").fillna(99)
    plan_rows["SortKey_Sortiment"] = plan_rows["Sortiment"].map(normalize_text)

    customer_counts = (
        plan_rows.groupby("SAP_Nr")
        .size()
        .rename("Planzeilen_Anzahl")
        .reset_index()
    )
    kunden_basis = kunden_basis.merge(customer_counts, on="SAP_Nr", how="left")
    kunden_basis["Planzeilen_Anzahl"] = kunden_basis["Planzeilen_Anzahl"].fillna(0).astype(int)

    return kunden_basis, plan_rows, df_kostenplan


def filter_customers(df_customers: pd.DataFrame, category: str, search_text: str, only_with_plan_rows: bool) -> pd.DataFrame:
    result = df_customers.copy()

    if only_with_plan_rows:
        result = result[result["Planzeilen_Anzahl"] > 0]

    if category != "Alle":
        result = result[result["Kategorie"] == category]

    search = normalize_text(search_text).lower()
    if search:
        result = result[
            result["SAP_Nr"].str.lower().str.contains(search, na=False)
            | result["Name"].str.lower().str.contains(search, na=False)
        ]

    return result.sort_values(["Name", "SAP_Nr"], na_position="last").reset_index(drop=True)


def app_css() -> str:
    return """
    <style>
        :root {
            --bg: #eef2f7;
            --sidebar: #dde6f0;
            --card: #ffffff;
            --paper: #ffffff;
            --line: #c6d0db;
            --line-strong: #9daaba;
            --text: #16202a;
            --muted: #526173;
            --accent: #1b4f72;
            --accent-soft: #e8f1f8;
            --paper-width: 210mm;
            --paper-min-height: 297mm;
        }

        html, body, [class*="css"] {
            color: var(--text) !important;
        }

        .stApp {
            background: var(--bg);
            color: var(--text);
        }

        .main .block-container {
            max-width: 1500px;
            padding-top: 1.2rem;
            padding-bottom: 2rem;
        }

        [data-testid="stSidebar"] {
            background: var(--sidebar) !important;
            border-right: 1px solid var(--line);
        }

        [data-testid="stSidebar"] * {
            color: var(--text) !important;
        }

        [data-testid="stFileUploaderDropzone"],
        [data-baseweb="input"],
        [data-baseweb="select"] > div,
        .stTextInput input,
        .stSelectbox [data-baseweb="select"] > div,
        .stMultiSelect [data-baseweb="select"] > div,
        textarea,
        input {
            background: #ffffff !important;
            color: var(--text) !important;
            border-color: var(--line-strong) !important;
        }

        .stButton > button,
        .stDownloadButton > button {
            background: #ffffff !important;
            color: var(--text) !important;
            border: 1px solid var(--line-strong) !important;
            border-radius: 10px !important;
            font-weight: 600 !important;
        }

        .stButton > button:hover,
        .stDownloadButton > button:hover {
            border-color: var(--accent) !important;
            color: var(--accent) !important;
        }

        [data-testid="stFileUploaderDropzone"] {
            border: 1.5px dashed var(--line-strong) !important;
            border-radius: 14px !important;
            background: rgba(255, 255, 255, 0.7) !important;
        }

        .stAlert {
            background: #ffffff !important;
            color: var(--text) !important;
            border: 1px solid var(--line) !important;
        }

        .hero-box, .info-box, .empty-box, .status-box {
            background: var(--card);
            border: 1px solid var(--line);
            border-radius: 16px;
            box-shadow: 0 8px 22px rgba(10, 30, 50, 0.06);
        }

        .hero-box {
            padding: 1.2rem 1.4rem;
            margin-bottom: 1rem;
        }

        .hero-box h1 {
            margin: 0 0 0.35rem 0;
            font-size: 1.9rem;
            color: var(--accent);
        }

        .hero-box p {
            margin: 0;
            color: var(--muted);
            font-size: 1rem;
        }

        .info-box {
            padding: 1rem 1.1rem;
            margin-bottom: 1rem;
        }

        .info-box h3 {
            margin: 0 0 0.65rem 0;
            color: var(--accent);
            font-size: 1.05rem;
        }

        .info-box ul {
            margin: 0;
            padding-left: 1.1rem;
        }

        .info-box li {
            margin: 0.35rem 0;
            color: var(--muted);
        }

        .status-box {
            padding: 0.9rem 1rem;
            margin-bottom: 0.8rem;
        }

        .status-label {
            display: block;
            font-size: 0.82rem;
            color: var(--muted);
            margin-bottom: 0.15rem;
        }

        .status-value {
            display: block;
            font-size: 1.25rem;
            font-weight: 700;
            color: var(--text);
        }

        .export-row {
            margin: 0.25rem 0 1rem 0;
        }

        .print-note {
            color: var(--muted);
            font-size: 0.95rem;
            margin: 0.3rem 0 0.9rem 0;
        }

        .empty-box {
            padding: 1.2rem 1.3rem;
            max-width: 900px;
            margin: 0 auto;
        }

        .empty-box h3 {
            margin-top: 0;
            color: var(--accent);
        }

        .empty-box p, .empty-box li {
            color: var(--muted);
        }

        .paper {
            width: 100%;
            max-width: var(--paper-width);
            min-height: var(--paper-min-height);
            margin: 0 auto 1.5rem auto;
            background: var(--paper);
            box-shadow: 0 10px 28px rgba(15, 23, 42, 0.12);
            border-radius: 8px;
            padding: 12mm;
            color: #1f1f1f;
        }

        .paper * {
            color: #1f1f1f;
        }

        .paper-header {
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            gap: 8mm;
            margin-bottom: 6mm;
            border-bottom: 1px solid #d7dde5;
            padding-bottom: 4mm;
        }

        .paper-title {
            margin: 0;
            font-size: 19pt;
            color: #184b6b;
        }

        .paper-subtitle {
            margin-top: 1.5mm;
            font-size: 9.5pt;
            color: #566372;
        }

        .header-facts {
            font-size: 9.5pt;
            text-align: right;
            color: #3d4a57;
            min-width: 55mm;
        }

        .meta-grid {
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 3.5mm 6mm;
            margin-bottom: 6mm;
        }

        .meta-card {
            border: 1px solid #dfe5eb;
            border-radius: 6px;
            padding: 3mm 3.5mm;
            background: #fafcfe;
        }

        .meta-label {
            display: block;
            font-size: 8.4pt;
            color: #667381;
            text-transform: uppercase;
            letter-spacing: 0.04em;
            margin-bottom: 0.8mm;
        }

        .meta-value {
            display: block;
            font-size: 10.5pt;
            font-weight: 700;
            color: #1f1f1f;
        }

        .section-title {
            margin: 6mm 0 2mm 0;
            font-size: 11.5pt;
            color: #184b6b;
        }

        .plan-table {
            width: 100%;
            border-collapse: collapse;
            border: 1.5px solid #aaa;
            font-size: 9pt;
            margin-top: 2mm;
        }

        .plan-table th,
        .plan-table td {
            border: 1px solid #aaa;
            padding: 2.2mm 2mm;
            text-align: left;
            vertical-align: top;
        }

        .plan-table th {
            background: #eef3f9;
            font-weight: 700;
        }

        .cost-block {
            border: 1px solid #dce3eb;
            border-radius: 7px;
            padding: 3.2mm 3.5mm;
            margin-top: 3mm;
            background: #fcfdff;
            page-break-inside: avoid;
        }

        .cost-head {
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap: 2mm 4mm;
            margin-bottom: 2.5mm;
            font-size: 8.8pt;
        }

        .cost-head strong {
            color: #184b6b;
        }

        .cost-note {
            font-size: 8.7pt;
            color: #7a2f2f;
            margin: 1.5mm 0 0 0;
        }

        .supplier-table {
            width: 100%;
            border-collapse: collapse;
            border: 1px solid #b8c2cf;
            font-size: 8.7pt;
        }

        .supplier-table th,
        .supplier-table td {
            border: 1px solid #b8c2cf;
            padding: 1.8mm 1.7mm;
            text-align: left;
            vertical-align: top;
        }

        .supplier-table th {
            background: #f0f5fa;
            font-weight: 700;
        }

        .cover-page,
        .separator-page {
            width: 100%;
            max-width: var(--paper-width);
            min-height: var(--paper-min-height);
            margin: 0 auto 1.5rem auto;
            background: white;
            box-shadow: 0 10px 28px rgba(15, 23, 42, 0.12);
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
            margin: 0 0 6mm 0;
            font-size: 26pt;
            color: #184b6b;
        }

        .cover-page h2,
        .separator-page h2 {
            margin: 0 0 4mm 0;
            font-size: 15pt;
            color: #253342;
        }

        .cover-page p,
        .separator-page p {
            margin: 1mm 0;
            font-size: 10.5pt;
            color: #5b6876;
        }

        .print-toolbar {
            display: flex;
            gap: 0.7rem;
            flex-wrap: wrap;
            margin-bottom: 0.6rem;
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
            .cover-page:last-child,
            .separator-page:last-child {
                page-break-after: auto !important;
                break-after: auto !important;
            }
        }
    </style>
    """


def render_status_box(label: str, value: str) -> str:
    return f"""
    <div class="status-box">
        <span class="status-label">{html.escape(label)}</span>
        <span class="status-value">{html.escape(value)}</span>
    </div>
    """


def render_plan_table(rows: pd.DataFrame) -> str:
    if rows.empty:
        return "<p>Keine Planzeilen vorhanden.</p>"

    ordered = rows.sort_values(["SortKey_Liefertag", "SortKey_Bestelltag", "SortKey_Sortiment", "Bestellzeitende"])
    body_rows: List[str] = []

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


def render_cost_schedule_blocks(schedule_blocks: List[Dict[str, object]]) -> str:
    if not schedule_blocks:
        return "<p>Keine Zusatzdaten aus dem Kostenstellenplan vorhanden.</p>"

    blocks: List[str] = []
    for block in schedule_blocks:
        supplier_rows = block.get("supplier_rows", [])
        if supplier_rows:
            rows_html = "".join(
                f"""
                <tr>
                    <td>{html.escape(normalize_text(item.get('lieferant', '')))}</td>
                    <td>{html.escape(normalize_text(item.get('zeit', '')))}</td>
                    <td>{html.escape(normalize_text(item.get('vorlauf', '')))}</td>
                    <td>{html.escape(normalize_text(item.get('bestelltag', '')))}</td>
                </tr>
                """
                for item in supplier_rows
            )
            supplier_table = f"""
            <table class="supplier-table">
                <thead>
                    <tr>
                        <th>Bereich</th>
                        <th>Zeit</th>
                        <th>Vorlauf in Tagen</th>
                        <th>Bestelltag</th>
                    </tr>
                </thead>
                <tbody>
                    {rows_html}
                </tbody>
            </table>
            """
        else:
            supplier_table = ""

        hint_html = ""
        if block.get("hinweis"):
            hint_html = f"<p class='cost-note'>{html.escape(normalize_text(block.get('hinweis', '')))}</p>"

        blocks.append(
            f"""
            <div class="cost-block">
                <div class="cost-head">
                    <div><strong>CSB-Tour:</strong> {html.escape(normalize_text(block.get('csb_tour', '')))}</div>
                    <div><strong>Liefertag:</strong> {html.escape(normalize_text(block.get('liefertag', '')))}</div>
                    <div><strong>Tourengruppe:</strong> {html.escape(normalize_text(block.get('tourengruppe', '')))}</div>
                    <div><strong>Bereich:</strong> {html.escape(normalize_text(block.get('bereich', '')))}</div>
                    <div><strong>Kostenstelle:</strong> {html.escape(normalize_text(block.get('kostenstelle', '')))}</div>
                    <div><strong>Leiter:</strong> {html.escape(normalize_text(block.get('leiter', '')))}</div>
                </div>
                {supplier_table}
                {hint_html}
            </div>
            """
        )

    return "".join(blocks)


def render_customer_plan(customer: pd.Series, customer_rows: pd.DataFrame, df_kostenplan: pd.DataFrame) -> str:
    sap_nr = normalize_text(customer.get("SAP_Nr", ""))
    name = normalize_text(customer.get("Name", ""))
    address = normalize_text(customer.get("Strasse", ""))
    plz_ort = " ".join(filter(None, [normalize_text(customer.get("PLZ", "")), normalize_text(customer.get("Ort", ""))]))
    category = normalize_text(customer.get("Kategorie", ""))
    csb_nr = normalize_text(customer.get("CSB_Nr", ""))
    fachberater = normalize_text(customer.get("Fachberater", ""))

    rahmentouren = sorted({normalize_text(v) for v in customer_rows.get("Rahmentour_Raw", pd.Series(dtype=str)).tolist() if normalize_text(v)})
    verladetore = sorted({normalize_text(v) for v in customer_rows.get("Verladetor", pd.Series(dtype=str)).tolist() if normalize_text(v)})
    csb_touren = sorted({normalize_text(v) for v in customer_rows.get("CSB Tournummer", pd.Series(dtype=str)).tolist() if normalize_text(v)})

    schedule_blocks = summarize_cost_schedules(customer_rows, df_kostenplan)

    return f"""
    <div class="paper">
        <div class="paper-header">
            <div>
                <h1 class="paper-title">Sendeplan</h1>
                <div class="paper-subtitle">Generiert am {datetime.now().strftime('%d.%m.%Y %H:%M')} Uhr</div>
            </div>
            <div class="header-facts">
                <div><strong>Kategorie:</strong> {html.escape(category)}</div>
                <div><strong>Rahmentouren:</strong> {html.escape(", ".join(rahmentouren) or "-")}</div>
                <div><strong>Verladetore:</strong> {html.escape(", ".join(verladetore) or "-")}</div>
            </div>
        </div>

        <div class="meta-grid">
            <div class="meta-card"><span class="meta-label">SAP-Nummer</span><span class="meta-value">{html.escape(sap_nr)}</span></div>
            <div class="meta-card"><span class="meta-label">CSB-Nummer</span><span class="meta-value">{html.escape(csb_nr)}</span></div>
            <div class="meta-card"><span class="meta-label">Kunde</span><span class="meta-value">{html.escape(name)}</span></div>
            <div class="meta-card"><span class="meta-label">Fachberater</span><span class="meta-value">{html.escape(fachberater)}</span></div>
            <div class="meta-card"><span class="meta-label">Adresse</span><span class="meta-value">{html.escape(address)}</span></div>
            <div class="meta-card"><span class="meta-label">Postleitzahl / Ort</span><span class="meta-value">{html.escape(plz_ort)}</span></div>
            <div class="meta-card"><span class="meta-label">Planzeilen</span><span class="meta-value">{html.escape(str(len(customer_rows)))}</span></div>
            <div class="meta-card"><span class="meta-label">CSB-Touren</span><span class="meta-value">{html.escape(", ".join(csb_touren) or "-")}</span></div>
        </div>

        <h3 class="section-title">Sortimentsplan</h3>
        {render_plan_table(customer_rows)}

        <h3 class="section-title">Kostenstellenplan mit Lagerware, AVO und Werbemitteln</h3>
        {render_cost_schedule_blocks(schedule_blocks)}
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


def build_full_document_html(customers: pd.DataFrame, plan_rows: pd.DataFrame, df_kostenplan: pd.DataFrame, include_separators: bool = True) -> str:
    docs: List[str] = [
        render_cover_page(
            title="Sendeplan-Generator",
            subtitle="Gesamtplan",
            lines=[
                f"Erstellt am {datetime.now().strftime('%d.%m.%Y %H:%M')} Uhr",
                f"Kundenanzahl: {len(customers)}",
                "Enthält Deckblatt, optionale Zwischenseiten und Kundenseiten.",
            ],
        )
    ]

    for _, customer in customers.iterrows():
        customer_rows = plan_rows[plan_rows["SAP_Nr"] == customer["SAP_Nr"]].copy()
        if include_separators:
            docs.append(render_separator_page(customer))
        docs.append(render_customer_plan(customer, customer_rows, df_kostenplan))

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


def build_single_document_html(customer: pd.Series, customer_rows: pd.DataFrame, df_kostenplan: pd.DataFrame) -> str:
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
        {render_customer_plan(customer, customer_rows, df_kostenplan)}
    </body>
    </html>
    """


def render_print_buttons(single_html: str, bulk_html: str) -> None:
    payload = {
        "single": single_html,
        "bulk": bulk_html,
    }
    components.html(
        f"""
        <div class="print-toolbar">
            <button onclick="openAndPrint('single')">Aktuellen Kunden drucken</button>
            <button onclick="openAndPrint('bulk')">Gefilterten Gesamtplan drucken</button>
        </div>
        <script>
            const docs = {json.dumps(payload)};
            function openAndPrint(kind) {{
                const win = window.open('', '_blank');
                if (!win) {{
                    alert('Der Browser hat das Druckfenster blockiert.');
                    return;
                }}
                win.document.open();
                win.document.write(docs[kind]);
                win.document.close();
                win.focus();
                win.onload = function() {{
                    win.print();
                }};
            }}
        </script>
        """,
        height=70,
    )


def ensure_session_state() -> None:
    if "category_filter" not in st.session_state:
        st.session_state.category_filter = "Alle"
    if "selected_sap" not in st.session_state:
        st.session_state.selected_sap = ""
    if "show_only_with_plan_rows" not in st.session_state:
        st.session_state.show_only_with_plan_rows = True


def set_category(category: str) -> None:
    st.session_state.category_filter = category


def build_option_labels(df_customers: pd.DataFrame) -> Dict[str, str]:
    return {
        row["SAP_Nr"]: f"{row['SAP_Nr']} | {row['Name']} | {row['Ort']}"
        for _, row in df_customers.iterrows()
    }


def all_required_uploads_present(upload_map: Dict[str, object]) -> bool:
    return all(upload_map.values())


def file_uploader_block() -> Dict[str, object]:
    st.sidebar.subheader("Datei-Uploads")
    kunden_file = st.sidebar.file_uploader("Kundenliste", type=["xlsx", "xls", "xlsm"], key="kunden_file")
    sap_file = st.sidebar.file_uploader("SAP-Datei", type=["xlsx", "xls", "xlsm"], key="sap_file")
    transport_file = st.sidebar.file_uploader("Transportgruppen", type=["xlsx", "xls", "xlsm"], key="transport_file")
    kisoft_file = st.sidebar.file_uploader("Kisoft", type=["csv"], key="kisoft_file")
    kostenplan_file = st.sidebar.file_uploader("Kostenstellenplan", type=["xlsx", "xls", "xlsm"], key="kostenplan_file")

    separator = st.sidebar.selectbox("CSV-Trennzeichen für Kisoft", options=[";", ","], index=0)
    return {
        "kunden": kunden_file,
        "sap": sap_file,
        "transport": transport_file,
        "kisoft": kisoft_file,
        "kostenplan": kostenplan_file,
        "separator": separator,
    }


def main() -> None:
    ensure_session_state()
    st.markdown(app_css(), unsafe_allow_html=True)

    st.markdown(
        """
        <div class="hero-box">
            <h1>Sendeplan-Generator</h1>
            <p>Lädt deine Quelldateien hoch, erzeugt den Plan je Kunde und bietet Druck sowie HTML-Export mit A4-Layout an.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    upload_data = file_uploader_block()
    upload_map = {key: upload_data[key] for key in ["kunden", "sap", "transport", "kisoft", "kostenplan"]}

    if not all_required_uploads_present(upload_map):
        st.markdown(
            """
            <div class="info-box">
                <h3>Was jetzt benötigt wird</h3>
                <ul>
                    <li>Kundenliste mit fester Zuordnung der Spalten A, I, J, K, L, M, N</li>
                    <li>SAP-Datei mit den festen Spalten A, H, I, O, Y</li>
                    <li>Transportgruppen mit den Spalten A und C</li>
                    <li>Kisoft als CSV mit SAP Rahmentour, CSB Tournummer, Wochentag und Verladetor</li>
                    <li>Kostenstellenplan aus dem Blatt CSB Standard</li>
                </ul>
            </div>
            """,
            unsafe_allow_html=True,
        )
        return

    kunden_file = upload_data["kunden"]
    sap_file = upload_data["sap"]
    transport_file = upload_data["transport"]
    kisoft_file = upload_data["kisoft"]
    kostenplan_file = upload_data["kostenplan"]
    csv_separator = upload_data["separator"]

    kunden_sheet = UPLOAD_SPECS["kunden"]["sheet_default"]
    sap_sheet = UPLOAD_SPECS["sap"]["sheet_default"]
    transport_sheet = UPLOAD_SPECS["transport"]["sheet_default"]
    kostenplan_sheet = "CSB Standard"

    try:
        customers_df, plan_rows_df, kostenplan_df = prepare_dataframes(
            kunden_file.getvalue(),
            sap_file.getvalue(),
            transport_file.getvalue(),
            kisoft_file.getvalue(),
            kostenplan_file.getvalue(),
            csv_separator,
            kunden_sheet,
            sap_sheet,
            transport_sheet,
            kostenplan_sheet,
        )
    except Exception as exc:
        st.error(f"Die hochgeladenen Dateien konnten nicht verarbeitet werden: {exc}")
        return

    with st.sidebar:
        st.divider()
        st.header("Filter")
        st.text_input(
            "Suche nach SAP-Nummer oder Name",
            key="search_text",
            placeholder="Zum Beispiel 213109 oder Adler",
        )
        st.checkbox("Nur Kunden mit Planzeilen anzeigen", key="show_only_with_plan_rows")

        category_counts = {}
        for category in KATEGORIEN:
            category_counts[category] = len(
                filter_customers(
                    customers_df,
                    category,
                    st.session_state.get("search_text", ""),
                    st.session_state.show_only_with_plan_rows,
                )
            )

        col1, col2 = st.columns(2)
        with col1:
            st.button(f"Alle ({category_counts['Alle']})", use_container_width=True, on_click=set_category, args=("Alle",))
            st.button(f"Malchow ({category_counts['Malchow']})", use_container_width=True, on_click=set_category, args=("Malchow",))
            st.button(f"MK ({category_counts['MK']})", use_container_width=True, on_click=set_category, args=("MK",))
        with col2:
            st.button(f"NMS ({category_counts['NMS']})", use_container_width=True, on_click=set_category, args=("NMS",))
            st.button(f"Direkt ({category_counts['Direkt']})", use_container_width=True, on_click=set_category, args=("Direkt",))

        st.caption(f"Aktiver Filter: {st.session_state.category_filter}")

    filtered_customers = filter_customers(
        customers_df,
        st.session_state.category_filter,
        st.session_state.get("search_text", ""),
        st.session_state.show_only_with_plan_rows,
    )

    main_col, info_col = st.columns([5, 2], gap="large")

    with info_col:
        st.markdown(render_status_box("Kunden gesamt", str(len(customers_df))), unsafe_allow_html=True)
        st.markdown(render_status_box("Planzeilen gesamt", str(len(plan_rows_df))), unsafe_allow_html=True)
        st.markdown(render_status_box("Kunden im Filter", str(len(filtered_customers))), unsafe_allow_html=True)

        st.markdown(
            f"""
            <div class="info-box">
                <h3>Aktive Dateien</h3>
                <ul>
                    <li>Kundenliste: {html.escape(kunden_file.name)}</li>
                    <li>SAP: {html.escape(sap_file.name)}</li>
                    <li>Transportgruppen: {html.escape(transport_file.name)}</li>
                    <li>Kisoft: {html.escape(kisoft_file.name)}</li>
                    <li>Kostenstellenplan: {html.escape(kostenplan_file.name)}</li>
                </ul>
            </div>
            <div class="info-box">
                <h3>Zusätzliche Regeln</h3>
                <ul>
                    <li>Doppelte SAP-Zeilen werden je SAP-Nummer, Bestelltag und Liefertyp bereinigt.</li>
                    <li>Der Kostenstellenplan wird je CSB-Tournummer aus dem Blatt CSB Standard zugeordnet.</li>
                    <li>AVO, Werbemittel-Sonder, Werbemittel und Hamburger Jungs werden mit Zeit, Vorlauf und Bestelltag ausgegeben.</li>
                </ul>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with main_col:
        if filtered_customers.empty:
            st.markdown(
                """
                <div class="empty-box">
                    <h3>Keine Kunden im aktuellen Filter</h3>
                    <p>Bitte Suchbegriff, Kategorie oder die Einstellung für Kunden ohne Planzeilen anpassen.</p>
                </div>
                """,
                unsafe_allow_html=True,
            )
            return

        options = filtered_customers["SAP_Nr"].tolist()
        option_labels = build_option_labels(filtered_customers)

        if st.session_state.selected_sap not in options:
            st.session_state.selected_sap = options[0]

        selected_sap = st.selectbox(
            "Kunde auswählen",
            options=options,
            format_func=lambda sap: option_labels.get(sap, sap),
            index=options.index(st.session_state.selected_sap) if st.session_state.selected_sap in options else 0,
        )
        st.session_state.selected_sap = selected_sap

        selected_customer = filtered_customers[filtered_customers["SAP_Nr"] == selected_sap].iloc[0]
        customer_rows = plan_rows_df[plan_rows_df["SAP_Nr"] == selected_sap].copy()

        single_html = build_single_document_html(selected_customer, customer_rows, kostenplan_df)
        bulk_html = build_full_document_html(filtered_customers, plan_rows_df, kostenplan_df, include_separators=True)

        export_col1, export_col2 = st.columns(2)
        with export_col1:
            st.download_button(
                "Aktuellen Kunden als HTML herunterladen",
                data=single_html,
                file_name=f"sendeplan_{normalize_text(selected_customer['SAP_Nr'])}.html",
                mime="text/html",
                use_container_width=True,
            )
        with export_col2:
            st.download_button(
                "Gefilterten Gesamtplan als HTML herunterladen",
                data=bulk_html,
                file_name=f"sendeplan_{normalize_text(st.session_state.category_filter).lower() or 'alle'}.html",
                mime="text/html",
                use_container_width=True,
            )

        st.markdown(
            "<p class='print-note'>Druck öffnet ein separates HTML-Dokument mit A4-Layout. Der Download speichert dieselbe HTML-Datei lokal.</p>",
            unsafe_allow_html=True,
        )
        render_print_buttons(single_html, bulk_html)

        st.markdown(render_customer_plan(selected_customer, customer_rows, kostenplan_df), unsafe_allow_html=True)


if __name__ == "__main__":
    main()
