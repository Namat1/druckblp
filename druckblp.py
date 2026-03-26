from __future__ import annotations

import html
import io
import re
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

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

KISOFT_COLUMNS = ["SAP Rahmentour", "CSB Tournummer", "Wochentag", "Bereitstelldatum und -zeit", "Verladetor"]


def normalize_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def normalize_digits(value: object) -> str:
    return "".join(ch for ch in normalize_text(value) if ch.isdigit())


def excel_column_to_index(letter: str) -> int:
    result = 0
    for char in letter.upper():
        if not ("A" <= char <= "Z"):
            raise ValueError(f"Ungültiger Spaltenbuchstabe: {letter}")
        result = result * 26 + (ord(char) - ord("A") + 1)
    return result - 1


@st.cache_data(show_spinner=False)
def list_excel_sheets(file_bytes: bytes) -> List[str]:
    excel_file = pd.ExcelFile(io.BytesIO(file_bytes))
    return list(excel_file.sheet_names)


@st.cache_data(show_spinner=False)
def read_excel_raw(file_bytes: bytes, sheet_name: Optional[str]) -> pd.DataFrame:
    return pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet_name, header=None, dtype=str)


@st.cache_data(show_spinner=False)
def read_csv_raw(file_bytes: bytes, separator: str) -> pd.DataFrame:
    errors: List[str] = []
    for encoding in ["utf-8-sig", "utf-8", "latin1", "cp1252"]:
        try:
            return pd.read_csv(
                io.BytesIO(file_bytes),
                sep=separator,
                header=None,
                dtype=str,
                encoding=encoding,
                keep_default_na=False,
            )
        except Exception as exc:
            errors.append(f"{encoding}: {exc}")
    raise ValueError("CSV konnte nicht gelesen werden: " + " | ".join(errors))


def read_raw_table(file_bytes: bytes, filename: str, separator: str, sheet_name: Optional[str] = None) -> pd.DataFrame:
    suffix = Path(filename).suffix.lower()
    if suffix == ".csv":
        return read_csv_raw(file_bytes, separator)
    if suffix in {".xlsx", ".xls", ".xlsm"}:
        return read_excel_raw(file_bytes, sheet_name)
    raise ValueError(f"Nicht unterstütztes Dateiformat: {suffix}")


def cleanup_dataframe(df: pd.DataFrame, key_column: str) -> pd.DataFrame:
    result = df.copy()
    for column in result.columns:
        result[column] = result[column].map(normalize_text)
    result = result.replace("", pd.NA).dropna(how="all").fillna("")
    result = result[result[key_column].map(normalize_text) != ""].copy()
    return result.reset_index(drop=True)


def drop_header_like_first_row(df: pd.DataFrame, key_column: str) -> pd.DataFrame:
    if df.empty:
        return df

    first_row = df.iloc[0]
    first_key = normalize_text(first_row.get(key_column, "")).lower()
    row_values = " | ".join(normalize_text(value).lower() for value in first_row.tolist())
    header_tokens = [
        "sap-nr",
        "sap nr",
        "kundennummer",
        "fa.-ber.",
        "marktname",
        "kundenname",
        "bestellzeit",
        "liefertyp",
        "transport",
        "name",
        "strasse",
        "plz",
        "ort",
    ]

    if any(token in first_key or token in row_values for token in header_tokens):
        return df.iloc[1:].reset_index(drop=True)
    return df


def extract_columns_by_letters(raw_df: pd.DataFrame, mapping: Dict[str, str], label: str) -> pd.DataFrame:
    extracted: Dict[str, pd.Series] = {}
    for target_name, column_letter in mapping.items():
        column_index = excel_column_to_index(column_letter)
        if column_index >= raw_df.shape[1]:
            raise ValueError(
                f"{label}: Spalte {column_letter} fehlt. Gefunden wurden nur {raw_df.shape[1]} Spalten."
            )
        extracted[target_name] = raw_df.iloc[:, column_index]
    return pd.DataFrame(extracted)


@st.cache_data(show_spinner=False)
def load_structured_upload(
    file_bytes: bytes,
    filename: str,
    separator: str,
    dataset_key: str,
    sheet_name: Optional[str],
) -> pd.DataFrame:
    config = UPLOAD_CONFIG[dataset_key]
    raw_df = read_raw_table(file_bytes, filename, separator, sheet_name)
    structured_df = extract_columns_by_letters(raw_df, config["mapping"], config["label"])
    structured_df = cleanup_dataframe(structured_df, config["key"])
    structured_df = drop_header_like_first_row(structured_df, config["key"])
    return structured_df


@st.cache_data(show_spinner=False)
def load_kisoft_upload(file_bytes: bytes, filename: str, separator: str, sheet_name: Optional[str]) -> pd.DataFrame:
    raw_df = read_raw_table(file_bytes, filename, separator, sheet_name)
    if raw_df.shape[1] < 5:
        raise ValueError("Kisoft-Datei benötigt mindestens 5 Spalten.")

    df = raw_df.iloc[:, :5].copy()
    df.columns = KISOFT_COLUMNS
    df = cleanup_dataframe(df, "SAP Rahmentour")

    first_header = {normalize_text(v).lower() for v in df.head(1).iloc[0].tolist()} if not df.empty else set()
    if {"sap rahmentour", "csb tournummer"} & first_header:
        df = df.iloc[1:].reset_index(drop=True)

    for column in KISOFT_COLUMNS:
        df[column] = df[column].map(normalize_text)

    return df


def parse_range_text(range_text: str) -> List[Tuple[int, int]]:
    text = normalize_text(range_text)
    if not text:
        return []

    parts = [part.strip() for part in text.split("+")]
    ranges: List[Tuple[int, int]] = []
    base_prefix: Optional[int] = None

    for part in parts:
        numbers = re.findall(r"\d+", part)
        if not numbers:
            continue

        if len(numbers) >= 2 and "-" in part:
            start = int(numbers[0])
            end = int(numbers[1])
            ranges.append((start, end))
            if start >= 1000:
                base_prefix = (start // 1000) * 1000
        else:
            value = int(numbers[0])
            if base_prefix is not None and value < 100:
                value = base_prefix + value
            ranges.append((value, value))

    return ranges


@st.cache_data(show_spinner=False)
def load_kostenstellen_ranges(file_bytes: bytes, filename: str, separator: str, sheet_name: Optional[str]) -> pd.DataFrame:
    raw_df = read_raw_table(file_bytes, filename, separator, sheet_name)

    weekday_rows: List[Tuple[int, str]] = []
    for idx in range(len(raw_df)):
        day_name = normalize_text(raw_df.iloc[idx, 3] if raw_df.shape[1] > 3 else "")
        if day_name in WOCHENTAGE.values():
            weekday_rows.append((idx, day_name))

    rows: List[Dict[str, str]] = []
    for start_idx, day_name in weekday_rows:
        for row_idx in range(start_idx + 1, min(start_idx + 7, len(raw_df))):
            label = normalize_text(raw_df.iloc[row_idx, 0] if raw_df.shape[1] > 0 else "")
            range_text = normalize_text(raw_df.iloc[row_idx, 1] if raw_df.shape[1] > 1 else "")
            kostenstelle = normalize_text(raw_df.iloc[row_idx, 2] if raw_df.shape[1] > 2 else "")
            leiter = normalize_text(raw_df.iloc[row_idx, 3] if raw_df.shape[1] > 3 else "")
            if not label or not range_text:
                continue
            rows.append(
                {
                    "Wochentag": day_name,
                    "Gruppenlabel": label,
                    "Tourengruppe": range_text,
                    "Kostenstelle": kostenstelle,
                    "Leiter": leiter,
                }
            )

    df = pd.DataFrame(rows)
    if df.empty:
        raise ValueError("Aus dem Kostenstellenplan konnten keine Tourgruppen-Blöcke gelesen werden.")
    return df


def is_mk_pattern(csb_nr: str) -> bool:
    digits = normalize_digits(csb_nr)
    return len(digits) >= 4 and (digits.endswith("881") or digits.endswith("884"))


def classify_customer(rahmentour_raw: str, csb_nr: str) -> str:
    route = normalize_text(rahmentour_raw).upper()
    if "M" in route:
        return "Malchow"
    if "N" in route:
        return "NMS"
    if is_mk_pattern(csb_nr):
        return "MK"
    return "Direkt"


def day_name_from_number(value: object) -> str:
    digits = normalize_digits(value)
    if not digits:
        return ""
    return WOCHENTAGE.get(int(digits[0]), "")


def build_kisoft_key(rahmentour_raw: str) -> str:
    raw = normalize_text(rahmentour_raw)
    return f"00{raw[:8]}" if raw else ""


def value_in_range(value: str, range_text: str) -> bool:
    digits = normalize_digits(value)
    if not digits:
        return False
    number = int(digits)
    for start, end in parse_range_text(range_text):
        if start <= number <= end:
            return True
    return False


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
    separator: str,
    kunden_sheet: Optional[str],
    sap_sheet: Optional[str],
    transport_sheet: Optional[str],
    kisoft_sheet: Optional[str],
    kostenstellen_sheet: Optional[str],
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, int]]:
    df_kunden = load_structured_upload(kunden_bytes, kunden_name, separator, "kunden", kunden_sheet)
    df_sap = load_structured_upload(sap_bytes, sap_name, separator, "sap", sap_sheet)
    df_transport = load_structured_upload(transport_bytes, transport_name, separator, "transport", transport_sheet)
    df_kisoft = load_kisoft_upload(kisoft_bytes, kisoft_name, separator, kisoft_sheet)
    df_ranges = load_kostenstellen_ranges(kostenstellen_bytes, kostenstellen_name, separator, kostenstellen_sheet)

    for column in df_kunden.columns:
        df_kunden[column] = df_kunden[column].map(normalize_text)
    for column in df_sap.columns:
        df_sap[column] = df_sap[column].map(normalize_text)
    for column in df_transport.columns:
        df_transport[column] = df_transport[column].map(normalize_text)
    for column in df_kisoft.columns:
        df_kisoft[column] = df_kisoft[column].map(normalize_text)

    df_kunden["SAP_Nr"] = df_kunden["SAP_Nr"].map(normalize_digits)
    df_kunden["CSB_Nr"] = df_kunden["CSB_Nr"].map(normalize_digits)

    df_sap["SAP_Nr"] = df_sap["SAP_Nr"].map(normalize_digits)
    df_sap["Liefertyp_ID"] = df_sap["Liefertyp_ID"].map(normalize_digits)
    df_sap["Kisoft_Key"] = df_sap["Rahmentour_Raw"].map(build_kisoft_key)
    df_sap["Bestelltag_Name"] = df_sap["Bestelltag"].map(day_name_from_number)

    df_transport["Liefertyp_ID"] = df_transport["Liefertyp_ID"].map(normalize_digits)

    df_kisoft = df_kisoft.rename(columns={"SAP Rahmentour": "Kisoft_Key", "CSB Tournummer": "Kisoft_CSB_Tournummer"})

    df_sap = df_sap.merge(df_transport, on="Liefertyp_ID", how="left")
    df_sap = df_sap.merge(
        df_kisoft[["Kisoft_Key", "Kisoft_CSB_Tournummer", "Wochentag", "Verladetor"]].drop_duplicates(subset=["Kisoft_Key"]),
        on="Kisoft_Key",
        how="left",
    )

    kunden_basis = df_kunden.merge(
        df_sap[["SAP_Nr", "Rahmentour_Raw", "Kisoft_CSB_Tournummer"]].drop_duplicates(subset=["SAP_Nr"]),
        on="SAP_Nr",
        how="left",
    )
    kunden_basis["Kategorie"] = kunden_basis.apply(
        lambda row: classify_customer(row.get("Rahmentour_Raw", ""), row.get("CSB_Nr", "")),
        axis=1,
    )

    def lookup_group(row: pd.Series) -> pd.Series:
        candidates = [normalize_digits(row.get("Kisoft_CSB_Tournummer", "")), normalize_digits(row.get("CSB_Nr", ""))]

        for candidate in candidates:
            if not candidate:
                continue

            day_from_first_digit = WOCHENTAGE.get(int(candidate[0]), "") if candidate[0].isdigit() else ""
            matches = df_ranges[df_ranges["Tourengruppe"].map(lambda value: value_in_range(candidate, value))]

            if matches.empty:
                continue

            if day_from_first_digit:
                matches_day = matches[matches["Wochentag"] == day_from_first_digit]
                if not matches_day.empty:
                    match_row = matches_day.iloc[0]
                    return pd.Series(
                        {
                            "Tourengruppe": normalize_text(match_row["Tourengruppe"]),
                            "Leiter": normalize_text(match_row["Leiter"]),
                            "Gruppenlabel": normalize_text(match_row["Gruppenlabel"]),
                        }
                    )

            match_row = matches.iloc[0]
            return pd.Series(
                {
                    "Tourengruppe": normalize_text(match_row["Tourengruppe"]),
                    "Leiter": normalize_text(match_row["Leiter"]),
                    "Gruppenlabel": normalize_text(match_row["Gruppenlabel"]),
                }
            )

        return pd.Series({"Tourengruppe": "", "Leiter": "", "Gruppenlabel": ""})

    kunden_basis[["Tourengruppe", "Leiter", "Gruppenlabel"]] = kunden_basis.apply(lookup_group, axis=1)

    sap_counts = df_sap.groupby("SAP_Nr").size().rename("Planzeilen_Anzahl")
    kunden_basis = kunden_basis.merge(sap_counts, on="SAP_Nr", how="left")
    kunden_basis["Planzeilen_Anzahl"] = kunden_basis["Planzeilen_Anzahl"].fillna(0).astype(int)
    kunden_basis["Hat_Planzeilen"] = kunden_basis["Planzeilen_Anzahl"] > 0

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
                "Gruppenlabel",
            ]
        ],
        on="SAP_Nr",
        how="left",
    )

    def infer_liefertag(row: pd.Series) -> str:
        tournummer = normalize_digits(row.get("Kisoft_CSB_Tournummer", ""))
        if tournummer and tournummer[0].isdigit():
            day = WOCHENTAGE.get(int(tournummer[0]))
            if day:
                return day
        if normalize_text(row.get("Wochentag", "")) in WOCHENTAGE.values():
            return normalize_text(row.get("Wochentag", ""))
        return normalize_text(row.get("Bestelltag_Name", ""))

    plan_rows["Liefertag"] = plan_rows.apply(infer_liefertag, axis=1)
    plan_rows["Sortiment"] = plan_rows["Liefertyp_Name"].fillna("")
    plan_rows["Bestelltag_Klartext"] = plan_rows["Bestelltag_Name"].fillna("")
    plan_rows["Bestellzeitende"] = plan_rows["Bestellzeitende"].fillna("")

    plan_rows = plan_rows.drop_duplicates(
        subset=["SAP_Nr", "Liefertag", "Sortiment", "Bestelltag_Klartext", "Bestellzeitende", "Rahmentour_Raw"]
    ).copy()
    plan_rows["SortKey_Liefertag"] = plan_rows["Liefertag"].map({v: k for k, v in WOCHENTAGE.items()}).fillna(99)
    plan_rows["SortKey_Bestelltag"] = plan_rows["Bestelltag"].map(lambda value: int(normalize_digits(value)) if normalize_digits(value) else 99)
    plan_rows = plan_rows.sort_values(["SAP_Nr", "SortKey_Liefertag", "SortKey_Bestelltag", "Sortiment", "Bestellzeitende"])

    counts = {cat: int((kunden_basis["Kategorie"] == cat).sum()) for cat in KATEGORIEN if cat != "Alle"}
    counts["Alle"] = int(len(kunden_basis))
    counts["Mit Plan"] = int(kunden_basis["Hat_Planzeilen"].sum())
    counts["Ohne Plan"] = int((~kunden_basis["Hat_Planzeilen"]).sum())

    return kunden_basis, plan_rows, counts


def filter_customers(df_customers: pd.DataFrame, category: str, search_text: str, only_with_plan: bool) -> pd.DataFrame:
    result = df_customers.copy()
    if category != "Alle":
        result = result[result["Kategorie"] == category]
    if only_with_plan:
        result = result[result["Hat_Planzeilen"]]

    search = normalize_text(search_text).lower()
    if search:
        result = result[
            result["SAP_Nr"].str.lower().str.contains(search, na=False)
            | result["Name"].str.lower().str.contains(search, na=False)
        ]

    return result.sort_values(["Hat_Planzeilen", "Name", "SAP_Nr"], ascending=[False, True, True]).reset_index(drop=True)


def format_customer_label(row: pd.Series) -> str:
    status = "✅" if row.get("Hat_Planzeilen", False) else "⚠️"
    return f"{status} {row.get('SAP_Nr', '')} | {row.get('Name', '')}"


def customer_meta(customer: pd.Series, plan_rows: pd.DataFrame) -> Dict[str, str]:
    route = ""
    gate = ""
    csb_tour = ""
    delivery_day = ""

    if not plan_rows.empty:
        first = plan_rows.iloc[0]
        route = normalize_text(first.get("Rahmentour_Raw", ""))
        gate = normalize_text(first.get("Verladetor", ""))
        csb_tour = normalize_text(first.get("Kisoft_CSB_Tournummer", ""))
        delivery_day = normalize_text(first.get("Liefertag", ""))

    if not route:
        route = normalize_text(customer.get("Rahmentour_Raw", ""))

    return {
        "SAP-Nummer": normalize_text(customer.get("SAP_Nr", "")),
        "CSB-Nummer": normalize_text(customer.get("CSB_Nr", "")),
        "Kisoft Tour": csb_tour,
        "Kunde": normalize_text(customer.get("Name", "")),
        "Fachberater": normalize_text(customer.get("Fachberater", "")),
        "Adresse": " ".join(
            part for part in [normalize_text(customer.get("Strasse", "")), normalize_text(customer.get("PLZ", "")), normalize_text(customer.get("Ort", ""))] if part
        ),
        "Kategorie": normalize_text(customer.get("Kategorie", "")),
        "Tourengruppe": normalize_text(customer.get("Tourengruppe", "")),
        "Gruppenlabel": normalize_text(customer.get("Gruppenlabel", "")),
        "Leiter": normalize_text(customer.get("Leiter", "")),
        "Rahmentour": route,
        "Verladetor": gate,
        "Liefertag": delivery_day,
        "Planzeilen": str(int(customer.get("Planzeilen_Anzahl", 0) or 0)),
    }


def plan_table_html(plan_rows: pd.DataFrame) -> str:
    if plan_rows.empty:
        return '<div class="notice warning">Für diesen Kunden gibt es in SAP.xlsx keine Planzeilen.</div>'

    table_rows: List[str] = []
    for _, row in plan_rows.iterrows():
        table_rows.append(
            "<tr>"
            f"<td>{html.escape(normalize_text(row.get('Liefertag', '')))}</td>"
            f"<td>{html.escape(normalize_text(row.get('Sortiment', '')))}</td>"
            f"<td>{html.escape(normalize_text(row.get('Bestelltag_Klartext', '')))}</td>"
            f"<td>{html.escape(normalize_text(row.get('Bestellzeitende', '')))}</td>"
            "</tr>"
        )

    return (
        '<table class="plan-table">'
        "<thead><tr><th>Liefertag</th><th>Sortiment</th><th>Bestelltag</th><th>Bestellzeitende</th></tr></thead>"
        f"<tbody>{''.join(table_rows)}</tbody>"
        "</table>"
    )


def css_block() -> str:
    return """
    <style>
        @page { size: A4 portrait; margin: 10mm; }
        * { box-sizing: border-box; }
        body {
            margin: 0;
            font-family: Arial, Helvetica, sans-serif;
            color: #20252b;
            background: #e9eef4;
        }
        .paper {
            width: 100%;
            max-width: 210mm;
            min-height: 297mm;
            margin: 16px auto;
            background: white;
            box-shadow: 0 10px 28px rgba(0, 0, 0, 0.12);
            padding: 12mm;
            page-break-after: always;
        }
        .topbar {
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            gap: 10px;
            margin-bottom: 10px;
        }
        .title {
            font-size: 20pt;
            font-weight: 700;
            color: #173a63;
            margin: 0 0 4px 0;
        }
        .subtitle {
            font-size: 9pt;
            color: #5a6774;
            margin: 0;
        }
        .meta-grid {
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap: 8px;
            margin: 10px 0 12px 0;
        }
        .meta-card {
            border: 1px solid #d9e1ea;
            border-radius: 8px;
            padding: 8px 10px;
            background: #f7f9fc;
            min-height: 52px;
        }
        .meta-label {
            display: block;
            font-size: 8pt;
            color: #67727d;
            margin-bottom: 4px;
            text-transform: uppercase;
            letter-spacing: 0.03em;
        }
        .meta-value {
            display: block;
            font-size: 10pt;
            font-weight: 600;
            color: #1d252d;
            word-break: break-word;
        }
        .plan-table {
            width: 100%;
            border-collapse: collapse;
            border: 1.5px solid #aaa;
            font-size: 9pt;
        }
        .plan-table th,
        .plan-table td {
            border: 1px solid #aaa;
            padding: 6px 7px;
            vertical-align: top;
            text-align: left;
        }
        .plan-table th {
            background: #eef3f8;
            font-weight: 700;
        }
        .notice {
            border-radius: 8px;
            padding: 10px 12px;
            margin: 12px 0;
            font-size: 10pt;
        }
        .notice.warning {
            background: #fff5d9;
            border: 1px solid #ead38a;
        }
        .section-title {
            font-size: 12pt;
            font-weight: 700;
            margin: 14px 0 8px 0;
            color: #173a63;
        }
        .deck {
            display: flex;
            flex-direction: column;
            justify-content: center;
            align-items: center;
            text-align: center;
        }
        .deck h1 {
            font-size: 28pt;
            margin: 0 0 12px 0;
            color: #173a63;
        }
        .deck p {
            margin: 4px 0;
            font-size: 12pt;
            color: #3f4a56;
        }
        .print-actions {
            margin: 0 auto 10px auto;
            max-width: 210mm;
            display: flex;
            justify-content: flex-end;
            gap: 8px;
        }
        .print-actions button {
            background: #173a63;
            color: white;
            border: none;
            border-radius: 8px;
            padding: 8px 12px;
            cursor: pointer;
            font-size: 10pt;
        }
        @media print {
            body { background: white; }
            .paper {
                margin: 0;
                box-shadow: none;
                border-radius: 0;
            }
            .print-actions { display: none; }
        }
    </style>
    """


def render_customer_html(customer: pd.Series, plan_rows: pd.DataFrame) -> str:
    meta = customer_meta(customer, plan_rows)
    cards = "".join(
        (
            '<div class="meta-card">'
            f'<span class="meta-label">{html.escape(label)}</span>'
            f'<span class="meta-value">{html.escape(value)}</span>'
            "</div>"
        )
        for label, value in meta.items()
    )

    table_html = plan_table_html(plan_rows)

    return f"""
    <div class="paper">
        <div class="topbar">
            <div>
                <div class="title">Sendeplan</div>
                <p class="subtitle">Kunde {html.escape(meta['SAP-Nummer'])} · {html.escape(meta['Kunde'])}</p>
            </div>
            <div style="font-size:9pt;color:#66717d;">Generiert aus Upload-Dateien</div>
        </div>
        <div class="meta-grid">{cards}</div>
        <div class="section-title">Planübersicht</div>
        {table_html}
    </div>
    """


def build_single_document(customer: pd.Series, plan_rows: pd.DataFrame) -> str:
    body = render_customer_html(customer, plan_rows)
    return f"""
    <!doctype html>
    <html lang="de">
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>Sendeplan {html.escape(normalize_text(customer.get('SAP_Nr', '')))}</title>
        {css_block()}
    </head>
    <body>
        <div class="print-actions"><button onclick="window.print()">Drucken</button></div>
        {body}
    </body>
    </html>
    """


def build_bulk_document(filtered_customers: pd.DataFrame, plan_rows: pd.DataFrame, title: str) -> str:
    pages: List[str] = [
        f"""
        <div class="paper deck">
            <h1>{html.escape(title)}</h1>
            <p>Kunden im Export: {len(filtered_customers)}</p>
            <p>Erstellt aus Kundenliste, SAP, Transportgruppen, Kisoft und Kostenstellenplan.</p>
        </div>
        """
    ]

    for _, customer in filtered_customers.iterrows():
        rows = plan_rows[plan_rows["SAP_Nr"] == customer["SAP_Nr"]].copy()
        pages.append(render_customer_html(customer, rows))

    body = "".join(pages)
    return f"""
    <!doctype html>
    <html lang="de">
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>{html.escape(title)}</title>
        {css_block()}
    </head>
    <body>
        <div class="print-actions"><button onclick="window.print()">Alle drucken</button></div>
        {body}
    </body>
    </html>
    """


def app_css() -> str:
    return """
    <style>
        .stApp {
            background: linear-gradient(180deg, #eef2f7 0%, #f7f9fb 100%);
        }
        section[data-testid="stSidebar"] {
            background: #f7f9fc;
            border-right: 1px solid #d9e1ea;
        }
        .app-hero {
            background: white;
            border: 1px solid #d9e1ea;
            border-radius: 14px;
            padding: 18px 20px;
            margin-bottom: 14px;
        }
        .app-hero h1 {
            margin: 0 0 6px 0;
            font-size: 2rem;
            color: #173a63;
        }
        .app-hero p {
            margin: 0;
            color: #55606c;
        }
        .status-box {
            background: white;
            border: 1px solid #d9e1ea;
            border-radius: 14px;
            padding: 14px 16px;
            margin-bottom: 14px;
        }
        .preview-wrap {
            background: transparent;
            padding-top: 4px;
        }
        .empty-state {
            background: white;
            border: 1px dashed #b8c5d3;
            border-radius: 14px;
            padding: 24px;
            text-align: center;
            color: #53606d;
        }
    </style>
    """


def upload_ready(upload_map: Dict[str, Optional[object]]) -> bool:
    return all(upload_map.values())


def infer_default_sheet(file_name: str, available_sheets: Iterable[str], desired_name: str) -> Optional[str]:
    names = list(available_sheets)
    if desired_name in names:
        return desired_name
    return names[0] if names else None


def main() -> None:
    st.markdown(app_css(), unsafe_allow_html=True)
    st.markdown(
        """
        <div class="app-hero">
            <h1>📦 Sendeplan-Generator</h1>
            <p>Dateien hochladen, Daten prüfen und den fertigen Sendeplan als HTML herunterladen.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    with st.sidebar:
        st.header("Quelldateien")
        separator = st.text_input("CSV-Trennzeichen", value=";", max_chars=1)

        kunden_file = st.file_uploader("Kundenliste", type=["xlsx", "xls", "xlsm", "csv"])
        sap_file = st.file_uploader("SAP-Datei", type=["xlsx", "xls", "xlsm", "csv"])
        transport_file = st.file_uploader("Transportgruppen", type=["xlsx", "xls", "xlsm", "csv"])
        kisoft_file = st.file_uploader("Kisoft-Datei", type=["csv", "xlsx", "xls", "xlsm"])
        kostenstellen_file = st.file_uploader("Kostenstellenplan", type=["xlsx", "xls", "xlsm", "csv"])

        upload_map = {
            "kunden": kunden_file,
            "sap": sap_file,
            "transport": transport_file,
            "kisoft": kisoft_file,
            "kostenstellen": kostenstellen_file,
        }

        if not upload_ready(upload_map):
            st.info("Bitte alle fünf Dateien hochladen. Danach werden Daten, Download und Druck freigeschaltet.")

        sheet_selection: Dict[str, Optional[str]] = {key: None for key in upload_map}
        with st.expander("Blattauswahl", expanded=False):
            for key, uploaded_file in upload_map.items():
                if not uploaded_file or Path(uploaded_file.name).suffix.lower() == ".csv":
                    continue
                file_bytes = uploaded_file.getvalue()
                sheet_names = list_excel_sheets(file_bytes)
                if key in UPLOAD_CONFIG:
                    desired = UPLOAD_CONFIG[key]["sheet_default"]
                elif key == "kostenstellen":
                    desired = "CSB Standard"
                else:
                    desired = sheet_names[0]
                default_sheet = infer_default_sheet(uploaded_file.name, sheet_names, desired)
                default_index = sheet_names.index(default_sheet) if default_sheet in sheet_names else 0
                sheet_selection[key] = st.selectbox(
                    f"{UPLOAD_CONFIG[key]['label'] if key in UPLOAD_CONFIG else 'Kostenstellenplan'} – Tabellenblatt",
                    options=sheet_names,
                    index=default_index,
                    key=f"sheet_{key}",
                )

    if not upload_ready(upload_map):
        st.markdown(
            """
            <div class="empty-state">
                <h3>Warte auf Uploads</h3>
                <p>Lade links alle Quelldateien hoch. Danach siehst du sofort, wie viele Kunden Planzeilen haben.</p>
            </div>
            """,
            unsafe_allow_html=True,
        )
        return

    try:
        kunden_basis, plan_rows, counts = prepare_dataframes(
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
            separator,
            sheet_selection.get("kunden"),
            sheet_selection.get("sap"),
            sheet_selection.get("transport"),
            sheet_selection.get("kisoft"),
            sheet_selection.get("kostenstellen"),
        )
    except Exception as exc:
        st.error(f"Fehler beim Einlesen der Dateien: {exc}")
        return

    stat_col1, stat_col2, stat_col3, stat_col4 = st.columns(4)
    stat_col1.metric("Kunden gesamt", counts.get("Alle", 0))
    stat_col2.metric("Mit Planzeilen", counts.get("Mit Plan", 0))
    stat_col3.metric("Ohne Planzeilen", counts.get("Ohne Plan", 0))
    stat_col4.metric("Planzeilen", len(plan_rows))

    with st.sidebar:
        st.header("Filter")
        category = st.radio(
            "Kategorie",
            options=KATEGORIEN,
            index=0,
            format_func=lambda value: f"{value} ({counts.get(value, 0)})" if value in counts else value,
        )
        search_text = st.text_input("Suche nach SAP oder Name")
        only_with_plan = st.checkbox("Nur Kunden mit Planzeilen", value=True)

    filtered_customers = filter_customers(kunden_basis, category, search_text, only_with_plan)

    if filtered_customers.empty:
        st.markdown(
            """
            <div class="empty-state">
                <h3>Keine Kunden im aktuellen Filter</h3>
                <p>Bitte Filter oder Suchbegriff anpassen.</p>
            </div>
            """,
            unsafe_allow_html=True,
        )
        return

    st.markdown(
        f"""
        <div class="status-box">
            <strong>Aktueller Filter:</strong> {html.escape(category)} · {len(filtered_customers)} Kunden
        </div>
        """,
        unsafe_allow_html=True,
    )

    options = filtered_customers.index.tolist()
    selected_index = st.selectbox(
        "Kunde auswählen",
        options=options,
        format_func=lambda idx: format_customer_label(filtered_customers.loc[idx]),
    )

    selected_customer = filtered_customers.loc[selected_index]
    selected_plan_rows = plan_rows[plan_rows["SAP_Nr"] == selected_customer["SAP_Nr"]].copy()

    single_html = build_single_document(selected_customer, selected_plan_rows)
    bulk_html = build_bulk_document(filtered_customers, plan_rows, f"Sendeplan {category}")

    btn_col1, btn_col2 = st.columns(2)
    btn_col1.download_button(
        "Aktuellen Kunden als HTML herunterladen",
        data=single_html,
        file_name=f"sendeplan_{normalize_text(selected_customer['SAP_Nr'])}.html",
        mime="text/html",
        use_container_width=True,
    )
    btn_col2.download_button(
        "Gefilterten Gesamtplan als HTML herunterladen",
        data=bulk_html,
        file_name=f"sendeplan_{category.lower()}_{len(filtered_customers)}.html",
        mime="text/html",
        use_container_width=True,
    )

    if not selected_customer.get("Hat_Planzeilen", False):
        st.warning(
            f"Für SAP {selected_customer['SAP_Nr']} gibt es in SAP.xlsx keine Planzeilen. "
            "Die Kundendaten sind vorhanden, aber der Plan bleibt deshalb leer."
        )

    preview_html = f"<div class='preview-wrap'>{render_customer_html(selected_customer, selected_plan_rows)}</div>"
    components.html(preview_html, height=1200, scrolling=True)

    with st.expander("Datenvorschau", expanded=False):
        st.subheader("Ausgewählter Kunde")
        st.dataframe(pd.DataFrame([customer_meta(selected_customer, selected_plan_rows)]), use_container_width=True)
        st.subheader("Planzeilen")
        st.dataframe(
            selected_plan_rows[["Liefertag", "Sortiment", "Bestelltag_Klartext", "Bestellzeitende", "Rahmentour_Raw", "Verladetor"]],
            use_container_width=True,
        )


if __name__ == "__main__":
    main()
