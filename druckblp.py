import base64
import hashlib
import html
import io
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import openpyxl
import pandas as pd
import streamlit as st


st.set_page_config(
    page_title="Sendeplan-Generator",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="collapsed",
)


WOCHENTAGE = {
    1: "Montag",
    2: "Dienstag",
    3: "Mittwoch",
    4: "Donnerstag",
    5: "Freitag",
    6: "Samstag",
}

KATEGORIEN = ["Alle", "MK", "Malchow", "NMS", "SuL", "Direkt"]

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
        "help": "Verwendet feste Excel-Spalten: A, G, H, I, O, Y",
        "mapping": {
            "SAP_Nr": "A",
            "Liefertag_Raw": "G",
            "Bestelltag": "H",
            "Bestellzeitende": "I",
            "Liefertyp_ID": "O",
            "Rahmentour_Raw": "Y",
        },
        "required": ["SAP_Nr", "Liefertag_Raw", "Bestelltag", "Bestellzeitende", "Liefertyp_ID", "Rahmentour_Raw"],
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

KISOFT_REQUIRED_COLUMNS = ["SAP Rahmentour", "CSB Tournummer", "Wochentag", "Verladetor"]
KOSTENSTELLEN_REQUIRED_COLUMNS = ["sap_von", "sap_bis", "tourengruppe", "kostenstelle", "leiter"]


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


_SuL_TOUREN = {"1058","2058","3058","4058","5058","6030",
                "14444","24444","34444","44444","54444"}


def classify_by_csb_tour(csb_tour: str) -> str:
    """Klassifiziert anhand der CSB-Tournummer (aus Kisoft).

    Priorität: SuL > MK (X88X) > Malchow (X777X) > NMS (X222X) > Direkt
    """
    csb = normalize_digits(csb_tour)
    if csb in _SuL_TOUREN:
        return "SuL"
    if "88" in csb:
        return "MK"
    if "777" in csb:
        return "Malchow"
    if "222" in csb:
        return "NMS"
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
            except Exception as exc:
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
            "kundennummer",
            "kundennummer / markt",
            "kunden-nr",
            "kunden nr",
        }
        # Auch Teilstring-Match: wenn der erste Schlüssel mit einem Token beginnt
        is_header = first_key in header_like_tokens or any(
            first_key.startswith(t) for t in header_like_tokens if len(t) > 3
        )
        if is_header:
            result = result.iloc[1:].copy()

    return result.reset_index(drop=True)


def load_structured_upload(file_bytes: bytes, filename: str, csv_separator: str, dataset_key: str) -> pd.DataFrame:
    config = UPLOAD_CONFIG[dataset_key]
    raw_df = read_upload_to_raw_dataframe(file_bytes, filename, csv_separator)
    structured_df = extract_columns_by_letter(raw_df, config["mapping"], config["label"])
    structured_df = cleanup_dataframe(structured_df, config["key"])
    validate_required_columns(structured_df, config["required"], config["label"])
    return structured_df


def load_kisoft_upload(file_bytes: bytes, filename: str, csv_separator: str) -> pd.DataFrame:
    """Liest die Kisoft-Datei.

    Unterstuetzt zwei Formate:
    - Mit Kopfzeile: Spalten werden per Name gefunden
      (SAP Rahmentour, CSB Tournummer, Wochentag, Verladetor)
    - Ohne Kopfzeile: feste Positionen 0, 1, 2, 4
    """
    raw_df = read_upload_to_raw_dataframe(file_bytes, filename, csv_separator)

    if raw_df.shape[1] < 3:
        raise ValueError("Kisoft-Datei muss mindestens 3 Spalten enthalten.")

    # Pruefen ob erste Zeile eine Kopfzeile ist
    first_row = {normalize_text(v).lower() for v in raw_df.iloc[0].tolist()}
    has_header = bool(first_row & {"sap rahmentour", "csb tournummer"})

    if has_header:
        # Mit Kopfzeile: per Name lesen
        raw_df.columns = [normalize_text(c) for c in raw_df.iloc[0]]
        raw_df = raw_df.iloc[1:].reset_index(drop=True)
        col_map = {c.lower(): c for c in raw_df.columns}
        def get_col(name):
            return raw_df[col_map[name.lower()]] if name.lower() in col_map else pd.Series([""] * len(raw_df))
        df = pd.DataFrame({
            "SAP Rahmentour": get_col("SAP Rahmentour"),
            "CSB Tournummer": get_col("CSB Tournummer"),
            "Wochentag":      get_col("Wochentag"),
            "Verladetor":     get_col("Verladetor"),
        })
    else:
        # Ohne Kopfzeile: feste Positionen
        df = pd.DataFrame({
            "SAP Rahmentour": raw_df.iloc[:, 0],
            "CSB Tournummer": raw_df.iloc[:, 1],
            "Wochentag":      raw_df.iloc[:, 2] if raw_df.shape[1] > 2 else "",
            "Verladetor":     raw_df.iloc[:, 4] if raw_df.shape[1] > 4 else "",
        })

    df = df.fillna("").apply(lambda col: col.map(normalize_text))
    df = df[df["SAP Rahmentour"] != ""].reset_index(drop=True)

    validate_required_columns(df, KISOFT_REQUIRED_COLUMNS, "Kisoft-Datei")
    return df


def _parse_sap_range_col(value) -> tuple:
    """Parst den SAP-Bereich aus Spalte B des Kostenstellenplans.

    Formate: '12221-14444', '1881 - 1886', '1001-1046 + 58', 5883 (Einzelwert)
    Toleriert None, float NaN und den String 'nan' (entsteht bei dtype=str).
    """
    if value is None:
        return ("", "")
    if isinstance(value, float):
        if pd.isna(value):
            return ("", "")
        # numerischer Einzelwert (z.B. 5883.0)
        text = str(int(value))
    else:
        text = str(value).strip()
    # leere oder NaN-artige Strings
    if not text or text.lower() in ("nan", "none", ""):
        return ("", "")
    text = re.sub(r'\s*\+.*$', '', text).strip()
    parts = re.split(r'\s*-\s*', text, maxsplit=1)
    if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
        return (parts[0].strip(), parts[1].strip())
    if text.isdigit():
        return (text, text)
    return ("", "")


def load_kostenstellen_upload(file_bytes: bytes, filename: str, csv_separator: str) -> pd.DataFrame:
    """Liest den Kostenstellenplan.

    Feste Spaltenreihenfolge:
      A = Tourengruppe  (z.B. 'HP-NMS/Zar', 'Direkt Früh')
      B = SAP-Bereich   (z.B. '12221-14444', '1001-1046 + 58', 5883)
      C = Kostenstelle  (z.B. 10, 41)
      D = Leiter        (z.B. 13, 43)

    Lookup erfolgt später über die CSB-Tournummer (z.B. 4007 liegt in 4001-4058).
    """
    suffix = Path(filename).suffix.lower()

    if suffix == ".csv":
        # CSV: nur ein Blatt, direkt lesen
        raw_df = read_upload_to_raw_dataframe(file_bytes, filename, csv_separator)
    else:
        # Excel: richtiges Sheet suchen (enthaelt numerische SAP-Bereiche in Spalte B)
        _wb = openpyxl.load_workbook(io.BytesIO(file_bytes), read_only=True)
        _chosen = None
        for _sname in _wb.sheetnames:
            _ws = _wb[_sname]
            for _row in _ws.iter_rows(max_row=20, values_only=True):
                _b = str(_row[1]).strip() if len(_row) > 1 and _row[1] is not None else ""
                if re.search(r'\d{4,}', _b):  # SAP-Bereich hat mind. 4 Ziffern
                    _chosen = _sname
                    break
            if _chosen:
                break
        if _chosen is None:
            _chosen = _wb.sheetnames[0]
        raw_df = pd.read_excel(io.BytesIO(file_bytes), sheet_name=_chosen, header=None, dtype=str)

    if raw_df.shape[1] < 4:
        raise ValueError("Kostenstellen-Datei benoetigt mindestens 4 Spalten (A-D).")

    records = []
    skip_tokens = ("lieferanten", "normale touren", "tourengruppen", "tourengruppe")
    for _, row in raw_df.iterrows():
        tourengruppe = normalize_text(row.iloc[0])
        sap_bereich  = row.iloc[1]
        kostenstelle = normalize_text(row.iloc[2])
        leiter       = normalize_text(row.iloc[3])

        if not tourengruppe:
            continue
        if any(tourengruppe.lower().startswith(t) for t in skip_tokens):
            continue

        sap_von, sap_bis = _parse_sap_range_col(sap_bereich)
        if not sap_von:
            continue

        records.append({
            "tourengruppe": tourengruppe,
            "sap_von":      sap_von,
            "sap_bis":      sap_bis,
            "kostenstelle": kostenstelle,
            "leiter":       leiter,
        })

    if not records:
        raise ValueError(
            "Kostenstellenplan: Keine Datenzeilen gefunden. "
            "Spaltenreihenfolge pruefen: A=Tourengruppe, B=SAP-Bereich, C=Kostenstelle, D=Leiter."
        )

    df = pd.DataFrame(records)
    validate_required_columns(df, KOSTENSTELLEN_REQUIRED_COLUMNS, "Kostenstellen-Datei")
    return df


# ============================================================
# ZUSATZ-SORTIMENTE AUS KOSTENSTELLENPLAN (AVO, WERBEMITTEL …)
# ============================================================

# Liefertyp-Gruppen: (Spaltenindex T.Zeit, Bestellzeitende-Fallback, Anzeigename)
_KST_ZUSATZ_GRUPPEN = [
    (7,  "09:00", "AVO-Gewürze"),
    (10, "09:00", "Werbemittel-Sonder"),
    (13, "09:00", "Werbemittel"),
    (16, "09:00", "Hamburger Jungs"),
]

_TAG_ABK = {
    "mo": "Montag", "die": "Dienstag", "mitt": "Mittwoch", "mi": "Mittwoch",
    "don": "Donnerstag", "do": "Donnerstag", "fr": "Freitag", "sa": "Samstag", "so": "Sonntag",
}

def _parse_kst_time(val) -> str:
    """Wandelt z.B. 915 -> '09:15', 1045 -> '10:45', 2045 -> '20:45'."""
    if val is None:
        return ""
    try:
        n = int(float(str(val).strip()))
    except (ValueError, TypeError):
        return ""
    if n <= 0:
        return ""
    s = f"{n:04d}"  # pad to 4 digits
    return f"{s[:2]}:{s[2:]}"


def _parse_kst_tag(val) -> str:
    """Wandelt 'Don' -> 'Donnerstag', 'Fr' -> 'Freitag' etc."""
    if val is None:
        return ""
    key = str(val).strip().lower()
    return _TAG_ABK.get(key, str(val).strip())


def extract_zusatz_schedule(file_bytes: bytes, filename: str) -> pd.DataFrame:
    """Extrahiert den Bestellplan fuer Zusatz-Sortimente (AVO, Werbemittel, …)
    aus dem Kostenstellenplan CSB Standard.

    Ergebnis-DataFrame: tourengruppe | liefertag | sortiment | bestelltag | bestellzeitende
    """
    wb = openpyxl.load_workbook(io.BytesIO(file_bytes), read_only=True)
    if "CSB Standard" not in wb.sheetnames:
        return pd.DataFrame(columns=["tourengruppe","liefertag","sortiment","bestelltag","bestellzeitende"])

    ws = wb["CSB Standard"]
    all_rows = list(ws.iter_rows(values_only=True))

    skip_tokens = ("lieferanten", "normale touren", "tourengruppen", "tourengruppe")
    current_day = ""
    records = []

    for row in all_rows:
        a = normalize_text(row[0]) if row[0] is not None else ""
        d = row[3] if len(row) > 3 else None

        # Kopfzeile erkennnen
        if a.lower() == "tourengruppen":
            current_day = normalize_text(d).capitalize() if d else ""
            continue

        # Zeilen ohne Tourengruppe oder ohne aktuellen Tag überspringen
        if not a or not current_day:
            continue
        if any(a.lower().startswith(t) for t in skip_tokens):
            continue

        # Brauchen SAP-Bereich (B) zur Validierung dass es eine Datenzeile ist
        b = row[1] if len(row) > 1 else None
        if b is None:
            continue
        b_str = str(b).strip()
        if not re.search(r'\d', b_str):
            continue

        # Für jede Zusatz-Gruppe
        for col_start, zeit_fallback, sortiment_name in _KST_ZUSATZ_GRUPPEN:
            if len(row) <= col_start + 2:
                continue
            zeit_val  = row[col_start]       # Abfahrtszeit
            tag_val   = row[col_start + 2]   # Bestelltag-Kuerzel

            if zeit_val is None and tag_val is None:
                continue  # kein Eintrag fuer diese Gruppe

            bestellzeitende = _parse_kst_time(zeit_val) or zeit_fallback
            bestelltag      = _parse_kst_tag(tag_val)

            if not bestelltag:
                continue

            records.append({
                "tourengruppe":   a,
                "liefertag":      current_day,
                "sortiment":      sortiment_name,
                "bestelltag":     bestelltag,
                "bestellzeitende": bestellzeitende,
            })

    return pd.DataFrame(records) if records else pd.DataFrame(
        columns=["tourengruppe","liefertag","sortiment","bestelltag","bestellzeitende"]
    )


def build_zusatz_plan_rows(plan_rows: pd.DataFrame, zusatz_schedule: pd.DataFrame) -> pd.DataFrame:
    """Generiert synthetische Planzeilen fuer AVO, Werbemittel etc.

    Fuer jede einzigartige (SAP_Nr, Liefertag) Kombination in plan_rows wird geprueft,
    ob es passende Eintraege in zusatz_schedule gibt (via Tourengruppe x Liefertag).
    Falls ja, wird eine neue Zeile erzeugt und angehaengt.
    """
    if zusatz_schedule.empty or plan_rows.empty:
        return plan_rows

    # Basis-Info pro (SAP_Nr, Liefertag): nimm erste Zeile
    basis_cols = ["SAP_Nr", "Liefertag", "Tourengruppe", "Kostenstelle", "Leiter",
                  "CSB Tournummer", "Verladetor", "Rahmentour_Raw", "SAP Rahmentour",
                  "Bestelltag", "SortKey_Bestelltag",
                  "CSB_Nr", "Name", "Strasse", "PLZ", "Ort", "Fachberater", "Kategorie",
                  "Kisoft_Key", "Liefertyp_ID", "Liefertyp_Name"]
    avail_cols = [c for c in basis_cols if c in plan_rows.columns]

    basis = (
        plan_rows[avail_cols]
        .drop_duplicates(subset=["SAP_Nr", "Liefertag"])
        .copy()
    )

    # Normalize für Merge
    sched = zusatz_schedule.copy()
    sched["_tg_norm"] = sched["tourengruppe"].str.strip().str.lower()
    sched["_lt_norm"] = sched["liefertag"].str.strip().str.lower()
    basis["_tg_norm"] = basis["Tourengruppe"].str.strip().str.lower()
    basis["_lt_norm"] = basis["Liefertag"].str.strip().str.lower()

    # Leere Tourengruppen / Liefertage ausfiltern
    basis = basis[(basis["_tg_norm"] != "") & (basis["_lt_norm"] != "")]

    if basis.empty:
        return plan_rows

    # Merge statt doppeltem iterrows
    merged = basis.merge(sched, on=["_tg_norm", "_lt_norm"], how="inner")

    if merged.empty:
        return plan_rows

    # Zusatz-Spalten setzen
    merged["Sortiment"] = merged["sortiment"]
    merged["Bestelltag_Name"] = merged["bestelltag"]
    merged["Bestellzeitende"] = merged["bestellzeitende"]
    merged["SortKey_Sortiment"] = merged["sortiment"]
    merged["_ist_zusatz"] = True

    # Aufräumen: nur plan_rows-Spalten behalten, Rest auffüllen
    drop_cols = ["_tg_norm", "_lt_norm", "tourengruppe", "liefertag", "sortiment",
                 "bestelltag", "bestellzeitende"]
    merged = merged.drop(columns=[c for c in drop_cols if c in merged.columns], errors="ignore")

    for col in plan_rows.columns:
        if col not in merged.columns:
            merged[col] = ""

    combined = pd.concat([plan_rows, merged[plan_rows.columns]], ignore_index=True)
    return combined


# ============================================================
# LOOKUP UND AUFBEREITUNG
# ============================================================
def apply_kostenstellen_lookup(df_plan: pd.DataFrame, df_kostenstellen: pd.DataFrame) -> pd.DataFrame:
    """Ergaenzt Tourengruppe, Kostenstelle und Leiter anhand der CSB-Tournummer.

    Der Kostenstellenplan enthaelt numerische Bereiche (sap_von/sap_bis).
    Die CSB-Tournummer (4-stellig, z.B. 4007) wird numerisch gegen diese
    Bereiche geprueft. Vectorisiert via numpy Broadcasting.
    """
    table = df_kostenstellen.copy()
    table["sap_von_num"] = pd.to_numeric(table["sap_von"], errors="coerce")
    table["sap_bis_num"] = pd.to_numeric(table["sap_bis"], errors="coerce")
    table = table.dropna(subset=["sap_von_num", "sap_bis_num"]).reset_index(drop=True)

    result = df_plan.copy()

    if table.empty:
        result["Tourengruppe"] = ""
        result["Kostenstelle"] = ""
        result["Leiter"] = ""
        return result

    # CSB Tournummern als numerische Werte
    csb_nums = pd.to_numeric(
        result["CSB Tournummer"].map(normalize_digits), errors="coerce"
    ).values

    # Vectorisierter Lookup: Kostenstellen-Tabelle ist klein (~50 Zeilen),
    # daher Loop über Tabelle mit numpy-vectorisierten Vergleichen über alle plan_rows.
    # Erster Treffer gewinnt (wie im Original).
    n = len(result)
    match_idx = np.full(n, -1, dtype=int)

    vons = table["sap_von_num"].values
    biss = table["sap_bis_num"].values

    for i in range(len(table)):
        # Nur Zeilen matchen die noch keinen Treffer haben
        unmatched = match_idx == -1
        in_range = (csb_nums >= vons[i]) & (csb_nums <= biss[i]) & unmatched
        match_idx[in_range] = i

    # Ergebnis-Spalten aus Lookup-Index ableiten
    matched = match_idx >= 0
    result["Tourengruppe"] = ""
    result["Kostenstelle"] = ""
    result["Leiter"] = ""

    if matched.any():
        valid_idx = match_idx[matched]
        result.loc[matched, "Tourengruppe"] = table["tourengruppe"].iloc[valid_idx].map(normalize_text).values
        result.loc[matched, "Kostenstelle"] = table["kostenstelle"].iloc[valid_idx].map(normalize_text).values
        result.loc[matched, "Leiter"] = table["leiter"].iloc[valid_idx].map(normalize_text).values

    return result


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
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, int], pd.DataFrame, pd.DataFrame]:
    df_kunden = load_structured_upload(kunden_bytes, kunden_name, csv_separator, "kunden")
    df_sap = load_structured_upload(sap_bytes, sap_name, csv_separator, "sap")
    df_transport = load_structured_upload(transport_bytes, transport_name, csv_separator, "transport")
    df_kisoft = load_kisoft_upload(kisoft_bytes, kisoft_name, csv_separator)
    df_kostenstellen = load_kostenstellen_upload(kostenstellen_bytes, kostenstellen_name, csv_separator)

    # normalize_text wurde bereits in cleanup_dataframe() angewendet – kein zweiter Pass nötig.

    df_sap["Kisoft_Key"] = df_sap["Rahmentour_Raw"].map(build_kisoft_key)
    df_sap["Bestelltag_Name"] = df_sap["Bestelltag"].map(day_name_from_number)

    df_sap = df_sap.merge(df_transport, on="Liefertyp_ID", how="left")

    # Kisoft ist 1:1 pro SAP Rahmentour – einfacher Merge, keine Dedup nötig.
    df_sap = df_sap.merge(
        df_kisoft[["SAP Rahmentour", "CSB Tournummer", "Wochentag", "Verladetor"]],
        left_on="Kisoft_Key",
        right_on="SAP Rahmentour",
        how="left",
    )

    # Echte Duplikate aus SAP entfernen: gleiche SAP + Bestelltag + Sortiment + Rahmentour.
    df_sap = df_sap.drop_duplicates(
        subset=["SAP_Nr", "Bestelltag", "Liefertyp_ID", "Rahmentour_Raw"], keep="first"
    ).copy()

    kunden_basis = df_kunden.merge(
        df_sap[["SAP_Nr", "Rahmentour_Raw"]].drop_duplicates(subset=["SAP_Nr"]),
        on="SAP_Nr",
        how="left",
    )
    # Kategorie wird erst nach dem Kisoft-Merge gesetzt (CSB Tournummer nötig).
    # Platzhalter damit der Merge unten funktioniert:
    kunden_basis["Kategorie"] = "Direkt"

    # Basis-Merge: Kundenstamm bekommt Grundinfos aus plan_rows
    plan_rows = df_sap.merge(
        kunden_basis[["SAP_Nr", "CSB_Nr", "Name", "Strasse", "PLZ", "Ort", "Fachberater", "Kategorie"]],
        on="SAP_Nr",
        how="left",
    )

    # Liefertag vectorisiert aus SAP Spalte G (1=Mo … 6=Sa)
    plan_rows["Liefertag"] = (
        plan_rows["Liefertag_Raw"]
        .map(normalize_digits)
        .str[:1]
        .map(lambda d: WOCHENTAGE.get(int(d), "Unbekannt") if d.isdigit() else "Unbekannt")
    )
    plan_rows["Sortiment"] = plan_rows["Liefertyp_Name"].fillna("")
    plan_rows["Bestellzeitende"] = plan_rows["Bestellzeitende"].fillna("")
    plan_rows["CSB Tournummer"] = plan_rows["CSB Tournummer"].fillna("")
    plan_rows["Verladetor"] = plan_rows["Verladetor"].fillna("")
    plan_rows["SortKey_Bestelltag"] = pd.to_numeric(plan_rows["Bestelltag"], errors="coerce").fillna(99)
    # Sortiment-Priorität: Fleisch/Heidemark zuerst, CSB-Kram zuletzt
    _SORTIMENT_PRIO = {
        "fleisch- & wurst bedienung": 0,
        "fleisch- & wurst sb":        1,
        "heidemark":                  2,
    }
    def _sortiment_key(name: str) -> tuple:
        n = str(name).strip().lower()
        # CSB-Kram (AVO, Werbemittel, Hamburger Jungs) ans Ende
        if any(k in n for k in ("avo", "werbemittel", "hamburger jungs")):
            return (9, name)
        # Prioritäts-Sortimente ganz vorne
        for key, prio in _SORTIMENT_PRIO.items():
            if key in n:
                return (prio, name)
        # Alles andere in der Mitte (alphabetisch)
        return (5, name)
    plan_rows["SortKey_Sortiment"] = plan_rows["Sortiment"].fillna("").map(_sortiment_key)

    # Kostenstellen-Lookup auf plan_rows (CSB-Tournummer ist jetzt verfuegbar)
    plan_rows = apply_kostenstellen_lookup(plan_rows, df_kostenstellen)

    # Kategorie aus erster CSB Tournummer pro Kunde bestimmen
    csb_tour_agg = (
        plan_rows[plan_rows["CSB Tournummer"] != ""]
        .drop_duplicates(subset=["SAP_Nr"])
        [["SAP_Nr", "CSB Tournummer"]]
    )
    csb_tour_agg["Kategorie"] = csb_tour_agg["CSB Tournummer"].map(classify_by_csb_tour)
    kunden_basis = kunden_basis.merge(csb_tour_agg[["SAP_Nr", "Kategorie"]], on="SAP_Nr", how="left", suffixes=("_alt", ""))
    kunden_basis["Kategorie"] = kunden_basis["Kategorie"].fillna("Direkt")
    if "Kategorie_alt" in kunden_basis.columns:
        kunden_basis = kunden_basis.drop(columns=["Kategorie_alt"])

    # Kategorie auch in plan_rows aktualisieren
    plan_rows = plan_rows.drop(columns=["Kategorie"], errors="ignore")
    plan_rows = plan_rows.merge(kunden_basis[["SAP_Nr", "Kategorie"]], on="SAP_Nr", how="left")
    plan_rows["Kategorie"] = plan_rows["Kategorie"].fillna("Direkt")

    # Tourengruppe / Kostenstelle / Leiter zurueck auf kunden_basis aggregieren
    # (erster nicht-leerer Wert pro SAP_Nr, damit die Kundenkarte diese Felder zeigt)
    kst_agg = (
        plan_rows[plan_rows["Tourengruppe"] != ""]
        .drop_duplicates(subset=["SAP_Nr"])
        [["SAP_Nr", "Tourengruppe", "Kostenstelle", "Leiter"]]
    )
    kunden_basis = kunden_basis.merge(kst_agg, on="SAP_Nr", how="left")
    for col in ["Tourengruppe", "Kostenstelle", "Leiter"]:
        kunden_basis[col] = kunden_basis[col].fillna("")

    # Zusatz-Sortimente (AVO, Werbemittel etc.) aus Kostenstellenplan generieren
    zusatz_schedule = extract_zusatz_schedule(kostenstellen_bytes, kostenstellen_name)
    plan_rows = build_zusatz_plan_rows(plan_rows, zusatz_schedule)

    counts = {cat: int((kunden_basis["Kategorie"] == cat).sum()) for cat in KATEGORIEN if cat != "Alle"}
    counts["Alle"] = int(len(kunden_basis))

    return kunden_basis, plan_rows, counts, df_kisoft, df_sap



# ============================================================
# DEBUG / QUALITÄTSPRÜFUNG
# ============================================================
def build_debug_report(
    plan_rows: pd.DataFrame,
    df_kisoft: pd.DataFrame,
    df_sap_raw: pd.DataFrame,
) -> Dict[str, pd.DataFrame]:
    """Erstellt Qualitäts-Reports für SAP ↔ Kisoft Abgleich."""
    reports: Dict[str, pd.DataFrame] = {}

    def safe_cols(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
        """Nur Spalten auswählen die wirklich vorhanden sind."""
        return df[[c for c in cols if c in df.columns]]

    # 1. SAP-Zeilen ohne Kisoft-Match (kein CSB Tournummer)
    csb_col = "CSB Tournummer"
    if csb_col in plan_rows.columns:
        no_kisoft = plan_rows[
            plan_rows[csb_col].map(normalize_text) == ""
        ]
        reports["Kein Kisoft-Match"] = safe_cols(
            no_kisoft, ["SAP_Nr", "Name", "Rahmentour_Raw", "Kisoft_Key", "Liefertag_Raw", "Liefertag", "Sortiment"]
        ).drop_duplicates().reset_index(drop=True)
    else:
        reports["Kein Kisoft-Match"] = pd.DataFrame()

    # 2. Liefertag-Konflikt: Spalte G weicht von CSB-Startzahl ab (vektorisiert)
    if "Liefertag_Raw" in plan_rows.columns and csb_col in plan_rows.columns:
        g_first   = plan_rows["Liefertag_Raw"].map(normalize_digits).str[:1]
        csb_first = plan_rows[csb_col].map(normalize_digits).str[:1]
        konflikt_mask = (
            g_first.str.match(r'\d', na=False)
            & csb_first.str.match(r'\d', na=False)
            & (g_first != csb_first)
        )
        reports["Liefertag-Konflikt SAP↔CSB"] = safe_cols(
            plan_rows[konflikt_mask],
            ["SAP_Nr", "Name", "Rahmentour_Raw", "Liefertag_Raw", "CSB Tournummer", "Liefertag", "Sortiment"]
        ).drop_duplicates().reset_index(drop=True)
    else:
        reports["Liefertag-Konflikt SAP↔CSB"] = pd.DataFrame()

    # 4. Direkt-Kunden ohne CSB-Tour
    if csb_col in plan_rows.columns and "Kategorie" in plan_rows.columns:
        unklar_mask = (
            (plan_rows["Kategorie"] == "Direkt") &
            (plan_rows[csb_col].map(normalize_text) == "")
        )
        reports["Direkt ohne CSB-Tour"] = safe_cols(
            plan_rows[unklar_mask],
            ["SAP_Nr", "Name", "Rahmentour_Raw", "Kisoft_Key", "Sortiment"]
        ).drop_duplicates().reset_index(drop=True)
    else:
        reports["Direkt ohne CSB-Tour"] = pd.DataFrame()

    # 4. Kunden mit mehreren Touren an einem Tag (verschiedene CSB Tournummern)
    if all(c in plan_rows.columns for c in ["SAP_Nr", "Liefertag", "CSB Tournummer"]):
        # Nur echte SAP-Zeilen (keine Zusatz-Sortimente), nur mit CSB Tour
        _sap_only = plan_rows[
            (plan_rows.get("_ist_zusatz", pd.Series(False, index=plan_rows.index)).fillna(False) == False)
            & (plan_rows["CSB Tournummer"].map(normalize_text) != "")
        ] if "_ist_zusatz" in plan_rows.columns else plan_rows[
            plan_rows["CSB Tournummer"].map(normalize_text) != ""
        ]
        # Eindeutige Touren pro Kunde+Tag zählen
        _tour_counts = (
            _sap_only.groupby(["SAP_Nr", "Liefertag"])["CSB Tournummer"]
            .nunique()
            .reset_index()
            .rename(columns={"CSB Tournummer": "Anzahl Touren"})
        )
        _multi = _tour_counts[_tour_counts["Anzahl Touren"] > 1]
        # Touren gruppiert: pro Tour eine Zeile mit CSB | SAP | Kisoft zusammen
        if not _multi.empty:
            has_kisoft = "SAP Rahmentour" in _sap_only.columns

            def _grouped_tours(grp):
                """Pro Gruppe (SAP_Nr, Liefertag) jede einzigartige Tour als gruppierten String."""
                seen = []
                for _, r in grp.iterrows():
                    csb = normalize_text(r.get("CSB Tournummer", ""))
                    sap = normalize_text(r.get("Rahmentour_Raw", ""))
                    kis = normalize_text(r.get("SAP Rahmentour", "")) if has_kisoft else ""
                    parts = [p for p in [csb, sap, kis] if p]
                    entry = " | ".join(parts)
                    if entry and entry not in seen:
                        seen.append(entry)
                return ", ".join(seen)

            _agg = (
                _sap_only
                .groupby(["SAP_Nr", "Liefertag"])
                .apply(_grouped_tours)
                .reset_index()
                .rename(columns={0: "Touren (CSB | SAP | Kisoft)"})
            )
            _multi = _multi.merge(_agg, on=["SAP_Nr", "Liefertag"], how="left")
            addr_cols = [c for c in ["SAP_Nr", "Name", "Strasse", "PLZ", "Ort"] if c in plan_rows.columns]
            if addr_cols:
                _addr = plan_rows[addr_cols].drop_duplicates("SAP_Nr")
                _multi = _multi.merge(_addr, on="SAP_Nr", how="left")
            _multi = _multi.sort_values(["SAP_Nr", "Liefertag"]).reset_index(drop=True)
            cols_order = [c for c in ["SAP_Nr", "Name", "Strasse", "PLZ", "Ort", "Liefertag", "Anzahl Touren", "Touren (CSB | SAP | Kisoft)"] if c in _multi.columns]
            reports["Mehrere Touren an einem Tag"] = _multi[cols_order]
        else:
            reports["Mehrere Touren an einem Tag"] = pd.DataFrame()
    else:
        reports["Mehrere Touren an einem Tag"] = pd.DataFrame()

    return reports


# ============================================================
# MASSENDRUCK – STANDARDWOCHE & SORTIERLOGIK
# ============================================================

def build_day_assignments(
    sw_sap_bytes: bytes,
    sw_sap_name: str,
    sw_kisoft_bytes: bytes,
    sw_kisoft_name: str,
    csv_separator: str,
) -> dict:
    """Erstellt Tages-Touren-Zuordnung aus Standardwoche SAP + Kisoft.

    Rückgabe: dict  { sap_nr: { "1": "1004", "3": "3007", ... }, ... }
    Schlüssel im inneren Dict = Liefertag als String ("1"=Mo … "6"=Sa).
    Wird als JSON in die HTML eingebettet – clientseitige JS-Logik übernimmt Sortierung.
    """
    df_sap = load_structured_upload(sw_sap_bytes, sw_sap_name, csv_separator, "sap")
    df_kisoft = load_kisoft_upload(sw_kisoft_bytes, sw_kisoft_name, csv_separator)

    df_sap["Kisoft_Key"] = df_sap["Rahmentour_Raw"].map(build_kisoft_key)
    df_sap["Liefertag_Num"] = (
        df_sap["Liefertag_Raw"]
        .map(normalize_digits)
        .str[:1]
        .map(lambda d: int(d) if d.isdigit() else 0)
    )

    df_merged = df_sap.merge(
        df_kisoft[["SAP Rahmentour", "CSB Tournummer"]],
        left_on="Kisoft_Key",
        right_on="SAP Rahmentour",
        how="left",
    )
    df_merged["CSB Tournummer"] = df_merged["CSB Tournummer"].fillna("").map(normalize_text)

    df_clean = (
        df_merged[df_merged["Liefertag_Num"].between(1, 6) & (df_merged["CSB Tournummer"] != "")]
        [["SAP_Nr", "Liefertag_Num", "CSB Tournummer"]]
        .sort_values(["SAP_Nr", "Liefertag_Num", "CSB Tournummer"])
        .drop_duplicates(subset=["SAP_Nr", "Liefertag_Num"])
    )

    result: dict = {}
    for _, row in df_clean.iterrows():
        sap = normalize_text(row["SAP_Nr"])
        day = str(int(row["Liefertag_Num"]))
        csb = normalize_text(row["CSB Tournummer"])
        if sap:
            result.setdefault(sap, {})[day] = csb

    return result


def render_debug_tab(reports: Dict[str, pd.DataFrame]) -> None:
    """Zeigt Debug-Reports im Streamlit-Tab."""
    total_issues = sum(len(df) for df in reports.values())
    if total_issues == 0:
        st.success("✅ Keine Auffälligkeiten gefunden – SAP und Kisoft sind konsistent.")
        return

    # Gesamt-Export aller Reports als Excel (ein Sheet pro Report)
    _buf = io.BytesIO()
    with pd.ExcelWriter(_buf, engine="openpyxl") as _writer:
        for _title, _df in reports.items():
            if not _df.empty:
                _sheet = _title[:31]  # Excel-Sheetname max 31 Zeichen
                _df.to_excel(_writer, sheet_name=_sheet, index=False)
    _buf.seek(0)
    st.download_button(
        label="📥 Alle Debug-Daten als Excel herunterladen",
        data=_buf.getvalue(),
        file_name="sendeplan_debug.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )
    st.divider()

    for title, df in reports.items():
        count = len(df)
        icon = "✅" if count == 0 else "⚠️"
        with st.expander(f"{icon} {title} ({count} Einträge)", expanded=count > 0):
            if df.empty:
                st.success("Keine Einträge.")
            else:
                st.dataframe(df, use_container_width=True, hide_index=True)
                # Download für diesen Report
                _buf2 = io.BytesIO()
                df.to_excel(_buf2, index=False, engine="openpyxl")
                _buf2.seek(0)
                _safe = title.replace("/", "-").replace(" ", "_")
                st.download_button(
                    label=f"📥 {title} exportieren",
                    data=_buf2.getvalue(),
                    file_name=f"debug_{_safe}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"dl_{_safe}",
                )


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
            | result["CSB_Nr"].str.lower().str.contains(search, na=False)
        ]

    return result.sort_values(["Name", "SAP_Nr"], na_position="last").reset_index(drop=True)


# ============================================================
# STREAMLIT-LAYOUT
# ============================================================
def streamlit_css() -> str:
    return """
    <style>
        .stApp { background: #0e1117; color: #e0e0e0; }
        .stApp p, .stApp li, .stApp label, .stApp div,
        .stApp h1, .stApp h2, .stApp h3, .stApp h4, .stApp h5,
        .stApp span { color: #e0e0e0; }
        section[data-testid="stSidebar"] { background: #161b22; border-right: 1px solid #21262d; }
        .stFileUploader { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 0.4rem; }
        div[data-baseweb="input"] input,
        .stTextInput input {
            background: #0d1117 !important; color: #e0e0e0 !important; border-color: #30363d !important;
        }
        /* Metric-Werte und -Labels */
        [data-testid="stMetricValue"] { color: #e0e0e0 !important; }
        [data-testid="stMetricLabel"] { color: #aaa !important; }
        .status-ok   { color: #3fb950; font-size: 0.85rem; }
        .status-miss { color: #f85149; font-size: 0.85rem; }

        /* Panel-Komponente */
        .app-panel {
            background: #161b22;
            border: 1px solid #30363d;
            border-radius: 10px;
            padding: 1rem 1.1rem;
            margin-bottom: 0.75rem;
        }
        .app-panel h3 { color: #e0e0e0 !important; }
        .muted-note { color: #888; font-size: 0.82rem; }

        /* Hero-Card */
        .hero-card {
            background: linear-gradient(135deg, #161b22 0%, #0d2035 100%);
            border: 1px solid #30363d;
            border-radius: 12px;
            padding: 1.2rem 1.4rem;
            margin-bottom: 1rem;
        }

        /* Status-Grid */
        .status-grid { display: flex; flex-direction: column; gap: 0.35rem; }
        .status-item { display: flex; justify-content: space-between; align-items: center; font-size: 0.83rem; }
        .status-label { color: #888; }
        .upload-ok   { color: #3fb950; }
        .upload-missing { color: #f85149; }
    </style>
    """


def render_panel(title: str, body: str) -> None:
    st.markdown(
        f"""
        <div class="app-panel">
            <h3 style="margin-top:0; margin-bottom:0.75rem;">{html.escape(title)}</h3>
            {body}
        </div>
        """,
        unsafe_allow_html=True,
    )


# ============================================================
# HTML EXPORT UND CSS
# ============================================================
def export_css() -> str:
    return """
    <style>
        :root {
            --ink:        #111;
            --ink-soft:   #444;
            --ink-muted:  #7a8fa0;
            --bg-main:    #111b25;
            --bg-card:    #18273a;
            --bg-hover:   #1e3347;
            --accent:     #f0a500;
            --accent-dim: #7a5200;
            --accent-soft:#fff3d0;
            --red:        #c00;
            --green:      #1a9e52;
            --border:     rgba(255,255,255,0.07);
            --paper-bg:   #ffffff;
        }

        *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

        body {
            background: var(--bg-main);
            font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;
            font-size: 10pt;
            color: var(--ink);
            display: flex;
            flex-direction: row;
            min-height: 100vh;
            margin: 0;
        }

        /* ══════════════════════════════════════
           SIDEBAR – Helles Design
        ══════════════════════════════════════ */
        .sidebar {
            --sb-bg:        #f5f7fa;
            --sb-border:    #dde2ea;
            --sb-hover:     #e8ecf2;
            --sb-active:    #e6a100;
            --sb-text:      #1a2332;
            --sb-muted:     #6b7a90;
            --sb-input-bg:  #ffffff;
            --sb-shadow:    rgba(0,0,0,0.06);

            width: 240px;
            min-width: 240px;
            background: var(--sb-bg);
            border-right: 1px solid var(--sb-border);
            box-shadow: 2px 0 12px var(--sb-shadow);
            min-height: 100vh;
            position: sticky;
            top: 0;
            height: 100vh;
            display: flex;
            flex-direction: column;
            z-index: 100;
            overflow-y: auto;
            scrollbar-width: thin;
            scrollbar-color: var(--sb-border) transparent;
        }
        .sidebar-logo {
            padding: 18px 16px 14px;
            border-bottom: 1px solid var(--sb-border);
            display: flex;
            align-items: center;
            gap: 10px;
            background: #fff;
        }
        .sidebar-logo-icon {
            width: 32px; height: 32px;
            background: #e6a100;
            border-radius: 8px;
            display: flex; align-items: center; justify-content: center;
            font-size: 15px; flex-shrink: 0;
            box-shadow: 0 2px 6px rgba(230,161,0,0.35);
        }
        .sidebar-logo-text {
            font-size: 13px;
            font-weight: 700;
            color: #1a2332;
            letter-spacing: 0.02em;
        }
        .sidebar-logo-sub {
            font-size: 10px;
            color: var(--sb-muted);
            font-weight: 400;
            margin-top: 1px;
        }
        .sidebar-section {
            padding: 14px 12px 10px;
            border-bottom: 1px solid var(--sb-border);
        }
        .sidebar-label {
            font-size: 9px;
            font-weight: 700;
            color: var(--sb-muted);
            text-transform: uppercase;
            letter-spacing: 0.13em;
            margin-bottom: 8px;
        }
        .sidebar input[type=text] {
            width: 100%;
            border: 1.5px solid var(--sb-border);
            border-radius: 8px;
            padding: 8px 11px;
            font-size: 12px;
            font-family: inherit;
            outline: none;
            background: var(--sb-input-bg);
            color: var(--sb-text);
            transition: border-color 0.15s, box-shadow 0.15s;
            box-shadow: 0 1px 3px rgba(0,0,0,0.05) inset;
        }
        .sidebar input[type=text]::placeholder { color: #aab2be; }
        .sidebar input[type=text]:focus {
            border-color: #e6a100;
            box-shadow: 0 0 0 3px rgba(230,161,0,0.15);
        }
        .filter-btn {
            display: flex;
            align-items: center;
            justify-content: space-between;
            width: 100%;
            border: 1px solid transparent;
            border-radius: 8px;
            padding: 8px 11px;
            font-size: 12px;
            font-weight: 500;
            font-family: inherit;
            cursor: pointer;
            background: transparent;
            color: var(--sb-text);
            text-align: left;
            margin-bottom: 2px;
            transition: all 0.14s;
        }
        .filter-btn:hover {
            background: var(--sb-hover);
            color: #0f1923;
            border-color: var(--sb-border);
        }
        .filter-btn.active {
            background: #e6a100;
            color: #fff;
            font-weight: 700;
            border-color: transparent;
            box-shadow: 0 2px 8px rgba(230,161,0,0.28);
        }
        .filter-btn .filter-count {
            font-size: 10px;
            font-family: 'Courier New', monospace;
            font-weight: 600;
            background: rgba(0,0,0,0.10);
            color: inherit;
            padding: 1px 6px;
            border-radius: 20px;
        }
        .filter-btn.active .filter-count {
            background: rgba(255,255,255,0.25);
        }
        .filter-btn-warn { color: #c0392b !important; font-weight: 600 !important; }
        .filter-btn-warn:hover { background: rgba(192,57,43,0.08) !important; color: #a02010 !important; }
        .filter-btn-warn.active { background: #c0392b !important; color: #fff !important; box-shadow: 0 2px 8px rgba(192,57,43,0.25) !important; }
        .search-btn {
            border: 1.5px solid var(--sb-border);
            border-radius: 7px;
            padding: 7px 11px;
            font-size: 12px;
            font-weight: 600;
            font-family: inherit;
            cursor: pointer;
            background: #fff;
            color: var(--sb-text);
            transition: all 0.14s;
        }
        .search-btn:hover { background: var(--sb-hover); border-color: #c5cdd8; }
        .search-btn.reset {
            background: #fff0f0;
            border-color: #f5b8b8;
            color: #c0392b;
        }
        .search-btn.reset:hover { background: #fde8e8; border-color: #e8a0a0; }
        .search-nav-row {
            display: flex;
            gap: 5px;
            margin-top: 8px;
            align-items: center;
        }
        .search-count {
            font-size: 11px;
            font-family: 'Courier New', monospace;
            color: var(--sb-muted);
            flex: 1;
        }
        .search-empty {
            display: none;
            background: #fff8e6;
            color: #9a6800;
            border-radius: 6px;
            padding: 6px 10px;
            font-size: 11px;
            font-weight: 600;
            margin-top: 6px;
            border: 1px solid #f0c84a;
        }
        .sidebar-print-btn {
            display: block;
            width: calc(100% - 28px);
            margin: 14px;
            border: none;
            border-radius: 10px;
            padding: 11px;
            font-size: 13px;
            font-weight: 700;
            font-family: inherit;
            cursor: pointer;
            background: #e6a100;
            color: #fff;
            text-align: center;
            transition: all 0.15s;
            letter-spacing: 0.01em;
            box-shadow: 0 2px 10px rgba(230,161,0,0.3);
        }
        .sidebar-print-btn:hover {
            background: #f5b400;
            transform: translateY(-1px);
            box-shadow: 0 4px 16px rgba(230,161,0,0.4);
        }
        .sidebar-subtitle-group {
            padding: 14px 12px 8px;
        }

        /* ══════════════════════════════════════
           MAIN + PAGE-STACK
        ══════════════════════════════════════ */
        .main-content { flex: 1; min-width: 0; }
        .page-stack { padding: 28px 0 40px; }

        /* ══════════════════════════════════════
           A4-PAPIER
        ══════════════════════════════════════ */
        .paper {
            width: 210mm;
            height: 297mm;
            overflow: hidden;
            margin: 0 auto 28px auto;
            background: var(--paper-bg);
            box-shadow:
                0 2px 4px rgba(0,0,0,0.12),
                0 8px 32px rgba(0,0,0,0.28);
            padding: 0;
            position: relative;
            border-radius: 3px;
        }
        .paper::before { display: none; }
        .paper-inner {
            width: 210mm;
            padding: 14mm 15mm 12mm 15mm;
            box-sizing: border-box;
            transform-origin: top left;
            zoom: 1;
        }

        /* ══════════════════════════════════════
           SEITENHEADER
        ══════════════════════════════════════ */
        .doc-header {
            display: grid;
            grid-template-columns: 52mm 1fr 44mm;
            gap: 3mm;
            align-items: flex-start;
            margin-bottom: 2mm;
            padding-bottom: 0;
            border-bottom: none;
        }
        .doc-address {
            font-size: 9pt;
            line-height: 1.5;
            color: #333;
        }
        .doc-address strong {
            font-size: 9.5pt;
            font-weight: 700;
            display: block;
            margin-bottom: 0.5mm;
            color: #111;
        }
        .doc-title-block { text-align: center; padding: 0 2mm; }
        .doc-title {
            font-size: 16pt;
            font-weight: 700;
            line-height: 1.15;
            margin-bottom: 1.5mm;
            color: #111;
        }
        .doc-subtitle {
            font-size: 22pt;
            font-weight: 700;
            color: #c00;
            margin-bottom: 1mm;
            cursor: text;
            border-radius: 3px;
            padding: 1px 4px;
            outline: none;
            display: inline-block;
        }
        .doc-subtitle:hover { background: rgba(200,0,0,0.06); }
        .doc-subtitle:focus {
            background: rgba(200,0,0,0.08);
            box-shadow: 0 0 0 2px rgba(200,0,0,0.15);
        }
        @media print {
            .doc-subtitle:hover, .doc-subtitle:focus {
                background: none; box-shadow: none;
            }
        }
        .doc-allsortiments { font-size: 8pt; color: #666; }
        .doc-logo { text-align: right; }

        /* ══════════════════════════════════════
           INFOLEISTE
        ══════════════════════════════════════ */
        .doc-infobar {
            font-size: 9pt;
            margin: 2mm 0 2mm;
            padding: 0;
            background: none;
            border: none;
            color: #333;
            display: flex;
            gap: 6mm;
        }
        .doc-infobar strong { font-weight: 700; color: #111; margin-right: 1mm; }

        /* ══════════════════════════════════════
           HAUPT-PLANTABELLE
        ══════════════════════════════════════ */
        .plan-table {
            width: 100%;
            border-collapse: collapse;
            font-size: 9pt;
            border: 0.3mm solid #999;
        }
        .plan-table thead th {
            border: 0.3mm solid #999;
            padding: 2mm 3mm;
            text-align: left;
            font-weight: 700;
            background: none;
            color: #111;
            font-size: 9pt;
        }
        .plan-table tbody td {
            border: 0.3mm solid #bbb;
            padding: 1.5mm 3mm;
            vertical-align: top;
        }
        .plan-table tr.day-start td { border-top: 1.2mm solid #222; }
        .plan-table td.liefertag-cell {
            font-weight: 700;
            width: 22mm;
            white-space: nowrap;
            vertical-align: top;
            color: #111;
            text-decoration: underline;
        }
        .plan-table td.bestelltag-cell { width: 24mm; white-space: nowrap; }
        .plan-table td.zeit-cell {
            width: 26mm;
            white-space: nowrap;
        }

        /* ══════════════════════════════════════
           COVER / SEPARATOR
        ══════════════════════════════════════ */
        .cover-page, .separator-page {
            width: 210mm;
            min-height: 297mm;
            margin: 0 auto 28px auto;
            background: #fff;
            box-shadow: 0 8px 32px rgba(0,0,0,0.28);
            padding: 20mm 16mm;
            display: flex;
            flex-direction: column;
            justify-content: center;
            align-items: center;
            text-align: center;
            border-radius: 3px;
        }
        .cover-page h1, .separator-page h1 { font-size: 26pt; color: #003366; margin-bottom: 8mm; }
        .cover-page h2, .separator-page h2 { font-size: 15pt; color: #333; margin-bottom: 4mm; }
        .cover-page p, .separator-page p   { font-size: 10pt; color: #666; margin: 1mm 0; }

        /* ══════════════════════════════════════
           SUCHE / HIGHLIGHT
        ══════════════════════════════════════ */
        .customer-entry { display: block; contain: layout style; }
        .paper.is-match  { box-shadow: 0 0 0 3px var(--accent), 0 8px 32px rgba(0,0,0,0.28); }
        .paper.is-current { box-shadow: 0 0 0 3px var(--red), 0 8px 32px rgba(0,0,0,0.28); }

        /* ══════════════════════════════════════
           DRUCK
        ══════════════════════════════════════ */
        @page { size: A4 portrait; margin: 0; }

        @media print {
            html, body {
                background: white !important;
                width: 210mm !important;
                margin: 0 !important; padding: 0 !important;
                display: block !important;
            }
            .sidebar { display: none !important; }
            .main-content { width: 210mm !important; }
            .page-stack { padding: 0 !important; }

            .customer-entry { page-break-after: always; break-after: page; }
            .customer-entry:last-child { page-break-after: auto; break-after: auto; }

            .paper {
                width: 210mm !important; height: 297mm !important;
                overflow: hidden !important;
                margin: 0 !important; padding: 0 !important;
                box-shadow: none !important; border-radius: 0 !important;
                page-break-inside: avoid; contain: layout style;
            }
            .paper-inner {
                width: 210mm !important; padding: 10mm 13mm !important;
                box-sizing: border-box !important; transform: none !important;
            }
            .doc-subtitle:hover, .doc-subtitle:focus { background: none; box-shadow: none; }
            .is-match, .is-current { box-shadow: none !important; }
            .print-hidden { display: none !important; }
        }
    </style>
    """


def render_tour_overview(customer_rows: pd.DataFrame) -> str:
    """Baut die Tourübersicht-Tabelle: Liefertag -> alle CSB-Tournummern."""
    if customer_rows.empty:
        return ""

    day_order = ["Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag", "Samstag", "Sonntag"]

    def _day_for_row(row) -> str:
        csb_digits = normalize_digits(normalize_text(row.get("CSB Tournummer", "")))
        if csb_digits and csb_digits[0].isdigit():
            return WOCHENTAGE.get(int(csb_digits[0]), "")
        return normalize_text(row.get("Liefertag", ""))

    # Effizient: nur Zeilen mit CSB-Tour
    relevant = customer_rows[customer_rows["CSB Tournummer"].map(normalize_text) != ""].copy()
    if relevant.empty:
        return ""

    relevant["_day"] = relevant.apply(_day_for_row, axis=1)
    relevant = relevant[relevant["_day"] != ""]

    tour_by_day: dict = {}
    for day, grp in relevant.groupby("_day", sort=False):
        unique_tours = list(dict.fromkeys(
            normalize_text(t) for t in grp["CSB Tournummer"].tolist() if normalize_text(t)
        ))
        if unique_tours:
            tour_by_day[day] = unique_tours

    if not tour_by_day:
        return ""

    days_present = [d for d in day_order if d in tour_by_day]
    n_cols = len(days_present)
    label_w = "18mm"
    col_w = f"calc((100% - {label_w}) / {n_cols})"
    day_spans = "".join(
        f'<span style="display:inline-block;width:{col_w}">{html.escape(d)}</span>' for d in days_present
    )
    tour_spans = "".join(
        f'<span style="display:inline-block;width:{col_w}">{"  ".join(html.escape(t) for t in tour_by_day[d])}</span>'
        for d in days_present
    )

    return f"""
    <div style="font-size:9pt; margin-bottom:2.5mm; line-height:1.6;">
        <div><strong style="display:inline-block;width:{label_w}">Liefertag:</strong>{day_spans}</div>
        <div><strong style="display:inline-block;width:{label_w}">Tour:</strong>{tour_spans}</div>
    </div>
    """


def render_plan_table(rows: pd.DataFrame) -> str:
    """Haupttabelle: Liefertag (rowspan) | Sortiment | Bestelltag | Bestellzeitende.
    Entspricht exakt dem PDF-Vorbild: Liefertag-Zelle nur einmal pro Tag, fett."""
    if rows.empty:
        return "<p>Keine Planzeilen vorhanden.</p>"

    day_order = ["Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag", "Samstag", "Sonntag", "Unbekannt"]

    # Sortieren: erst nach Liefertag (Wochentag-Reihenfolge), dann Sortiment
    def day_sort_key(day: str) -> int:
        try:
            return day_order.index(day)
        except ValueError:
            return 99

    def time_to_minutes(t: str) -> int:
        """'20:00' -> 1200, '09:15' -> 555, '' -> 9999"""
        try:
            clean = t.replace(" Uhr", "").strip()
            h, m = clean.split(":")
            return int(h) * 60 + int(m)
        except Exception:
            return 9999

    ordered = rows.copy()
    ordered["_day_order"]  = ordered["Liefertag"].map(day_sort_key)
    ordered["_time_order"] = ordered["Bestellzeitende"].map(normalize_text).map(time_to_minutes)
    ordered = ordered.sort_values(["_day_order", "SortKey_Sortiment", "_time_order"], ascending=[True, True, False])

    # Rowspan pro Liefertag zählen
    day_counts: dict = {}
    for _, row in ordered.iterrows():
        d = normalize_text(row.get("Liefertag", "Unbekannt"))
        day_counts[d] = day_counts.get(d, 0) + 1

    body_rows: list = []
    day_seen: set = set()

    for i, (_, row) in enumerate(ordered.iterrows()):
        day        = normalize_text(row.get("Liefertag", "Unbekannt"))
        sortiment  = normalize_text(row.get("Sortiment", ""))
        bestelltag = normalize_text(row.get("Bestelltag_Name", ""))
        zeitende   = normalize_text(row.get("Bestellzeitende", ""))

        # "Uhr" anhängen falls nicht schon vorhanden
        if zeitende and "uhr" not in zeitende.lower():
            zeitende = zeitende + " Uhr"

        is_day_start = day not in day_seen
        day_seen.add(day)

        day_cell = ""
        if is_day_start:
            rowspan = day_counts[day]
            day_cell = f'<td class="liefertag-cell" rowspan="{rowspan}">{html.escape(day)}</td>'

        tr_class = ' class="day-start"' if is_day_start else ""

        body_rows.append(
            f"""<tr{tr_class}>
                {day_cell}
                <td class="sortiment-cell">{html.escape(sortiment)}</td>
                <td class="bestelltag-cell">{html.escape(bestelltag)}</td>
                <td class="zeit-cell">{html.escape(zeitende)}</td>
            </tr>"""
        )

    return f"""
    <table class="plan-table">
        <thead>
            <tr>
                <th style="width:20mm;">Liefertag</th>
                <th>Sortiment</th>
                <th style="width:22mm;">Bestelltag</th>
                <th style="width:24mm;">Bestellzeitende</th>
            </tr>
        </thead>
        <tbody>
            {"".join(body_rows)}
        </tbody>
    </table>
    """


def logo_img_tag(logo_b64: str, logo_mime: str = "image/png") -> str:
    """Gibt ein <img>-Tag zurück. Im Bulk-Export wird src per JS gesetzt (einmalig im Head),
    im Einzel-Export direkt als Data-URI. Beide tragen class=doc-logo-img.
    """
    if logo_b64:
        # Einzeldokument: src direkt; Bulk: wird per JS-Injection überschrieben (kein doppeltes Einbetten)
        return (
            f'<img class="doc-logo-img" '
            f'src="data:{logo_mime};base64,{logo_b64}" '
            f'alt="NORDfrische Center" '
            f'style="max-width:44mm; max-height:20mm; width:auto; height:auto; display:block; margin-left:auto;">'
        )
    # CSS-Fallback ohne Logo
    return """
        <div style="display:inline-flex; align-items:flex-start; gap:3px;">
            <div style="border:1.5px solid #003366; padding:2mm 3mm; line-height:1.25; display:inline-block;">
                <span style="display:block; font-size:6.5pt; font-weight:800; color:#003366;
                             letter-spacing:0.06em; text-transform:uppercase;">NORDfrische</span>
                <span style="display:block; font-size:9.5pt; font-weight:900; color:#003366;">Center</span>
                <span style="display:block; font-size:5.5pt; color:#555;">Das Fleischwerk von EDEKA Nord</span>
            </div>
            <div style="border:1.5px solid #f5a623; background:#f5a623; color:#003366;
                        font-size:12pt; font-weight:900; padding:1.5mm 2.5mm; line-height:1.15;">NORD</div>
        </div>"""


def _logo_bulk_placeholder() -> str:
    """Im Bulk-Export: leeres img-Tag, src wird per JS aus window.LOGO_DATA gesetzt."""
    return (
        '<img class="doc-logo-img" src="" alt="NORDfrische Center" '
        'style="max-width:44mm; max-height:20mm; width:auto; height:auto; display:block; margin-left:auto;">'
    )



def render_customer_plan(
    customer: pd.Series,
    customer_rows: pd.DataFrame,
    logo_b64: str = "",
    logo_mime: str = "image/png",
    bulk_mode: bool = False,
) -> str:
    """Rendert eine einzelne Kundenseite exakt nach dem PDF-Vorbild."""
    sap_nr      = normalize_text(customer.get("SAP_Nr", ""))
    csb_nr      = normalize_text(customer.get("CSB_Nr", ""))
    name        = normalize_text(customer.get("Name", ""))
    strasse     = normalize_text(customer.get("Strasse", ""))
    plz         = normalize_text(customer.get("PLZ", ""))
    ort         = normalize_text(customer.get("Ort", ""))
    fachberater = normalize_text(customer.get("Fachberater", ""))
    tourengruppe = normalize_text(customer.get("Tourengruppe", ""))
    kostenstelle = normalize_text(customer.get("Kostenstelle", ""))
    leiter       = normalize_text(customer.get("Leiter", ""))
    stand = datetime.now().strftime("%d.%m.%Y")

    # Tourengruppe -> Subtitle (Standard / NMS / Malchow / MK …)
    kategorie = normalize_text(customer.get("Kategorie", ""))
    subtitle = "Standard"  # Immer Standard – per contenteditable änderbar

    tour_overview_html = render_tour_overview(customer_rows)
    plan_table_html    = render_plan_table(customer_rows)

    # Kunden-Nr. im Kopf immer aus der SAP-Nummer
    kunden_nr = sap_nr

    return f"""
    <div class="paper">
    <div class="paper-inner">

        <!-- ===== HEADER: Adresse | Titel | Logo ===== -->
        <div class="doc-header">
            <div class="doc-address">
                <strong>{html.escape(name)}</strong><br>
                {html.escape(strasse)}<br>
                {html.escape(plz)} {html.escape(ort)}
            </div>

            <div class="doc-title-block">
                <div class="doc-title">Sende- &amp; Belieferungsplan</div>
                <div class="doc-subtitle" contenteditable="true" title="Klicken zum Bearbeiten">{html.escape(subtitle)}</div>
            </div>

            <div class="doc-logo">
                {_logo_bulk_placeholder() if bulk_mode else logo_img_tag(logo_b64, logo_mime)}
            </div>
        </div>

        <!-- ===== INFOLEISTE ===== -->
        <div class="doc-infobar">
            <span><strong>Kunden-Nr.:</strong> {html.escape(kunden_nr)}</span>
            <span><strong>Fachberater:</strong> {html.escape(fachberater)}</span>
            <span><strong>Stand:</strong> {html.escape(stand)}</span>
            <span class="md-tour-inline" style="display:none"></span>
        </div>

        <!-- ===== TOUR-ÜBERSICHT ===== -->
        {tour_overview_html}

        <!-- ===== PLANTABELLE ===== -->
        {plan_table_html}

    </div>
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


def render_export_search_toolbar(massendruck_section: str = "") -> str:
    return """
    <aside class="sidebar" id="sidebar">
        <div class="sidebar-logo">
            <div class="sidebar-logo-icon">&#128230;</div>
            <div>
                <div class="sidebar-logo-text">Sendeplan</div>
                <div class="sidebar-logo-sub">NORDfrische Center</div>
            </div>
        </div>

        <div class="sidebar-section">
            <div class="sidebar-label">Suche</div>
            <input id="search-input" type="text"
                placeholder="SAP, CSB, Name, Ort \u2026"
                autocomplete="off" spellcheck="false" />
            <div class="search-nav-row">
                <button type="button" class="search-btn" id="btn-prev" title="Vorheriger (Shift+Enter)">&#8679;</button>
                <button type="button" class="search-btn" id="btn-next" title="N\u00e4chster (Enter)">&#8681;</button>
                <button type="button" class="search-btn reset" id="btn-reset" title="Zur\u00fccksetzen (Esc)">&#10005;</button>
                <span class="search-count" id="search-count"></span>
            </div>
            <div class="search-empty" id="search-empty">Keine Treffer.</div>
        </div>

        <div class="sidebar-section">
            <div class="sidebar-label">Kategorie</div>
            <button type="button" class="filter-btn active" data-kat="alle">
                Alle <span class="filter-count" id="cnt-alle"></span>
            </button>
            <button type="button" class="filter-btn" data-kat="MK">
                MK <span class="filter-count" id="cnt-mk"></span>
            </button>
            <button type="button" class="filter-btn" data-kat="Malchow">
                Malchow <span class="filter-count" id="cnt-malchow"></span>
            </button>
            <button type="button" class="filter-btn" data-kat="NMS">
                NMS <span class="filter-count" id="cnt-nms"></span>
            </button>
            <button type="button" class="filter-btn" data-kat="SuL">
                SuL <span class="filter-count" id="cnt-sul"></span>
            </button>
            <button type="button" class="filter-btn" data-kat="Direkt">
                Direkt <span class="filter-count" id="cnt-direkt"></span>
            </button>
            <button type="button" class="filter-btn filter-btn-warn" data-kat="ohne-csb">
                Ohne CSB-Tour <span class="filter-count" id="cnt-ohne-csb"></span>
            </button>
        </div>

        <div class="sidebar-subtitle-group">
            <div class="sidebar-label">Untertitel global \u00e4ndern</div>
            <input id="global-subtitle-input" type="text"
                placeholder="z.B. Standard, NMS \u2026"
                autocomplete="off" spellcheck="false" />
            <div class="search-nav-row" style="margin-top:6px;">
                <button type="button" class="search-btn" id="btn-apply-subtitle">&#10003; Alle setzen</button>
            </div>
        </div>

        """ + massendruck_section + """

        <button type="button" class="sidebar-print-btn" onclick="printCurrent()">&#128438; Drucken</button>
        <button type="button" class="sidebar-debug-btn" onclick="toggleDebug()">&#128269; Debug</button>
    </aside>
    """


def build_full_document_html(customers: pd.DataFrame, plan_rows: pd.DataFrame, include_separators: bool = False, logo_b64: str = "", logo_mime: str = "image/png", debug_data: Optional[Dict[str, pd.DataFrame]] = None, massendruck_data: Optional[dict] = None) -> str:
    # Logo einmalig als JS-Variable – wird nach DOMContentLoaded auf alle Bilder gesetzt.
    # Spart mehrere MB bei größeren Kundenstämmen (logo_b64 × N Kunden).
    if logo_b64:
        logo_head_script = f"""
        <script>
        (function() {{
            var src = "data:{logo_mime};base64,{logo_b64}";
            document.addEventListener("DOMContentLoaded", function() {{
                document.querySelectorAll(".doc-logo-img").forEach(function(img) {{ img.src = src; }});
            }});
        }})();
        </script>"""
    else:
        logo_head_script = ""

    def _build_debug_html(data: Optional[Dict[str, pd.DataFrame]]) -> str:
        if not data:
            return ""
        sections = []
        all_csv_parts: List[str] = []

        for title, df in data.items():
            count = len(df)
            icon = "✅" if count == 0 else "⚠️"
            if df.empty:
                rows_html = "<tr><td colspan='99' style='color:#888;padding:8px'>Keine Einträge</td></tr>"
                thead_html = ""
                export_btn = ""
            else:
                cols = list(df.columns)
                # Thead
                thead_html = "<thead><tr>" + "".join(f"<th>{html.escape(c)}</th>" for c in cols) + "</tr></thead>"
                # Tbody + CSV in einem Durchgang
                rows_html_parts: List[str] = []
                csv_lines: List[str] = [";".join(cols)]
                for _, row in df.iterrows():
                    row_cells = [str(row[c]) for c in cols]
                    rows_html_parts.append("<tr>" + "".join(
                        f"<td>{html.escape(cell)}</td>" for cell in row_cells
                    ) + "</tr>")
                    csv_lines.append(";".join(f'"{cell}"' for cell in row_cells))
                rows_html = "".join(rows_html_parts)

                # CSV als data-URI für diesen Report
                csv_bytes = "\n".join(csv_lines).encode("utf-8-sig")
                csv_b64 = base64.b64encode(csv_bytes).decode()
                safe_title = title.replace("/", "-").replace(" ", "_")
                export_btn = (
                    f'<a class="dbg-export" '
                    f'href="data:text/csv;base64,{csv_b64}" '
                    f'download="debug_{html.escape(safe_title)}.csv">&#8595; CSV</a>'
                )

                # Für Gesamt-Export sammeln
                all_csv_parts.append(f"=== {title} ===")
                all_csv_parts.extend(csv_lines)
                all_csv_parts.append("")

            sections.append(f"""
            <div class="dbg-section">
                <div class="dbg-title" onclick="this.parentElement.classList.toggle('open')">
                    <span>{icon} {html.escape(title)}</span>
                    <div style="display:flex;align-items:center;gap:8px;">
                        <span class="dbg-count">{count}</span>
                        {export_btn}
                    </div>
                </div>
                <div class="dbg-body">
                    <table class="dbg-table">
                        {thead_html}
                        <tbody>{rows_html}</tbody>
                    </table>
                </div>
            </div>""")

        # Gesamt-Export
        if all_csv_parts:
            all_bytes = "\n".join(all_csv_parts).encode("utf-8-sig")
            all_b64 = base64.b64encode(all_bytes).decode()
            gesamt_btn = (
                f'<a class="dbg-gesamt-export" '
                f'href="data:text/csv;base64,{all_b64}" '
                f'download="sendeplan_debug_gesamt.csv">&#8595; Alle exportieren</a>'
            )
            sections.insert(0, f'<div style="padding:0 0 12px 0;">{gesamt_btn}</div>')

        return "".join(sections)

    # ── Massendruck: JSON-Daten + Sidebar-Sektion + JS aufbauen ──
    if massendruck_data:
        md_json = json.dumps(massendruck_data, ensure_ascii=False)
        md_days_json = json.dumps({str(k): v for k, v in WOCHENTAGE.items()}, ensure_ascii=False)
        massendruck_data_script = f"""
        <script>
        window.MASSENDRUCK = {{
            assignments: {md_json},
            days: {md_days_json}
        }};
        </script>"""

        massendruck_sidebar_section = """
        <div class="sidebar-section md-section" id="md-section">
            <div class="sidebar-label">&#128438; Massendruck &ndash; Sortiert auf Normalwoche</div>
            <div class="md-day-row" id="md-day-row">
                <button class="md-day-btn" data-day="1">Mo</button>
                <button class="md-day-btn" data-day="2">Di</button>
                <button class="md-day-btn" data-day="3">Mi</button>
                <button class="md-day-btn" data-day="4">Do</button>
                <button class="md-day-btn" data-day="5">Fr</button>
                <button class="md-day-btn" data-day="6">Sa</button>
            </div>
            <div class="md-stats" id="md-stats" style="display:none"></div>
            <div class="md-btn-row" id="md-btn-row" style="display:none">
                <button type="button" class="md-overview-btn" onclick="openMdOverlay()">
                    &#128269; Reihenfolge ansehen
                </button>
                <button type="button" class="sidebar-print-btn md-print-btn"
                    onclick="printMassendruck()">&#128438; Drucken (aktive Kategorie)</button>
            </div>
        </div>

        <!-- Massendruck Overlay -->
        <div id="md-overlay" class="md-overlay" style="display:none" onclick="if(event.target===this)closeMdOverlay()">
            <div class="md-overlay-box">
                <div class="md-overlay-header">
                    <div class="md-overlay-title" id="md-overlay-title">Druckreihenfolge</div>
                    <button class="md-overlay-close" onclick="closeMdOverlay()">&#10005;</button>
                </div>
                <div class="md-overlay-stats" id="md-overlay-stats"></div>
                <div class="md-overlay-table-wrap">
                    <table class="md-table">
                        <thead><tr>
                            <th style="width:36px">#</th>
                            <th>Kundenname</th>
                            <th style="width:54px">SAP-Nr</th>
                            <th style="width:70px">Kategorie</th>
                            <th id="md-th-p" style="width:72px">Prim\u00e4r</th>
                            <th id="md-th-s" style="width:72px">Sekund\u00e4r</th>
                            <th style="width:70px">Priorit\u00e4t</th>
                        </tr></thead>
                        <tbody id="md-table-body"></tbody>
                    </table>
                </div>
                <div class="md-overlay-footer">
                    <button type="button" class="sidebar-print-btn" style="width:auto;padding:10px 28px"
                        onclick="printMassendruck()">&#128438; Drucken (aktive Kategorie)</button>
                    <button type="button" class="md-overview-btn" onclick="closeMdOverlay()">Schlie\u00dfen</button>
                </div>
            </div>
        </div>"""

        massendruck_css = """
        /* ── Massendruck Sidebar (helles Design) ── */
        .md-day-row {
            display: flex; gap: 4px; flex-wrap: wrap; margin-bottom: 6px;
        }
        .md-day-btn {
            flex: 1; min-width: 28px;
            border: 1.5px solid var(--sb-border, #dde2ea); border-radius: 6px;
            padding: 5px 2px; font-size: 11px; font-weight: 600;
            font-family: inherit; cursor: pointer;
            background: #fff; color: #4a5568;
            transition: all 0.14s;
        }
        .md-day-btn:hover { background: var(--sb-hover, #e8ecf2); color: #1a2332; border-color: #b0bac8; }
        .md-day-btn.active { background: #e6a100; color: #fff; border-color: transparent;
            box-shadow: 0 2px 6px rgba(230,161,0,0.3); }
        .md-stats {
            font-size: 10px; line-height: 1.8; margin-bottom: 6px;
            padding: 6px 9px; background: #fff;
            border-radius: 6px; border: 1px solid var(--sb-border, #dde2ea);
            color: #3a4a5c;
        }
        .md-btn-row {
            display: flex; flex-direction: column; gap: 5px;
        }
        .md-overview-btn {
            display: block; width: 100%;
            border: 1.5px solid var(--sb-border, #dde2ea); border-radius: 8px;
            padding: 8px 10px; font-size: 12px; font-weight: 600;
            font-family: inherit; cursor: pointer;
            background: #fff; color: #2a3848;
            text-align: center; transition: all 0.14s;
        }
        .md-overview-btn:hover { background: var(--sb-hover, #e8ecf2); border-color: #b0bac8; }
        .md-print-btn { margin: 0 !important; width: 100% !important; font-size: 12px !important; }

        /* ── Overlay (bleibt dunkel – Modal über weißen Papieren) ── */
        .md-overlay {
            position: fixed; inset: 0;
            background: rgba(0,0,0,0.6);
            z-index: 500;
            display: flex; align-items: center; justify-content: center;
            padding: 20px;
        }
        .md-overlay-box {
            background: #fff;
            border: 1px solid #dde2ea;
            border-radius: 14px;
            width: min(900px, 96vw);
            max-height: 88vh;
            display: flex; flex-direction: column;
            box-shadow: 0 24px 80px rgba(0,0,0,0.2);
            overflow: hidden;
        }
        .md-overlay-header {
            display: flex; align-items: center; justify-content: space-between;
            padding: 18px 22px 14px;
            border-bottom: 1px solid #e5e9f0;
            background: #f8fafc;
            flex-shrink: 0;
        }
        .md-overlay-title {
            font-size: 15px; font-weight: 700; color: #1a2332;
        }
        .md-overlay-close {
            background: none; border: none; color: #6b7a90; font-size: 18px;
            cursor: pointer; padding: 4px 8px; border-radius: 6px;
            transition: all 0.15s;
        }
        .md-overlay-close:hover { background: #e8ecf2; color: #1a2332; }
        .md-overlay-stats {
            padding: 10px 22px 8px;
            font-size: 12px; line-height: 1.8; color: #4a5568;
            flex-shrink: 0;
            border-bottom: 1px solid #e5e9f0;
            background: #fafbfc;
        }
        .md-overlay-table-wrap {
            flex: 1; overflow-y: auto; overflow-x: auto;
            scrollbar-width: thin; scrollbar-color: #dde2ea transparent;
        }
        .md-overlay-footer {
            padding: 14px 22px;
            border-top: 1px solid #e5e9f0;
            background: #f8fafc;
            display: flex; gap: 10px; align-items: center;
            flex-shrink: 0;
        }
        .md-table {
            width: 100%; border-collapse: collapse; font-size: 12px;
        }
        .md-table thead th {
            position: sticky; top: 0;
            background: #f0f4f8; color: #4a5568; padding: 8px 12px;
            text-align: left; font-size: 10px; letter-spacing: 0.06em;
            font-weight: 700; text-transform: uppercase;
            border-bottom: 1.5px solid #dde2ea;
            white-space: nowrap;
        }
        .md-table tbody td {
            padding: 7px 12px;
            border-bottom: 1px solid #f0f4f8;
            color: #2a3848;
        }
        .md-table tbody tr:hover td { background: #f5f7fa; }
        .md-table .md-tour { font-family: 'Courier New', monospace; font-size: 11px; font-weight: 600; }
        .md-prio-p { color: #1a7f3c; font-weight: 700; }
        .md-prio-s { color: #1a60b0; font-weight: 700; }
        .md-prio-u { color: #aab2be; }
        @media print { .md-section, .md-overlay { display: none !important; } }
        .md-tour-inline {
            font-family: 'Courier New', monospace;
            font-size: 9pt;
            font-weight: 700;
            color: #1a5a9e;
            letter-spacing: 0.06em;
            margin-left: auto;
        }
        @media screen { .md-tour-inline { display: none !important; } }
        """

        massendruck_js = """
        // ── Massendruck ──
        (function() {
            if (!window.MASSENDRUCK) return;
            var MD = window.MASSENDRUCK;
            var activeMdDay = null;
            var lastOrdered = [];

            function escHtml(s) {
                return (s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
            }

            function getActiveKat() {
                var btn = document.querySelector('.filter-btn.active');
                return btn ? (btn.getAttribute('data-kat') || 'alle') : 'alle';
            }

            function computeOrder(primaryDay) {
                var secondaryDay = (primaryDay % 6) + 1;
                var entries = window._allEntries || Array.from(document.querySelectorAll('.customer-entry'));
                var ordered = entries.map(function(entry) {
                    var sap = (entry.getAttribute('data-sap') || '').trim();
                    var name = entry.getAttribute('data-name') || '';
                    var kat  = entry.getAttribute('data-kategorie') || '';
                    var sap_display = entry.getAttribute('data-sap') || '';
                    var asgn = MD.assignments[sap] || {};
                    var pt = asgn[String(primaryDay)] || '';
                    var st = asgn[String(secondaryDay)] || '';
                    var prio = pt ? 0 : (st ? 1 : 2);
                    var tourDigits = (pt || st || '').replace(/\\D/g,'').padStart(8,'0');
                    return { entry: entry, pt: pt, st: st, prio: prio, name: name,
                             kat: kat, sap: sap_display,
                             key: prio + tourDigits + name };
                });
                ordered.sort(function(a,b) { return a.key < b.key ? -1 : a.key > b.key ? 1 : 0; });
                return ordered;
            }

            function buildTable(ordered, pdName, sdName) {
                var thP = document.getElementById('md-th-p');
                var thS = document.getElementById('md-th-s');
                if (thP) thP.textContent = pdName.slice(0,2) + '-Tour (Prim\u00e4r)';
                if (thS) thS.textContent = sdName.slice(0,2) + '-Tour (Sekund\u00e4r)';

                var activeKat = getActiveKat();
                var tbody = document.getElementById('md-table-body');
                tbody.innerHTML = '';
                var nr = 0;
                ordered.forEach(function(o) {
                    var katOk = activeKat === 'alle' || o.kat === activeKat ||
                                (activeKat === 'ohne-csb' && o.entry.getAttribute('data-ohne-csb') === '1');
                    if (!katOk) return;
                    nr++;
                    var prioLabel = o.prio===0
                        ? '<span class="md-prio-p">Prim\u00e4r</span>'
                        : o.prio===1
                            ? '<span class="md-prio-s">Sekund\u00e4r</span>'
                            : '<span class="md-prio-u">\u00dcbrig</span>';
                    var tr = document.createElement('tr');
                    tr.innerHTML =
                        '<td style="color:#555;text-align:right;padding-right:8px">' + nr + '</td>' +
                        '<td style="font-weight:600;color:#e0e0e0">' + escHtml(o.name) + '</td>' +
                        '<td style="font-family:monospace;font-size:11px;color:#888">' + escHtml(o.sap) + '</td>' +
                        '<td style="font-size:11px;color:#aaa">' + escHtml(o.kat) + '</td>' +
                        '<td class="md-tour" style="color:#f0a500">' + escHtml(o.pt) + '</td>' +
                        '<td class="md-tour" style="color:#58a6ff">' + escHtml(o.st) + '</td>' +
                        '<td>' + prioLabel + '</td>';
                    tbody.appendChild(tr);
                });
                return nr;
            }

            function applyMassendruck(primaryDay) {
                activeMdDay = primaryDay;
                var secondaryDay = (primaryDay % 6) + 1;
                var pdName = MD.days[String(primaryDay)] || ('Tag ' + primaryDay);
                var sdName = MD.days[String(secondaryDay)] || ('Tag ' + secondaryDay);

                lastOrdered = computeOrder(primaryDay);

                // DOM-Reihenfolge anpassen
                var stack = document.querySelector('.page-stack');
                lastOrdered.forEach(function(o) { stack.appendChild(o.entry); });
                window._allEntries = lastOrdered.map(function(o) { return o.entry; });

                // Tour-Nummer in Infoleiste eintragen
                lastOrdered.forEach(function(o) {
                    var span = o.entry.querySelector('.md-tour-inline');
                    if (!span) return;
                    var tour = o.pt || o.st || '';
                    span.style.display = '';
                    span.textContent = tour ? 'Tour: ' + tour : '';
                });

                // Zählungen (alle, unabhängig von Kategorie-Filter)
                var pCount = lastOrdered.filter(function(o){ return o.prio===0; }).length;
                var sCount = lastOrdered.filter(function(o){ return o.prio===1; }).length;
                var uCount = lastOrdered.filter(function(o){ return o.prio===2; }).length;

                var activeKat = getActiveKat();
                var katLabel = activeKat === 'alle' ? 'alle Kategorien' : activeKat;

                // Sidebar-Statistik
                var stats = document.getElementById('md-stats');
                stats.style.display = '';
                stats.innerHTML =
                    '<span style="color:#3fb950">&#9679; Prim\u00e4r (' + escHtml(pdName) + '): <strong>' + pCount + '</strong></span><br>' +
                    '<span style="color:#58a6ff">&#9679; Sekund\u00e4r (' + escHtml(sdName) + '): <strong>' + sCount + '</strong></span><br>' +
                    '<span style="color:#666">&#9679; \u00dcbrige: <strong>' + uCount + '</strong></span><br>' +
                    '<span style="color:#f0a500;font-size:9px">Druck: ' + escHtml(katLabel) + '</span>';

                var row = document.getElementById('md-btn-row');
                if (row) row.style.display = '';

                // Tag-Buttons
                document.querySelectorAll('.md-day-btn').forEach(function(b) {
                    b.classList.toggle('active', parseInt(b.getAttribute('data-day')) === primaryDay);
                });
            }

            window.openMdOverlay = function() {
                if (activeMdDay === null) return;
                var primaryDay = activeMdDay;
                var secondaryDay = (primaryDay % 6) + 1;
                var pdName = MD.days[String(primaryDay)] || ('Tag ' + primaryDay);
                var sdName = MD.days[String(secondaryDay)] || ('Tag ' + secondaryDay);
                var activeKat = getActiveKat();
                var katLabel = activeKat === 'alle' ? 'alle Kategorien' : activeKat;

                var title = document.getElementById('md-overlay-title');
                if (title) title.textContent =
                    'Druckreihenfolge \u2013 ' + pdName + ' (Prim\u00e4r) / ' + sdName + ' (Sekund\u00e4r)';

                var nr = buildTable(lastOrdered, pdName, sdName);

                var ostats = document.getElementById('md-overlay-stats');
                var pCount = lastOrdered.filter(function(o){ return o.prio===0; }).length;
                var sCount = lastOrdered.filter(function(o){ return o.prio===1; }).length;
                var uCount = lastOrdered.filter(function(o){ return o.prio===2; }).length;
                if (ostats) ostats.innerHTML =
                    '<span style="color:#3fb950;margin-right:16px">&#9679; Prim\u00e4r: <strong>' + pCount + '</strong></span>' +
                    '<span style="color:#58a6ff;margin-right:16px">&#9679; Sekund\u00e4r: <strong>' + sCount + '</strong></span>' +
                    '<span style="color:#666;margin-right:20px">&#9679; \u00dcbrige: <strong>' + uCount + '</strong></span>' +
                    '<span style="color:#f0a500">Gedruckt wird: <strong>' + escHtml(katLabel) + '</strong> (' + nr + ' Kunden)</span>';

                var overlay = document.getElementById('md-overlay');
                if (overlay) overlay.style.display = 'flex';
            };

            window.closeMdOverlay = function() {
                var overlay = document.getElementById('md-overlay');
                if (overlay) overlay.style.display = 'none';
            };

            window.printMassendruck = function() {
                closeMdOverlay();
                // Nur die aktuell sichtbare Kategorie drucken (activeKat-Filter bleibt)
                // DOM ist bereits in Massendruck-Reihenfolge – window.print() druckt was sichtbar ist
                window.print();
            };

            document.addEventListener('DOMContentLoaded', function() {
                document.querySelectorAll('.md-day-btn').forEach(function(btn) {
                    btn.addEventListener('click', function() {
                        applyMassendruck(parseInt(btn.getAttribute('data-day')));
                    });
                });
                document.addEventListener('keydown', function(e) {
                    if (e.key === 'Escape') closeMdOverlay();
                });
            });
        })();
        """
    else:
        massendruck_data_script = ""
        massendruck_sidebar_section = ""
        massendruck_css = ""
        massendruck_js = ""

    debug_html = _build_debug_html(debug_data)
    docs: List[str] = []

    # Vorab gruppieren statt pro Kunde den gesamten DataFrame zu filtern
    _plan_grouped = {sap: grp for sap, grp in plan_rows.groupby("SAP_Nr")}

    entry_count = 0
    for _, customer in customers.iterrows():
        sap = normalize_text(customer.get("SAP_Nr", ""))
        rows = _plan_grouped.get(sap, pd.DataFrame(columns=plan_rows.columns)).copy()
        csb_nr = normalize_text(customer.get("CSB_Nr", ""))
        csb_touren = sorted({
            normalize_text(value)
            for value in rows.get("CSB Tournummer", pd.Series(dtype=str)).tolist()
            if normalize_text(value)
        })
        # Volltext-Blob: alle durchsuchbaren Felder zusammenfassen
        sortimente_text = " ".join(sorted({
            normalize_text(v) for v in rows.get("Sortiment", pd.Series(dtype=str)).tolist() if normalize_text(v)
        }))
        search_blob = " ".join(
            part for part in [
                sap, csb_nr, " ".join(csb_touren),
                normalize_text(customer.get("Name", "")),
                normalize_text(customer.get("Ort", "")),
                normalize_text(customer.get("PLZ", "")),
                normalize_text(customer.get("Strasse", "")),
                normalize_text(customer.get("Fachberater", "")),
                normalize_text(customer.get("Tourengruppe", "")),
                normalize_text(customer.get("Kategorie", "")),
                sortimente_text,
            ]
            if part
        ).lower()

        entry_parts: List[str] = []
        if include_separators:
            entry_parts.append(render_separator_page(customer))
        entry_parts.append(render_customer_plan(customer, rows, logo_b64="", logo_mime=logo_mime, bulk_mode=True))

        csb_search = " ".join([part for part in [csb_nr, *csb_touren] if part]).lower()
        cust_name_escaped = html.escape(normalize_text(customer.get("Name", "")).lower())
        docs.append(
            (
                f'<section class="customer-entry" '
                f'data-sap="{html.escape(sap.lower())}" '
                f'data-csb="{html.escape(csb_search)}" '
                f'data-name="{cust_name_escaped}" '
                f'data-kategorie="{html.escape(normalize_text(customer.get("Kategorie", "")))}" '
                f'data-ohne-csb="{1 if not csb_touren else 0}" '
                f'data-search="{html.escape(search_blob)}">'
                f'{"".join(entry_parts)}'
                f'</section>'
            )
        )
        entry_count += 1

    search_script = """
    <script>
    (function () {
        "use strict";
        var allEntries = [];
        var matches    = [];
        var cursor     = -1;
        var activeKat  = "alle";

        function norm(s) {
            return (s || "").toLowerCase()
                .normalize("NFD").replace(/[\u0300-\u036f]/g, "")
                .replace(/[^a-z0-9 ]/g, " ")
                .replace(/ +/g, " ").trim();
        }

        function papers(entry) {
            return entry.querySelectorAll(".paper");
        }

        function setClass(entry, cls, on) {
            papers(entry).forEach(function (p) {
                p.classList.toggle(cls, on);
            });
        }

        function clearHighlights() {
            matches.forEach(function (e) {
                setClass(e, "is-match",   false);
                setClass(e, "is-current", false);
            });
        }

        function scrollToCurrent() {
            if (cursor < 0 || cursor >= matches.length) return;
            var entry = matches[cursor];
            setClass(entry, "is-current", true);
            entry.scrollIntoView({ behavior: "smooth", block: "start" });
        }

        function updateCounts() {
            var q = norm(document.getElementById("search-input").value);
            var counts = { alle: 0, MK: 0, Malchow: 0, NMS: 0, SuL: 0, Direkt: 0, "ohne-csb": 0 };
            allEntries.forEach(function (e) {
                var kat = e.getAttribute("data-kategorie") || "";
                var blob = norm(e.getAttribute("data-search") || "");
                var ohnecsb = e.getAttribute("data-ohne-csb") === "1";
                var matchesSearch = !q || blob.indexOf(q) !== -1;
                if (matchesSearch) {
                    counts.alle++;
                    if (counts[kat] !== undefined) counts[kat]++;
                    if (ohnecsb) counts["ohne-csb"]++;
                }
            });
            var map = { alle: "cnt-alle", MK: "cnt-mk", Malchow: "cnt-malchow", NMS: "cnt-nms", SuL: "cnt-sul", Direkt: "cnt-direkt", "ohne-csb": "cnt-ohne-csb" };
            Object.keys(map).forEach(function (k) {
                var el = document.getElementById(map[k]);
                if (el) el.textContent = counts[k] !== undefined ? counts[k] : "";
            });
        }

        function updateSearchCount() {
            var lbl = document.getElementById("search-count");
            var emp = document.getElementById("search-empty");
            var q   = document.getElementById("search-input").value.trim();
            if (!q) {
                var vis = allEntries.filter(function (e) { return e.style.display !== "none"; }).length;
                lbl.textContent = vis + " Kunden";
                emp.style.display = "none";
            } else {
                if (matches.length === 0) {
                    lbl.textContent = "0 Treffer";
                    emp.style.display = "block";
                } else {
                    lbl.textContent = (cursor + 1) + " / " + matches.length;
                    emp.style.display = "none";
                }
            }
        }

        var _searchJumped = false;  // true wenn Auto-Jump die Kategorie gesetzt hat
        var _fromSearch = false;    // true wenn applyFilter von Sucheingabe getriggert wird

        function applyFilter() {
            var q = norm(document.getElementById("search-input").value);
            clearHighlights();
            matches = [];
            cursor  = -1;

            // Auto-Jump: nur wenn von Sucheingabe getriggert
            if (_fromSearch) {
                if (q) {
                    var firstMatch = null;
                    for (var i = 0; i < allEntries.length; i++) {
                        var blob = norm(allEntries[i].getAttribute("data-search") || "");
                        if (blob.indexOf(q) !== -1) { firstMatch = allEntries[i]; break; }
                    }
                    if (firstMatch) {
                        var kat = firstMatch.getAttribute("data-kategorie") || "";
                        if (kat && kat !== activeKat) {
                            activeKat = kat;
                            _searchJumped = true;
                            document.querySelectorAll(".filter-btn").forEach(function (b) {
                                b.classList.toggle("active", b.getAttribute("data-kat") === kat);
                            });
                        }
                    }
                } else if (_searchJumped) {
                    activeKat = "alle";
                    _searchJumped = false;
                    document.querySelectorAll(".filter-btn").forEach(function (b) {
                        b.classList.toggle("active", b.getAttribute("data-kat") === "alle");
                    });
                }
                _fromSearch = false;
            }

            allEntries.forEach(function (entry) {
                var kat  = entry.getAttribute("data-kategorie") || "";
                var blob = norm(entry.getAttribute("data-search") || "");
                var ohnecsb = entry.getAttribute("data-ohne-csb") === "1";
                var katOk  = activeKat === "alle" || kat === activeKat || (activeKat === "ohne-csb" && ohnecsb);
                var srchOk = !q || blob.indexOf(q) !== -1;
                var show   = katOk && srchOk;
                entry.style.display = show ? "" : "none";
                if (show && q) {
                    setClass(entry, "is-match", true);
                    matches.push(entry);
                }
            });

            if (matches.length > 0) {
                cursor = 0;
                setClass(matches[0], "is-current", true);
                scrollToCurrent();
            }
            updateSearchCount();
            updateCounts();
        }

        function step(dir) {
            if (matches.length === 0) return;
            setClass(matches[cursor], "is-current", false);
            cursor = (cursor + dir + matches.length) % matches.length;
            setClass(matches[cursor], "is-current", true);
            scrollToCurrent();
            updateSearchCount();
        }

        function resetSearch() {
            clearHighlights();
            document.getElementById("search-input").value = "";
            activeKat = "alle";
            document.querySelectorAll(".filter-btn").forEach(function (b) {
                b.classList.toggle("active", b.getAttribute("data-kat") === "alle");
            });
            allEntries.forEach(function (e) { e.style.display = ""; });
            matches = [];
            cursor  = -1;
            updateSearchCount();
            updateCounts();
        }

        document.addEventListener("DOMContentLoaded", function () {
            allEntries = Array.from(document.querySelectorAll(".customer-entry"));
            window._allEntries = allEntries;  // für Massendruck-IIFE sichtbar

            // Debounce: bei schnellem Tippen nicht bei jedem Tastendruck filtern
            var _searchTimer = null;
            document.getElementById("search-input").addEventListener("input", function () {
                clearTimeout(_searchTimer);
                _searchTimer = setTimeout(function () { _fromSearch = true; applyFilter(); }, 150);
            });
            document.getElementById("btn-next").addEventListener("click",  function () { step(1); });
            document.getElementById("btn-prev").addEventListener("click",  function () { step(-1); });
            document.getElementById("btn-reset").addEventListener("click", resetSearch);

            document.getElementById("search-input").addEventListener("keydown", function (e) {
                if (e.key === "Enter")  { e.preventDefault(); step(e.shiftKey ? -1 : 1); }
                if (e.key === "Escape") { resetSearch(); }
            });

            // Kategorie-Filter Buttons
            document.querySelectorAll(".filter-btn").forEach(function (btn) {
                btn.addEventListener("click", function () {
                    activeKat = btn.getAttribute("data-kat");
                    _searchJumped = false;  // manueller Klick überschreibt Auto-Jump
                    document.querySelectorAll(".filter-btn").forEach(function (b) {
                        b.classList.toggle("active", b === btn);
                    });
                    applyFilter();
                });
            });

            // Globaler Untertitel
            function applyGlobalSubtitle() {
                var val = document.getElementById("global-subtitle-input").value;
                if (!val.trim()) return;
                document.querySelectorAll(".doc-subtitle").forEach(function (el) {
                    el.textContent = val;
                });
            }
            document.getElementById("btn-apply-subtitle").addEventListener("click", applyGlobalSubtitle);
            document.getElementById("global-subtitle-input").addEventListener("keydown", function (e) {
                if (e.key === "Enter") { e.preventDefault(); applyGlobalSubtitle(); }
            });

            updateSearchCount();
            updateCounts();

            // ── Inhalt auf A4 skalieren (zoom + Firefox-Fallback via transform) ──
            var supportsZoom = 'zoom' in document.documentElement.style &&
                !/firefox/i.test(navigator.userAgent);

            function fitToPage() {
                var inners = Array.from(document.querySelectorAll(".paper-inner"));
                // Reset zuerst – alle auf einmal damit Layout stabil ist
                inners.forEach(function (el) {
                    el.style.zoom = "";
                    el.style.transform = "";
                    el.style.transformOrigin = "";
                    el.style.width = "210mm";
                });
                // Alle Reads in einem Durchgang (kein Reflow-Thrashing)
                var measurements = inners.map(function (inner) {
                    return {
                        inner:   inner,
                        paperH:  inner.parentElement.clientHeight,
                        paperW:  inner.parentElement.clientWidth,
                        contentH: inner.scrollHeight,
                        contentW: inner.scrollWidth,
                    };
                });
                // Alle Writes in einem Durchgang
                measurements.forEach(function (m) {
                    var scale = Math.min(
                        m.paperH / m.contentH,
                        m.paperW / m.contentW,
                        1
                    );
                    if (scale < 1) {
                        if (supportsZoom) {
                            m.inner.style.zoom = scale;
                        } else {
                            m.inner.style.transform = "scale(" + scale + ")";
                            m.inner.style.transformOrigin = "top left";
                        }
                        m.inner.style.width = "210mm";
                    }
                });
            }
            // requestAnimationFrame: erst nach erstem Paint messen
            requestAnimationFrame(function () {
                requestAnimationFrame(fitToPage);
            });
            var _resizeTimer = null;
            window.addEventListener("resize", function () {
                clearTimeout(_resizeTimer);
                _resizeTimer = setTimeout(fitToPage, 200);
            });

            // ── Aktuell sichtbaren Kunden tracken (IntersectionObserver) ──
            var currentVisible = null;
            var observer = new IntersectionObserver(function (entries) {
                entries.forEach(function (entry) {
                    if (entry.isIntersecting) {
                        currentVisible = entry.target;
                    }
                });
            }, { threshold: 0.3 });
            allEntries.forEach(function (e) { observer.observe(e); });
        });

        window.printCurrent = function printCurrent() {
            // Wenn Suche aktiv und genau 1 Treffer: diesen drucken
            // Sonst: aktuell sichtbaren Kunden im Viewport drucken
            var entries = Array.from(document.querySelectorAll(".customer-entry"));
            var visible = entries.filter(function (e) { return e.style.display !== "none"; });

            var target = null;
            if (visible.length === 1) {
                target = visible[0];
            } else if (typeof currentVisible !== "undefined" && currentVisible) {
                target = currentVisible;
            }

            if (!target) {
                window.print();
                return;
            }

            // Alle anderen verstecken, drucken, wiederherstellen
            entries.forEach(function (e) {
                if (e !== target) e.classList.add("print-hidden");
            });
            window.print();
            entries.forEach(function (e) { e.classList.remove("print-hidden"); });
        };
    })();
    """ + massendruck_js + """
    </script>
    """

    return f"""
    <!DOCTYPE html>
    <html lang="de">
    <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1.0" />
        <title>Sendeplan-Export</title>
        {export_css()}
        <style>
        .sidebar-debug-btn {{
            display:block; width:calc(100% - 28px); margin:0 14px 10px;
            border:1.5px solid var(--sb-border,#dde2ea); border-radius:10px;
            padding:9px; font-size:12px; font-weight:600; font-family:inherit;
            cursor:pointer; background:#fff; color:#4a5568;
            text-align:center; transition:all 0.15s;
        }}
        .sidebar-debug-btn:hover {{ background:var(--sb-hover,#e8ecf2); color:#1a2332; border-color:#b0bac8; }}
        @media print {{ .sidebar-debug-btn,.debug-panel {{ display:none !important; }} }}
        .debug-panel {{
            display:none; position:fixed; top:0; right:0; bottom:0;
            width:720px; max-width:92vw;
            background:#111b25; border-left:1px solid rgba(255,255,255,0.1);
            z-index:200; overflow-y:auto; padding:20px;
            font-family:'Segoe UI',system-ui,sans-serif;
            box-shadow:-8px 0 32px rgba(0,0,0,0.5);
        }}
        .debug-panel.open {{ display:block; }}
        .debug-panel-header {{
            display:flex; align-items:center; justify-content:space-between;
            margin-bottom:16px; padding-bottom:12px;
            border-bottom:1px solid rgba(255,255,255,0.1);
        }}
        .debug-panel-title {{ font-size:15px; font-weight:700; color:#fff; }}
        .debug-close {{
            background:none; border:none; color:#aaa; font-size:20px;
            cursor:pointer; padding:4px 8px; border-radius:6px;
        }}
        .debug-close:hover {{ background:rgba(255,255,255,0.1); color:#fff; }}
        .dbg-section {{
            margin-bottom:8px; border:1px solid rgba(255,255,255,0.08);
            border-radius:8px; overflow:hidden;
        }}
        .dbg-title {{
            display:flex; justify-content:space-between; align-items:center;
            padding:10px 14px; background:rgba(255,255,255,0.05);
            cursor:pointer; font-size:12px; font-weight:600; color:#ccc;
            user-select:none;
        }}
        .dbg-title:hover {{ background:rgba(255,255,255,0.09); }}
        .dbg-count {{
            font-family:'Courier New',monospace;
            background:rgba(255,255,255,0.1);
            padding:2px 8px; border-radius:20px; font-size:11px;
        }}
        .dbg-body {{ display:none; overflow-x:auto; }}
        .dbg-section.open .dbg-body {{ display:block; }}
        .dbg-table {{
            width:100%; border-collapse:collapse; font-size:11px; color:#ccc;
        }}
        .dbg-table thead th {{
            background:#0d2035; color:#fff; padding:6px 8px; text-align:left;
            font-size:10px; letter-spacing:0.05em;
            border-bottom:1px solid rgba(255,255,255,0.1);
        }}
        .dbg-table tbody td {{
            padding:5px 8px; border-bottom:1px solid rgba(255,255,255,0.05);
        }}
        .dbg-table tbody tr:hover td {{ background:rgba(255,255,255,0.04); }}
        .dbg-export {{
            font-size:10px; font-weight:600; color:#f0a500;
            text-decoration:none; padding:2px 8px;
            border:1px solid rgba(240,165,0,0.3);
            border-radius:5px; white-space:nowrap;
        }}
        .dbg-export:hover {{ background:rgba(240,165,0,0.15); }}
        .dbg-gesamt-export {{
            display:inline-block; font-size:12px; font-weight:700;
            color:#0d2035; background:var(--accent,#f0a500);
            text-decoration:none; padding:7px 14px;
            border-radius:8px; margin-bottom:4px;
        }}
        .dbg-gesamt-export:hover {{ opacity:0.9; }}
        {massendruck_css}
        </style>
        <script>
        function toggleDebug() {{ document.getElementById('debug-panel').classList.toggle('open'); }}
        document.addEventListener('click', function(e) {{
            if (e.target.classList.contains('dbg-export') || e.target.classList.contains('dbg-gesamt-export')) {{
                e.stopPropagation();
            }}
        }});
        </script>
        {massendruck_data_script}
        {logo_head_script}
    </head>
    <body>
        {render_export_search_toolbar(massendruck_sidebar_section)}
        <div class="main-content">
        <div class="page-stack">
        {''.join(docs)}
        </div>
        </div>
        <div class="debug-panel" id="debug-panel">
            <div class="debug-panel-header">
                <div class="debug-panel-title">&#128269; SAP &harr; Kisoft Debug</div>
                <button class="debug-close" onclick="toggleDebug()">&#10005;</button>
            </div>
            {debug_html if debug_html else '<p style="color:#666;font-size:12px">Keine Debug-Daten vorhanden.</p>'}
        </div>
        {search_script}
    </body>
    </html>
    """


def build_single_document_html(customer: pd.Series, customer_rows: pd.DataFrame, logo_b64: str = "", logo_mime: str = "image/png") -> str:
    return f"""
    <!DOCTYPE html>
    <html lang="de">
    <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1.0" />
        <title>Sendeplan {html.escape(normalize_text(customer.get('SAP_Nr', '')))}</title>
        {export_css()}
    </head>
    <body>
        {render_customer_plan(customer, customer_rows, logo_b64=logo_b64, logo_mime=logo_mime)}
    </body>
    </html>
    """


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
    if "search_text" not in st.session_state:
        st.session_state.search_text = ""
    for _k in ["_massendruck_ready"]:
        if _k not in st.session_state:
            st.session_state[_k] = False


def set_category(category: str) -> None:
    st.session_state.category_filter = category


def all_required_uploads_present(upload_map: Dict[str, Optional[object]]) -> bool:
    return all(upload_map.values())


def upload_status_lines(upload_map: Dict[str, Optional[object]]) -> str:
    labels = {
        "kunden": "Kundenliste",
        "sap": "SAP",
        "transport": "Transportgruppen",
        "kisoft": "Kisoft",
        "kostenstellen": "Kostenstellen",
    }
    lines = []
    for key, label in labels.items():
        file = upload_map.get(key)
        if file is None:
            lines.append(f"<div class='status-item'><div class='status-label'>{label}</div><div class='upload-missing'>Fehlt noch</div></div>")
        else:
            lines.append(f"<div class='status-item'><div class='status-label'>{label}</div><div class='upload-ok'>{html.escape(file.name)}</div></div>")
    return "<div class='status-grid'>" + "".join(lines) + "</div>"


def show_onboarding(upload_map: Dict[str, Optional[object]]) -> None:
    st.markdown(
        """
        <div class="hero-card">
            <h1 style="margin:0;">📦 Sendeplan-Generator</h1>
            <p style="margin:0.6rem 0 0 0;">Lade links alle fünf Quelldateien hoch. Danach bekommst du sofort Filter, Kundenvorschau und eine eigenständige HTML-Datei mit Suche nach SAP- und CSB-Nummer.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    col1, col2 = st.columns([3, 2], gap="large")
    with col1:
        render_panel(
            "So funktioniert die App",
            """
            <ol style="margin:0; padding-left:1.1rem; line-height:1.7;">
                <li>Links alle fünf Dateien hochladen.</li>
                <li>Filter und Kundenauswahl werden automatisch freigeschaltet.</li>
                <li>Im Bereich <strong>Export</strong> die Standalone-HTML herunterladen.</li>
                <li>Die HTML im Browser öffnen und dort direkt nach SAP oder CSB suchen.</li>
            </ol>
            <p class="muted-note" style="margin-top:0.9rem;">Die App verarbeitet feste Spaltenpositionen. Es gibt keine automatische Erkennung.</p>
            """,
        )
        render_panel(
            "Pflichtdateien",
            """
            <ul style="margin:0; padding-left:1.1rem; line-height:1.7;">
                <li>Kundenliste: A, I, J, K, L, M, N</li>
                <li>SAP-Datei: A, H, I, O, Y</li>
                <li>Transportgruppen: A, C</li>
                <li>Kisoft: SAP Rahmentour, CSB Tournummer, Verladetor</li>
                <li>Kostenstellen: A=Tourengruppe, B=SAP-Bereich, C=Kostenstelle, D=Leiter</li>
            </ul>
            """,
        )

    with col2:
        render_panel("Upload-Status", upload_status_lines(upload_map))


def show_customer_preview(customer: pd.Series, customer_rows: pd.DataFrame) -> None:
    left, right = st.columns([4, 1.6], gap="large")

    with left:
        st.markdown(f"### {customer['Name']}")
        st.caption(f"SAP {customer['SAP_Nr']} · CSB {customer['CSB_Nr']} · {customer['PLZ']} {customer['Ort']}")

    with right:
        st.markdown("#### Eckdaten")
        st.write(f"**Kategorie:** {normalize_text(customer.get('Kategorie', '')) or '-'}")
        st.write(f"**Tourengruppe:** {normalize_text(customer.get('Tourengruppe', '')) or '-'}")
        st.write(f"**Kostenstelle:** {normalize_text(customer.get('Kostenstelle', '')) or '-'}")
        st.write(f"**Leiter:** {normalize_text(customer.get('Leiter', '')) or '-'}")

    info_cols = st.columns(4)
    info_cols[0].metric("SAP-Nummer", normalize_text(customer.get("SAP_Nr", "")) or "-")
    info_cols[1].metric("CSB-Nummer", normalize_text(customer.get("CSB_Nr", "")) or "-")
    info_cols[2].metric("Fachberater", normalize_text(customer.get("Fachberater", "")) or "-")
    info_cols[3].metric("Planzeilen", len(customer_rows))

    address = " · ".join(
        part for part in [
            normalize_text(customer.get("Strasse", "")),
            " ".join(filter(None, [normalize_text(customer.get("PLZ", "")), normalize_text(customer.get("Ort", ""))])),
        ]
        if part
    )
    st.write(f"**Adresse:** {address or '-'}")

    st.markdown("#### Planliste")
    if customer_rows.empty:
        st.warning("Für diesen Kunden sind aktuell keine Planzeilen vorhanden.")
        return

    table = customer_rows.sort_values(["SortKey_Bestelltag", "CSB Tournummer", "SortKey_Sortiment", "Bestellzeitende"]).copy()
    table = table[["Liefertag", "CSB Tournummer", "Sortiment", "Bestelltag_Name", "Bestellzeitende", "Verladetor"]].rename(
        columns={
            "CSB Tournummer": "CSB-Tour",
            "Sortiment": "Eintrag",
            "Bestelltag_Name": "Bestelltag",
        }
    )
    st.dataframe(table, use_container_width=True, hide_index=True)


def main() -> None:
    init_session_state()
    st.markdown(streamlit_css(), unsafe_allow_html=True)
    st.title("📦 Sendeplan-Generator")

    # ── Uploads ──
    col_left, col_mid, col_right = st.columns(3, gap="medium")
    with col_left:
        kunden_file = st.file_uploader("Kundenliste", type=["xlsx", "xls", "xlsm", "csv"],
                                        help="Spalten: A, I, J, K, L, M, N")
        sap_file = st.file_uploader("SAP-Datei", type=["xlsx", "xls", "xlsm", "csv"],
                                     help="Spalten: A, G, H, I, O, Y")
        transport_file = st.file_uploader("Transportgruppen", type=["xlsx", "xls", "xlsm", "csv"],
                                          help="Spalten: A, C")
    with col_mid:
        kisoft_file = st.file_uploader("Kisoft-Datei", type=["csv", "xlsx", "xls", "xlsm"],
                                        help="SAP Rahmentour, CSB Tournummer, Verladetor")
        kostenstellen_file = st.file_uploader("Kostenstellen-Datei", type=["xlsx", "xls", "xlsm", "csv"],
                                              help="A=Tourengruppe, B=SAP-Bereich, C=Kostenstelle, D=Leiter")
        logo_file = st.file_uploader("Logo (optional)", type=["png", "jpg", "jpeg", "svg", "gif", "webp"],
                                      help="Oben rechts auf jedem Sendeplan")
    with col_right:
        st.markdown("**📅 Massendruck – Standardwoche** *(optional)*")
        st.caption("Liefert Toursortiering für den Massendruck im HTML-Export.")
        sw_sap_file = st.file_uploader(
            "SAP Standardwoche",
            type=["xlsx", "xls", "xlsm", "csv"],
            key="sw_sap",
            help="SAP-Referenzwoche für Liefertag-Sortierung",
        )
        sw_kisoft_file = st.file_uploader(
            "Kisoft Standardwoche",
            type=["csv", "xlsx", "xls", "xlsm"],
            key="sw_kisoft",
            help="Kisoft-Referenzwoche für CSB-Tournummern",
        )

    upload_map = {
        "kunden": kunden_file, "sap": sap_file, "transport": transport_file,
        "kisoft": kisoft_file, "kostenstellen": kostenstellen_file,
    }

    # ── CSV-Trennzeichen ──
    csv_separator = st.selectbox(
        "CSV-Trennzeichen",
        options=[";", ",", "\t"],
        format_func=lambda x: {";" : "Semikolon ;", ",": "Komma ,", "\t": "Tab ⇥"}[x],
        index=0,
        help="Nur relevant für CSV-Uploads. Excel-Dateien ignorieren diese Einstellung.",
    )

    # ── Status-Zeile ──
    uploaded = sum(1 for v in upload_map.values() if v is not None)
    file_names = [f'<span class="status-ok">✓ {html.escape(v.name)}</span>' if v else '<span class="status-miss">✗ fehlt</span>'
                  for k, v in upload_map.items()]
    labels = ["Kunden", "SAP", "Transport", "Kisoft", "Kostenstellen"]
    status_parts = [f"{l}: {f}" for l, f in zip(labels, file_names)]
    st.markdown(f"<p style='font-size:0.85rem;margin:0.5rem 0;'>{'&ensp;·&ensp;'.join(status_parts)}</p>", unsafe_allow_html=True)

    if not all_required_uploads_present(upload_map):
        st.info("Alle 5 Dateien hochladen, dann erscheint der Button.")
        return

    # ── Daten verarbeiten (csv_separator kommt aus Selectbox oben) ──
    try:
        _cache_key = hashlib.md5(
            kunden_file.getvalue() + sap_file.getvalue() +
            transport_file.getvalue() + kisoft_file.getvalue() +
            kostenstellen_file.getvalue() +
            csv_separator.encode()
        ).hexdigest()

        if st.session_state.get("_df_cache_key") != _cache_key:
            _result = prepare_dataframes(
                kunden_file.getvalue(), kunden_file.name,
                sap_file.getvalue(), sap_file.name,
                transport_file.getvalue(), transport_file.name,
                kisoft_file.getvalue(), kisoft_file.name,
                kostenstellen_file.getvalue(), kostenstellen_file.name,
                csv_separator,
            )
            st.session_state["_df_cache_key"] = _cache_key
            st.session_state["_df_cache_result"] = _result
            st.session_state["_export_ready"] = False  # alte HTML verwerfen

        (customers_df, plan_rows_df, counts,
         df_kisoft_debug, df_sap_debug) = st.session_state["_df_cache_result"]
    except Exception as exc:
        st.error(f"Fehler beim Verarbeiten: {exc}")
        return

    # Debug-Reports cachen
    _data_key = st.session_state.get("_df_cache_key", "")
    if st.session_state.get("_debug_cache_key") != _data_key:
        st.session_state["_debug_reports"] = build_debug_report(plan_rows_df, df_kisoft_debug, df_sap_debug)
        st.session_state["_debug_cache_key"] = _data_key
    debug_reports = st.session_state["_debug_reports"]

    # ── Logo vorbereiten ──
    logo_b64 = ""
    logo_mime = "image/png"
    if logo_file is not None:
        logo_b64 = base64.b64encode(logo_file.getvalue()).decode("utf-8")
        ext = logo_file.name.rsplit(".", 1)[-1].lower()
        logo_mime = {"jpg": "image/jpeg", "jpeg": "image/jpeg", "png": "image/png",
                     "svg": "image/svg+xml", "gif": "image/gif", "webp": "image/webp"}.get(ext, "image/png")

    # ── Massendruck-Daten vorbereiten (gecacht) ──
    md_data = None
    if sw_sap_file and sw_kisoft_file:
        try:
            _sw_key = hashlib.md5(sw_sap_file.getvalue() + sw_kisoft_file.getvalue()).hexdigest()
            if st.session_state.get("_sw_cache_key") != _sw_key:
                st.session_state["_day_assignments"] = build_day_assignments(
                    sw_sap_file.getvalue(), sw_sap_file.name,
                    sw_kisoft_file.getvalue(), sw_kisoft_file.name,
                    csv_separator,
                )
                st.session_state["_sw_cache_key"] = _sw_key
            md_data = st.session_state.get("_day_assignments")
        except Exception as exc:
            st.warning(f"Standardwoche konnte nicht verarbeitet werden: {exc}")

    st.divider()

    # ── Tabs: Plan | Vorschau | Debug ──
    total_issues = sum(len(df) for df in debug_reports.values())
    debug_label = f"🔍 Debug ({total_issues} ⚠️)" if total_issues > 0 else "🔍 Debug ✅"
    tab_plan, tab_preview, tab_debug = st.tabs(["⚡ Export", "👁 Kundenvorschau", debug_label])

    # ── Tab: Export ──
    with tab_plan:
        cat_parts = [f"{k}: {v}" for k, v in counts.items() if k != "Alle"]
        st.markdown(
            f"**{len(customers_df)} Kunden** · {len(plan_rows_df)} Planzeilen · {' · '.join(cat_parts)}"
        )
        if md_data:
            st.caption("✓ Standardwoche geladen – Massendruck-Sortierung im HTML verfügbar.")

        include_sep = st.checkbox(
            "Trennseiten einfügen (Separator-Pages vor jedem Kunden)",
            value=False,
            help="Fügt vor jede Kundenseite ein A4-Deckblatt mit Name und SAP-Nr. ein.",
        )

        if st.button("⚡ Plan generieren", use_container_width=True, type="primary"):
            progress = st.progress(0, text="Vorbereitung …")
            n = len(customers_df)
            _plan_grouped = {sap: grp for sap, grp in plan_rows_df.groupby("SAP_Nr")}
            # Fortschritt: HTML-Build mit Zwischenmeldungen
            with st.spinner(f"Generiere HTML für {n} Kunden …"):
                bulk_html = build_full_document_html(
                    customers_df, plan_rows_df,
                    include_separators=include_sep,
                    logo_b64=logo_b64, logo_mime=logo_mime,
                    debug_data=debug_reports,
                    massendruck_data=md_data,
                )
            progress.progress(100, text="Fertig!")
            st.session_state["_export_html"] = bulk_html
            st.session_state["_export_ready"] = True

        if st.session_state.get("_export_ready"):
            html_bytes = st.session_state["_export_html"].encode("utf-8")
            size_kb = len(html_bytes) / 1024
            size_label = f"{size_kb/1024:.1f} MB" if size_kb > 1024 else f"{size_kb:.0f} KB"
            st.download_button(
                label=f"⬇  sendeplan.html herunterladen  ({size_label})",
                data=html_bytes,
                file_name="sendeplan.html",
                mime="text/html",
                use_container_width=True,
            )
            st.caption("HTML im Browser öffnen → Suche, Filter, Druck alles drin.")

    # ── Tab: Kundenvorschau ──
    with tab_preview:
        st.markdown("Wähle einen Kunden zur schnellen Voransicht – ohne vollen HTML-Export.")

        # Kategorie-Filter
        kat_filter = st.selectbox(
            "Kategorie filtern",
            options=KATEGORIEN,
            index=0,
            key="preview_kat",
        )
        search_input = st.text_input(
            "Suche (Name, SAP, CSB, Ort)",
            key="preview_search",
            placeholder="z.B. Edeka Muster oder 1234567",
        )

        filtered = filter_customers(customers_df, kat_filter, search_input)

        if filtered.empty:
            st.warning("Keine Kunden gefunden.")
        else:
            option_labels = {
                row["SAP_Nr"]: f"{row['SAP_Nr']}  |  {row['Name']}  |  {row['Ort']}"
                for _, row in filtered.iterrows()
            }
            selected_sap = st.selectbox(
                f"{len(filtered)} Kunden gefunden – auswählen:",
                options=list(option_labels.keys()),
                format_func=lambda k: option_labels[k],
                key="preview_select",
            )
            if selected_sap:
                customer_row = customers_df[customers_df["SAP_Nr"] == selected_sap].iloc[0]
                customer_plan_rows = plan_rows_df[plan_rows_df["SAP_Nr"] == selected_sap]
                st.divider()
                show_customer_preview(customer_row, customer_plan_rows)

    # ── Tab: Debug ──
    with tab_debug:
        render_debug_tab(debug_reports)


if __name__ == "__main__":
    main()
