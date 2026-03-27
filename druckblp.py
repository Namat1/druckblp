import base64
import html
import io
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st


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


def classify_customer(rahmentour_raw: str, csb_nr: str) -> str:
    """Fallback-Klassifizierung (wird überschrieben sobald CSB Tournummer bekannt ist)."""
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


@st.cache_data(show_spinner=False)
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
        import openpyxl as _openpyxl
        _wb = _openpyxl.load_workbook(io.BytesIO(file_bytes), read_only=True)
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


@st.cache_data(show_spinner=False)
def extract_zusatz_schedule(file_bytes: bytes, filename: str) -> pd.DataFrame:
    """Extrahiert den Bestellplan fuer Zusatz-Sortimente (AVO, Werbemittel, …)
    aus dem Kostenstellenplan CSB Standard.

    Ergebnis-DataFrame: tourengruppe | liefertag | sortiment | bestelltag | bestellzeitende
    """
    import openpyxl as _opx
    import io as _io
    import re as _re

    wb = _opx.load_workbook(_io.BytesIO(file_bytes), read_only=True)
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
        if not _re.search(r'\d', b_str):
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

    # Normalize Tourengruppe in schedule for matching
    sched = zusatz_schedule.copy()
    sched["tourengruppe_norm"] = sched["tourengruppe"].str.strip().str.lower()
    basis["tourengruppe_norm"] = basis["Tourengruppe"].str.strip().str.lower()

    new_rows = []
    for _, base in basis.iterrows():
        tg_norm   = base["tourengruppe_norm"]
        liefertag = base["Liefertag"]
        if not tg_norm or not liefertag:
            continue

        matches = sched[
            (sched["tourengruppe_norm"] == tg_norm) &
            (sched["liefertag"].str.lower() == liefertag.lower())
        ]

        for _, m in matches.iterrows():
            new_row = base.drop("tourengruppe_norm").to_dict()
            new_row["Sortiment"]       = m["sortiment"]
            new_row["Bestelltag_Name"] = m["bestelltag"]
            new_row["Bestellzeitende"] = m["bestellzeitende"]
            new_row["Liefertag"]       = liefertag
            new_row["SortKey_Sortiment"] = m["sortiment"]
            new_rows.append(new_row)

    if not new_rows:
        return plan_rows

    zusatz_df = pd.DataFrame(new_rows)
    # Fehlende Spalten auffuellen
    for col in plan_rows.columns:
        if col not in zusatz_df.columns:
            zusatz_df[col] = ""

    combined = pd.concat([plan_rows, zusatz_df[plan_rows.columns]], ignore_index=True)
    return combined


# ============================================================
# LOOKUP UND AUFBEREITUNG
# ============================================================
def apply_kostenstellen_lookup(df_plan: pd.DataFrame, df_kostenstellen: pd.DataFrame) -> pd.DataFrame:
    """Ergaenzt Tourengruppe, Kostenstelle und Leiter anhand der CSB-Tournummer.

    Der Kostenstellenplan enthaelt numerische Bereiche (sap_von/sap_bis).
    Die CSB-Tournummer (4-stellig, z.B. 4007) wird numerisch gegen diese
    Bereiche geprueft. Das Ergebnis wird je plan_row gesetzt.
    """
    table = df_kostenstellen.copy()
    table["sap_von_num"] = pd.to_numeric(table["sap_von"], errors="coerce")
    table["sap_bis_num"] = pd.to_numeric(table["sap_bis"], errors="coerce")
    table = table.dropna(subset=["sap_von_num", "sap_bis_num"])

    def lookup_csb(csb_tour: str) -> pd.Series:
        empty = pd.Series({"Tourengruppe": "", "Kostenstelle": "", "Leiter": ""})
        num = pd.to_numeric(normalize_digits(csb_tour), errors="coerce")
        if pd.isna(num):
            return empty
        match = table[(table["sap_von_num"] <= num) & (table["sap_bis_num"] >= num)]
        if match.empty:
            return empty
        row = match.iloc[0]
        return pd.Series({
            "Tourengruppe": normalize_text(row["tourengruppe"]),
            "Kostenstelle": normalize_text(row["kostenstelle"]),
            "Leiter":       normalize_text(row["leiter"]),
        })

    result = df_plan.copy()
    result[["Tourengruppe", "Kostenstelle", "Leiter"]] = result["CSB Tournummer"].apply(lookup_csb)
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

    for dataset in [df_kunden, df_sap, df_transport, df_kisoft]:
        for column in dataset.columns:
            dataset[column] = dataset[column].map(normalize_text)

    df_sap["Kisoft_Key"] = df_sap["Rahmentour_Raw"].map(build_kisoft_key)
    df_sap["Bestelltag_Name"] = df_sap["Bestelltag"].map(day_name_from_number)

    df_sap = df_sap.merge(df_transport, on="Liefertyp_ID", how="left")
    df_sap = df_sap.merge(
        df_kisoft[["SAP Rahmentour", "CSB Tournummer", "Wochentag", "Verladetor"]],
        left_on="Kisoft_Key",
        right_on="SAP Rahmentour",
        how="left",
    )

    # Doppelte SAP-Zeilen nur einmal übernehmen: gleiche SAP, gleicher Bestelltag, gleiche Transportgruppe.
    df_sap = df_sap.drop_duplicates(subset=["SAP_Nr", "Bestelltag", "Liefertyp_ID"], keep="first").copy()

    def infer_liefertag(row: pd.Series) -> str:
        # 1. Erste Ziffer der CSB-Tournummer = Liefertag (1=Mo, 2=Di, …)
        #    Hat immer Vorrang – auch wenn Kisoft einen abweichenden Wochentag liefert.
        csb_tour = normalize_digits(row.get("CSB Tournummer", ""))
        if csb_tour and csb_tour[0].isdigit():
            day = int(csb_tour[0])
            if day in WOCHENTAGE:
                return WOCHENTAGE[day]
        # 2. Fallback: Wochentag aus Kisoft (wenn keine gültige CSB-Startzahl)
        wochentag = normalize_text(row.get("Wochentag", ""))
        if wochentag and wochentag.lower() not in ("", "nan"):
            return wochentag.capitalize()
        # 3. Letzter Fallback: Bestelltag aus SAP
        return row.get("Bestelltag_Name", "Unbekannt")

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

    plan_rows["Liefertag"] = plan_rows.apply(infer_liefertag, axis=1)
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
            | result["CSB_Nr"].str.lower().str.contains(search, na=False)
        ]

    return result.sort_values(["Name", "SAP_Nr"], na_position="last").reset_index(drop=True)


# ============================================================
# STREAMLIT-LAYOUT
# ============================================================
def streamlit_css() -> str:
    return """
    <style>
        .stApp {
            background: #f3f6fb;
            color: #111827;
        }

        .stApp, .stApp p, .stApp li, .stApp label, .stApp div,
        .stApp h1, .stApp h2, .stApp h3, .stApp h4, .stApp h5 {
            color: #111827;
        }

        section[data-testid="stSidebar"] {
            background: #f8fafc;
            border-right: 1px solid #dbe3ef;
        }

        div[data-baseweb="input"] input,
        div[data-baseweb="select"] input,
        textarea,
        .stTextInput input {
            background: #ffffff !important;
            color: #111827 !important;
        }

        .stFileUploader {
            background: #ffffff;
            border: 1px solid #d8e0ea;
            border-radius: 14px;
            padding: 0.6rem;
        }

        .app-panel {
            background: #ffffff;
            border: 1px solid #d8e0ea;
            border-radius: 16px;
            padding: 1rem 1.1rem;
            margin-bottom: 1rem;
            box-shadow: 0 6px 18px rgba(15, 23, 42, 0.04);
        }

        .hero-card {
            background: linear-gradient(135deg, #123a63 0%, #1f5d97 100%);
            color: white;
            border-radius: 18px;
            padding: 1.3rem 1.4rem;
            margin-bottom: 1rem;
            box-shadow: 0 12px 24px rgba(18, 58, 99, 0.18);
        }

        .hero-card h1,
        .hero-card h3,
        .hero-card p,
        .hero-card li {
            color: white !important;
        }

        .muted-note {
            color: #516074;
            font-size: 0.95rem;
        }

        .status-grid {
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 0.75rem;
        }

        .status-item {
            background: #f8fbff;
            border: 1px solid #dbe7f3;
            border-radius: 12px;
            padding: 0.8rem 0.9rem;
        }

        .status-label {
            font-size: 0.84rem;
            color: #526173;
            margin-bottom: 0.2rem;
        }

        .status-value {
            font-weight: 700;
            font-size: 1.02rem;
            color: #123a63;
        }

        .upload-ok {
            color: #166534;
            font-weight: 600;
        }

        .upload-missing {
            color: #92400e;
            font-weight: 600;
        }
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
        *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

        body {
            background: #d8dfe8;
            font-family: Arial, Helvetica, sans-serif;
            font-size: 10pt;
            color: #111;
            display: flex;
            flex-direction: row;
            min-height: 100vh;
            margin: 0;
        }

        /* ══════════════════════════════════════
           LINKE SIDEBAR (on-screen only)
        ══════════════════════════════════════ */
        .sidebar {
            width: 230px;
            min-width: 230px;
            background: #1a3a5c;
            min-height: 100vh;
            position: sticky;
            top: 0;
            height: 100vh;
            display: flex;
            flex-direction: column;
            gap: 0;
            z-index: 100;
            box-shadow: 2px 0 12px rgba(0,0,0,0.2);
            overflow-y: auto;
        }
        .sidebar-logo {
            font-size: 15px;
            font-weight: 800;
            color: #f5a623;
            letter-spacing: 0.04em;
            padding: 18px 16px 12px 16px;
            border-bottom: 1px solid #2a5080;
        }
        .sidebar-section {
            padding: 14px 12px 10px 12px;
            border-bottom: 1px solid #2a5080;
        }
        .sidebar-label {
            font-size: 10px;
            font-weight: 700;
            color: #7aafd4;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            margin-bottom: 7px;
        }
        .sidebar input[type=text] {
            width: 100%;
            border: none;
            border-radius: 6px;
            padding: 8px 10px;
            font-size: 12px;
            outline: none;
            background: #fff;
            color: #111;
            box-sizing: border-box;
        }
        .sidebar input[type=text]:focus { box-shadow: 0 0 0 2px #f5a623; }
        .filter-btn {
            display: block;
            width: 100%;
            border: none;
            border-radius: 6px;
            padding: 9px 12px;
            font-size: 13px;
            font-weight: 700;
            cursor: pointer;
            background: #2a4e72;
            color: #cde;
            text-align: left;
            margin-bottom: 5px;
            transition: background 0.15s, color 0.15s;
        }
        .filter-btn:hover { background: #2a5298; color: #fff; }
        .filter-btn.active { background: #f5a623; color: #1a3a5c; }
        .filter-btn .filter-count {
            float: right;
            font-size: 11px;
            font-weight: 600;
            opacity: 0.75;
        }
        .search-btn {
            border: none;
            border-radius: 6px;
            padding: 8px 10px;
            font-size: 12px;
            font-weight: 700;
            cursor: pointer;
            background: #2a5298;
            color: #fff;
            transition: background 0.15s;
        }
        .search-btn:hover { background: #3a6bc4; }
        .search-btn.reset { background: #c0392b; }
        .search-btn.reset:hover { background: #e74c3c; }
        .search-nav-row {
            display: flex;
            gap: 5px;
            margin-top: 7px;
            align-items: center;
        }
        .search-count {
            font-size: 11px;
            color: #bcd0ec;
            flex: 1;
        }
        .search-empty {
            display: none;
            background: #fff3cd;
            color: #856404;
            border-radius: 6px;
            padding: 6px 10px;
            font-size: 11px;
            font-weight: 600;
            margin-top: 6px;
        }
        .sidebar-print-btn {
            display: block;
            width: calc(100% - 24px);
            margin: 12px;
            border: none;
            border-radius: 8px;
            padding: 10px;
            font-size: 13px;
            font-weight: 700;
            cursor: pointer;
            background: #1a7a3a;
            color: #fff;
            text-align: center;
            transition: background 0.15s;
        }
        .sidebar-print-btn:hover { background: #22a34e; }
        .sidebar-subtitle-group {
            padding: 12px 12px 6px 12px;
        }

        /* ══════════════════════════════════════
           SEITEN-WRAPPER + MAIN
        ══════════════════════════════════════ */
        .main-content {
            flex: 1;
            min-width: 0;
        }
        .page-stack {
            padding: 20px 0;
        }

        /* ══════════════════════════════════════
           A4-PAPIER
        ══════════════════════════════════════ */
        .paper {
            width: 210mm;
            min-height: 297mm;
            margin: 0 auto 20px auto;
            background: #fff;
            box-shadow: 0 4px 24px rgba(0,0,0,0.18);
            padding: 12mm 13mm 14mm 13mm;
            position: relative;
        }

        /* ══════════════════════════════════════
           SEITENHEADER
        ══════════════════════════════════════ */
        .doc-header {
            display: grid;
            grid-template-columns: 52mm 1fr 44mm;
            gap: 3mm;
            align-items: flex-start;
            margin-bottom: 4mm;
            padding-bottom: 3mm;
        }
        .doc-address {
            font-size: 9pt;
            line-height: 1.5;
        }
        .doc-address strong {
            font-size: 9.5pt;
            font-weight: 700;
            display: block;
            margin-bottom: 0.5mm;
        }
        .doc-title-block {
            text-align: center;
            padding: 0 4mm;
        }
        .doc-title {
            font-size: 20pt;
            font-weight: 700;
            letter-spacing: -0.01em;
            line-height: 1.1;
            margin-bottom: 1mm;
        }
        .doc-subtitle {
            font-size: 14pt;
            font-weight: 700;
            color: #cc0000;
            margin-bottom: 1.5mm;
            cursor: text;
            border-radius: 3px;
            padding: 0 2px;
            outline: none;
            transition: background 0.15s;
        }
        .doc-subtitle:hover {
            background: rgba(204, 0, 0, 0.07);
        }
        .doc-subtitle:focus {
            background: rgba(204, 0, 0, 0.1);
            box-shadow: 0 0 0 2px rgba(204,0,0,0.25);
        }
        @media print {
            .doc-subtitle:hover, .doc-subtitle:focus {
                background: none;
                box-shadow: none;
            }
        }
        .doc-allsortiments {
            font-size: 8.5pt;
            color: #444;
        }
        .doc-logo {
            text-align: right;
        }

        /* ══════════════════════════════════════
           INFOLEISTE
        ══════════════════════════════════════ */
        .doc-infobar {
            font-size: 9.5pt;
            margin: 3.5mm 0 4mm 0;
            padding-top: 2.5mm;
            border-top: 1px solid #ccc;
        }
        .doc-infobar strong { font-weight: 700; }

        /* ══════════════════════════════════════
           TOUR-ÜBERSICHT
        ══════════════════════════════════════ */
        .tour-overview {
            width: 100%;
            border-collapse: collapse;
            font-size: 9pt;
            margin-bottom: 4mm;
        }
        .tour-overview td {
            border: 1px solid #aaa;
            padding: 1.2mm 2.5mm;
            white-space: nowrap;
        }
        .tour-overview tr:first-child td { font-weight: 700; }
        .tour-overview td:first-child {
            font-weight: 700;
            background: #f0f0f0;
            width: 20mm;
        }

        /* ══════════════════════════════════════
           HAUPT-PLANTABELLE
        ══════════════════════════════════════ */
        .plan-table {
            width: 100%;
            border-collapse: collapse;
            border: 1.5px solid #999;
            font-size: 9pt;
        }
        .plan-table thead th {
            border: 1px solid #999;
            padding: 2mm 2.5mm;
            text-align: left;
            font-weight: 700;
            background: #fff;
            font-size: 9.5pt;
        }
        .plan-table tbody td {
            border: 1px solid #bbb;
            padding: 1.5mm 2.5mm;
            vertical-align: top;
        }
        .plan-table tr.day-start td {
            border-top: 1.5px solid #888;
        }
        .plan-table td.liefertag-cell {
            font-weight: 700;
            width: 22mm;
            white-space: nowrap;
            vertical-align: top;
        }
        .plan-table td.bestelltag-cell {
            width: 24mm;
            white-space: nowrap;
        }
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
            margin: 0 auto 20px auto;
            background: #fff;
            box-shadow: 0 4px 24px rgba(0,0,0,0.18);
            padding: 20mm 16mm;
            display: flex;
            flex-direction: column;
            justify-content: center;
            align-items: center;
            text-align: center;
        }
        .cover-page h1, .separator-page h1 {
            font-size: 26pt; color: #003366; margin-bottom: 8mm;
        }
        .cover-page h2, .separator-page h2 {
            font-size: 15pt; color: #333; margin-bottom: 4mm;
        }
        .cover-page p, .separator-page p {
            font-size: 10pt; color: #666; margin: 1mm 0;
        }

        /* ══════════════════════════════════════
           HIGHLIGHT BEIM SUCHEN
        ══════════════════════════════════════ */
        .customer-entry { display: block; }
        .paper.is-match {
            outline: 3px solid #f5a623;
            outline-offset: -2px;
        }
        .paper.is-current {
            outline: 4px solid #e74c3c;
            outline-offset: -2px;
        }

        /* ══════════════════════════════════════
           DRUCK
        ══════════════════════════════════════ */
        @page {
            size: A4 portrait;
            margin: 0;
        }

        @media print {
            html, body {
                background: white !important;
                width: 210mm !important;
                margin: 0 !important;
                padding: 0 !important;
                display: block !important;
            }
            .sidebar { display: none !important; }
            .main-content { width: 210mm !important; }
            .page-stack { padding: 0 !important; }

            .customer-entry {
                page-break-after: always;
                break-after: page;
            }
            .customer-entry:last-child {
                page-break-after: auto;
                break-after: auto;
            }

            .paper {
                width: 210mm !important;
                min-height: 0 !important;
                max-height: none !important;
                margin: 0 !important;
                padding: 10mm 12mm !important;
                box-shadow: none !important;
                border-radius: 0 !important;
                page-break-inside: avoid;
            }

            .is-match, .is-current { outline: none !important; }
        }
    </style>
    """


def render_tour_overview(customer_rows: pd.DataFrame) -> str:
    """Baut die Tourübersicht-Tabelle: Liefertag -> CSB-Tournummer, nach PDF-Vorbild."""
    if customer_rows.empty:
        return ""

    # Pro Liefertag die eindeutigen CSB-Tournummern sammeln (sortiert nach Wochentag-Reihenfolge)
    day_order = ["Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag", "Samstag", "Sonntag"]
    tour_by_day: dict = {}
    for _, row in customer_rows.iterrows():
        day = normalize_text(row.get("Liefertag", ""))
        csb = normalize_text(row.get("CSB Tournummer", ""))
        if day and csb and day not in tour_by_day:
            tour_by_day[day] = csb

    if not tour_by_day:
        return ""

    days_present = [d for d in day_order if d in tour_by_day]

    day_cells  = "".join(f"<td>{html.escape(d)}</td>" for d in days_present)
    tour_cells = "".join(f"<td>{html.escape(tour_by_day[d])}</td>" for d in days_present)

    return f"""
    <table class="tour-overview">
        <tr>
            <td>Liefertag:</td>
            {day_cells}
        </tr>
        <tr>
            <td>Tour:</td>
            {tour_cells}
        </tr>
    </table>
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
    """Gibt ein <img>-Tag zurueck. Base64 wird einmalig per JS gesetzt – nicht pro Seite."""
    if logo_b64:
        return (
            '<img class="doc-logo-img" alt="NORDfrische Center" '
            'style="max-width:44mm; max-height:20mm; width:auto; height:auto; display:block; margin-left:auto;">'
        )
    # CSS-Fallback
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



def render_customer_plan(customer: pd.Series, customer_rows: pd.DataFrame, logo_b64: str = "", logo_mime: str = "image/png") -> str:
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

    # Tourengruppe -> Subtitle (Standart / NMS / Malchow / MK …)
    kategorie = normalize_text(customer.get("Kategorie", ""))
    subtitle_map = {
        "Direkt": "Standart",
        "NMS":    "NMS",
        "Malchow":"Malchow",
        "MK":     "MK",
    }
    subtitle = subtitle_map.get(kategorie, kategorie or "Standart")

    tour_overview_html = render_tour_overview(customer_rows)
    plan_table_html    = render_plan_table(customer_rows)

    # Kunden-Nr. im Kopf immer aus der SAP-Nummer
    kunden_nr = sap_nr

    return f"""
    <div class="paper">

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
                {logo_img_tag(logo_b64, logo_mime)}
            </div>
        </div>

        <!-- ===== INFOLEISTE ===== -->
        <div class="doc-infobar">
            <strong>Kunden-Nr.:</strong> {html.escape(kunden_nr)}&nbsp;&nbsp;&nbsp;
            <strong>Fachberater:</strong> {html.escape(fachberater)}&nbsp;&nbsp;&nbsp;
            <strong>Stand:</strong> {html.escape(stand)}
        </div>

        <!-- ===== TOUR-ÜBERSICHT ===== -->
        {tour_overview_html}

        <!-- ===== PLANTABELLE ===== -->
        {plan_table_html}

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
    <aside class="sidebar" id="sidebar">
        <div class="sidebar-logo">&#128230; Sendeplan</div>

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
        </div>

        <div class="sidebar-subtitle-group">
            <div class="sidebar-label">Untertitel global \u00e4ndern</div>
            <input id="global-subtitle-input" type="text"
                placeholder="z.B. Standart, NMS \u2026"
                autocomplete="off" spellcheck="false" />
            <div class="search-nav-row" style="margin-top:6px;">
                <button type="button" class="search-btn" id="btn-apply-subtitle">&#10003; Alle setzen</button>
            </div>
        </div>

        <button type="button" class="sidebar-print-btn" onclick="window.print()">&#128438; Drucken</button>
    </aside>
    """


def build_full_document_html(customers: pd.DataFrame, plan_rows: pd.DataFrame, include_separators: bool = True, logo_b64: str = "", logo_mime: str = "image/png") -> str:
    # Logo-Base64 einmalig im <head> einbetten, nicht pro Seite wiederholen
    if logo_b64:
        logo_head_script = (
            f'<script>'  
            f'window.__LOGO_SRC="data:{logo_mime};base64,{logo_b64}";'  
            f'document.addEventListener("DOMContentLoaded",function(){{'  
            f'document.querySelectorAll(".doc-logo-img").forEach(function(img){{img.src=window.__LOGO_SRC;}});'  
            f'}});</script>'
        )
    else:
        logo_head_script = ""
    docs: List[str] = []

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

        entry_parts: List[str] = [render_customer_plan(customer, rows, logo_b64=logo_b64, logo_mime=logo_mime)]

        csb_search = " ".join([part for part in [csb_nr, *csb_touren] if part]).lower()
        docs.append(
            (
                f'<section class="customer-entry" '
                f'data-sap="{html.escape(sap.lower())}" '
                f'data-csb="{html.escape(csb_search)}" '
                f'data-search="{html.escape(search_blob)}">'
                f'data-kategorie="{html.escape(normalize_text(customer.get("Kategorie", "")))}" '
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
            var counts = { alle: 0, MK: 0, Malchow: 0, NMS: 0, SuL: 0, Direkt: 0 };
            allEntries.forEach(function (e) {
                var kat = e.getAttribute("data-kategorie") || "";
                var blob = norm(e.getAttribute("data-search") || "");
                var matchesSearch = !q || blob.indexOf(q) !== -1;
                if (matchesSearch) {
                    counts.alle++;
                    if (counts[kat] !== undefined) counts[kat]++;
                }
            });
            var map = { alle: "cnt-alle", MK: "cnt-mk", Malchow: "cnt-malchow", NMS: "cnt-nms", SuL: "cnt-sul" };
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

        function applyFilter() {
            var q = norm(document.getElementById("search-input").value);
            clearHighlights();
            matches = [];
            cursor  = -1;

            allEntries.forEach(function (entry) {
                var kat  = entry.getAttribute("data-kategorie") || "";
                var blob = norm(entry.getAttribute("data-search") || "");
                var katOk  = activeKat === "alle" || kat === activeKat;
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

            document.getElementById("search-input").addEventListener("input", applyFilter);
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
        });
    })();
    </script>
    """

    return f"""
    <!DOCTYPE html>
    <html lang="de">
    <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=210mm, initial-scale=1.0" />
        <title>Sendeplan-Export</title>
        {export_css()}
        {logo_head_script}
    </head>
    <body>
        {render_export_search_toolbar()}
        <div class="main-content">
        <div class="page-stack">
        {''.join(docs)}
        </div>
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
        <meta name="viewport" content="width=210mm, initial-scale=1.0" />
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

    with st.sidebar:
        st.title("Sendeplan")
        st.caption("Uploads, Filter und Export")
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

        st.divider()
        logo_file = st.file_uploader(
            "Logo (optional)",
            type=["png", "jpg", "jpeg", "svg", "gif", "webp"],
            help="Wird oben rechts auf jedem Sendeplan angezeigt (PNG/JPG empfohlen)",
        )

        upload_map = {
            "kunden": kunden_file,
            "sap": sap_file,
            "transport": transport_file,
            "kisoft": kisoft_file,
            "kostenstellen": kostenstellen_file,
        }

    if not all_required_uploads_present(upload_map):
        show_onboarding(upload_map)
        return

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
        render_panel("Hinweis", upload_status_lines(upload_map))
        return

    with st.sidebar:
        st.divider()
        st.subheader("Filter")
        st.text_input(
            "Suche nach SAP, CSB oder Name",
            key="search_text",
            placeholder="zum Beispiel 211393 oder Kunde",
        )

        category = st.radio(
            "Kategorie",
            options=KATEGORIEN,
            index=KATEGORIEN.index(st.session_state.category_filter) if st.session_state.category_filter in KATEGORIEN else 0,
            horizontal=False,
        )
        st.session_state.category_filter = category

        filtered_customers = filter_customers(customers_df, st.session_state.category_filter, st.session_state.search_text)
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

    filtered_customers = filter_customers(customers_df, st.session_state.category_filter, st.session_state.search_text)

    overview_tab, preview_tab, export_tab = st.tabs(["Übersicht", "Kundenvorschau", "Export"])

    with overview_tab:
        st.markdown(
            """
            <div class="hero-card">
                <h3 style="margin:0;">Daten erfolgreich geladen</h3>
                <p style="margin:0.5rem 0 0 0;">Nutze links die Suche und den Kategoriefilter. Rechts in den Tabs findest du Übersicht, Vorschau und HTML-Export.</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

        metric_cols = st.columns(4)
        metric_cols[0].metric("Kunden gesamt", len(customers_df))
        metric_cols[1].metric("Planzeilen gesamt", len(plan_rows_df))
        metric_cols[2].metric("Treffer im Filter", len(filtered_customers))
        metric_cols[3].metric("Aktive Kategorie", st.session_state.category_filter)

        left, right = st.columns([3, 2], gap="large")
        with left:
            st.subheader("Kunden nach Kategorie")
            cat_df = pd.DataFrame(
                {"Kategorie": list(counts.keys()), "Anzahl": list(counts.values())}
            )
            st.dataframe(cat_df, use_container_width=True, hide_index=True)

            st.subheader("Erste Treffer im aktuellen Filter")
            preview_customers = filtered_customers[["SAP_Nr", "CSB_Nr", "Name", "Ort", "Kategorie"]].head(15)
            st.dataframe(preview_customers, use_container_width=True, hide_index=True)

        with right:
            render_panel(
                "Aktive Dateien",
                upload_status_lines(upload_map),
            )
            render_panel(
                "Verarbeitungsregeln",
                """
                <ul style="margin:0; padding-left:1.1rem; line-height:1.7;">
                    <li>Starres Spalten-Mapping ohne automatische Erkennung</li>
                    <li>Kisoft-Mapping über <strong>00 + erste 8 Stellen</strong> aus Rahmentour</li>
                    <li>Dubletten in SAP werden nach SAP, Bestelltag und Liefertyp entfernt</li>
                    <li>HTML-Export ist eigenständig und im Browser suchbar</li>
                </ul>
                """,
            )

    with preview_tab:
        if filtered_customers.empty or not st.session_state.selected_sap:
            st.warning("Im aktuellen Filter wurde kein Kunde gefunden.")
        else:
            selected_customer = filtered_customers[filtered_customers["SAP_Nr"] == st.session_state.selected_sap].iloc[0]
            customer_rows = plan_rows_df[plan_rows_df["SAP_Nr"] == st.session_state.selected_sap].copy()
            show_customer_preview(selected_customer, customer_rows)

    with export_tab:
        st.subheader("HTML-Export")
        st.write(
            "Die exportierte HTML-Datei enthält alle Daten direkt im Code. Sie kann ohne Streamlit geöffnet, durchsucht und gedruckt werden."
        )

        if filtered_customers.empty:
            st.warning("Es gibt im aktuellen Filter keine Daten für einen HTML-Export.")
        else:
            logo_b64 = ""
            logo_mime = "image/png"
            if logo_file is not None:
                raw = logo_file.getvalue()
                logo_b64 = base64.b64encode(raw).decode("utf-8")
                ext = logo_file.name.rsplit(".", 1)[-1].lower()
                logo_mime = {"jpg": "image/jpeg", "jpeg": "image/jpeg",
                             "png": "image/png", "svg": "image/svg+xml",
                             "gif": "image/gif", "webp": "image/webp"}.get(ext, "image/png")
            bulk_html = build_full_document_html(filtered_customers, plan_rows_df,
                            logo_b64=logo_b64, logo_mime=logo_mime)
            filename_suffix = normalize_text(st.session_state.category_filter).lower() or "alle"

            col1, col2 = st.columns(2)
            with col1:
                st.download_button(
                    label="Gefilterten Gesamtplan als Standalone-HTML herunterladen",
                    data=bulk_html,
                    file_name=f"sendeplan_{filename_suffix}.html",
                    mime="text/html",
                    use_container_width=True,
                )
            with col2:
                if st.session_state.selected_sap:
                    selected_customer = filtered_customers[filtered_customers["SAP_Nr"] == st.session_state.selected_sap].iloc[0]
                    customer_rows = plan_rows_df[plan_rows_df["SAP_Nr"] == st.session_state.selected_sap].copy()
                    single_html = build_single_document_html(selected_customer, customer_rows, logo_b64=logo_b64, logo_mime=logo_mime)
                    st.download_button(
                        label="Aktuellen Kunden als HTML herunterladen",
                        data=single_html,
                        file_name=f"sendeplan_{normalize_text(selected_customer['SAP_Nr'])}.html",
                        mime="text/html",
                        use_container_width=True,
                    )

            st.info("Nach dem Download die HTML-Datei im Browser öffnen. Dort kannst du direkt nach SAP- oder CSB-Nummer suchen und anschließend drucken.")


if __name__ == "__main__":
    main()
