from __future__ import annotations

import hashlib
import re
import unicodedata
from typing import Any

import pandas as pd


def _clean_text_value(value: Any) -> Any:
    """Limpia texto problemático, elimina acentos y deja ASCII estable."""
    if pd.isna(value):
        return value

    text = str(value)
    text = text.replace("�", "")
    text = text.replace("\ufeff", "")
    text = " ".join(text.split())

    text = (
        unicodedata.normalize("NFKD", text)
        .encode("ascii", "ignore")
        .decode("ascii")
    )

    return text.strip()


def normalize_column_name(name: Any) -> str:
    """Normaliza nombres de columnas para evitar problemas de tildes/codificacion."""
    text = str(name)
    text = text.replace("�", "")
    text = text.replace("\ufeff", "")
    text = text.strip()

    text = (
        unicodedata.normalize("NFKD", text)
        .encode("ascii", "ignore")
        .decode("ascii")
    )

    text = text.lower().strip()
    text = re.sub(r"[^\w]+", "_", text)
    text = re.sub(r"_+", "_", text)
    text = text.strip("_")

    alias_map = {
        "ano_ingreso": "anio_ingreso",
        "aao_ingreso": "anio_ingreso",
        "ao_ingreso": "anio_ingreso",
        "ingreso_ano": "anio_ingreso",
        "ano": "anio",
        "aao": "anio",
        "ao": "anio",
    }

    return alias_map.get(text, text)


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Devuelve un dataframe con nombres de columnas normalizados."""
    out = df.copy()
    out.columns = [normalize_column_name(col) for col in out.columns]
    return out


def normalize_spaces(series: pd.Series) -> pd.Series:
    """Limpia espacios y deja texto estable."""
    return series.astype("string").map(_clean_text_value)


def to_title_case(series: pd.Series) -> pd.Series:
    """Normaliza texto a Title Case sin acentos."""
    return normalize_spaces(series).str.lower().str.title()


def to_upper(series: pd.Series) -> pd.Series:
    """Normaliza texto a mayusculas sin acentos."""
    return normalize_spaces(series).str.upper()


def to_lower(series: pd.Series) -> pd.Series:
    """Normaliza texto a minusculas sin acentos."""
    return normalize_spaces(series).str.lower()


def parse_mixed_date(series: pd.Series) -> pd.Series:
    """Convierte fechas en formatos DD/MM/YYYY y YYYY-MM-DD."""
    clean = normalize_spaces(series)
    ddmmyyyy = pd.to_datetime(clean, format="%d/%m/%Y", errors="coerce")
    yyyymmdd = pd.to_datetime(clean, format="%Y-%m-%d", errors="coerce")
    return ddmmyyyy.fillna(yyyymmdd)


def extract_numeric(series: pd.Series) -> pd.Series:
    """Extrae el primer numero valido desde strings mixtos."""
    clean = normalize_spaces(series).str.replace(",", ".", regex=False)
    extracted = clean.str.extract(r"(-?\d+(?:\.\d+)?)", expand=False)
    return pd.to_numeric(extracted, errors="coerce")


def dataframe_checksum(df: pd.DataFrame) -> str:
    """Genera una huella SHA256 del contenido del DataFrame."""
    safe = df.copy()
    for column in safe.columns:
        if str(safe[column].dtype).startswith("datetime64"):
            safe[column] = safe[column].astype("string")
    hashed = pd.util.hash_pandas_object(safe.fillna("__NA__"), index=True)
    return hashlib.sha256(hashed.values.tobytes()).hexdigest()


def profile_dataframe(df: pd.DataFrame, dataset_name: str) -> pd.DataFrame:
    """Devuelve un perfil resumido del dataset."""
    rows = []
    for column in df.columns:
        dtype = str(df[column].dtype)
        nulls = int(df[column].isna().sum())
        uniques = int(df[column].nunique(dropna=True))
        sample = (
            ", ".join(df[column].dropna().astype(str).head(3).tolist())
            if not df[column].dropna().empty
            else ""
        )
        rows.append(
            {
                "dataset": dataset_name,
                "columna": column,
                "dtype": dtype,
                "nulos": nulls,
                "porcentaje_nulos": round((nulls / max(len(df), 1)) * 100, 2),
                "valores_unicos": uniques,
                "muestra": sample,
            }
        )
    return pd.DataFrame(rows)


def summary_row(df: pd.DataFrame, dataset_name: str) -> dict[str, Any]:
    """Construye una fila resumen con metricas globales."""
    return {
        "dataset": dataset_name,
        "filas": int(df.shape[0]),
        "columnas": int(df.shape[1]),
        "nulos_totales": int(df.isna().sum().sum()),
        "duplicados_exactos": int(df.duplicated().sum()),
        "columnas_con_nulos": int((df.isna().sum() > 0).sum()),
        "checksum": dataframe_checksum(df),
    }


def normalize_carrera(series: pd.Series) -> pd.Series:
    normalized = to_title_case(series)
    mapping = {
        "Ing Informatica": "Ingenieria Informatica",
        "Ingenieria Informatica": "Ingenieria Informatica",
        "Ing Civil": "Ingenieria Civil",
        "Ingenieria Civil": "Ingenieria Civil",
    }
    return normalized.replace(mapping)


def normalize_asignatura(series: pd.Series) -> pd.Series:
    normalized = to_title_case(series)
    mapping = {
        "Fisica": "Fisica",
        "Quimica": "Quimica",
        "Etica": "Etica",
        "Calculo I": "Calculo I",
        "Ingles": "Ingles",
        "Estadistica": "Estadistica",
    }
    return normalized.replace(mapping)


def normalize_estado_matricula(series: pd.Series) -> pd.Series:
    normalized = to_title_case(series)
    mapping = {
        "Regular": "Regular",
        "Egresado": "Egresado",
        "Congelada": "Congelada",
        "Desertor": "Desertor",
    }
    return normalized.replace(mapping)


def min_max_scale(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Escala columnas numericas al rango [0, 1]."""
    out = df.copy()
    for column in columns:
        if column not in out.columns:
            continue

        col_min = out[column].min()
        col_max = out[column].max()
        if pd.isna(col_min) or pd.isna(col_max) or col_min == col_max:
            out[f"{column}_scaled"] = 0.0
        else:
            out[f"{column}_scaled"] = (
                (out[column] - col_min) / (col_max - col_min)
            ).round(4)
    return out