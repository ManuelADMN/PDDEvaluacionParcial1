from __future__ import annotations

import pandas as pd

from evparcial.utils import (
    extract_numeric,
    normalize_asignatura,
    normalize_columns,
    normalize_estado_matricula,
    normalize_carrera,
    parse_mixed_date,
    to_lower,
    to_title_case,
    to_upper,
)


def _pick_first_existing_column(df: pd.DataFrame, candidates: list[str], target_name: str) -> pd.DataFrame:
    """Renombra la primera columna existente de candidates hacia target_name."""
    out = df.copy()
    for col in candidates:
        if col in out.columns:
            if col != target_name:
                out = out.rename(columns={col: target_name})
            return out
    return out


def _validate_required_columns(df: pd.DataFrame, required_cols: list[str], dataset_name: str) -> None:
    """Valida columnas obligatorias y entrega error útil."""
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise KeyError(
            f"Faltan columnas en {dataset_name} tras normalizar: {missing}. "
            f"Columnas disponibles: {list(df.columns)}"
        )


def clean_estudiantes(
    df: pd.DataFrame,
    categorical_fill_values: dict,
    valid_years: dict,
) -> pd.DataFrame:
    """Limpia tabla de estudiantes."""
    out = normalize_columns(df)

    # Alias defensivos por problemas de encoding / tildes / variaciones de nombre
    out = _pick_first_existing_column(
        out,
        ["anio_ingreso", "ano_ingreso", "aao_ingreso", "ao_ingreso", "ingreso_ano"],
        "anio_ingreso",
    )

    _validate_required_columns(
        out,
        [
            "id_estudiante",
            "nombre",
            "rut",
            "carrera",
            "sede",
            "anio_ingreso",
            "email",
            "estado_matricula",
        ],
        "estudiantes",
    )

    out["id_estudiante"] = pd.to_numeric(out["id_estudiante"], errors="coerce")
    out["anio_ingreso"] = pd.to_numeric(out["anio_ingreso"], errors="coerce")

    out["nombre"] = to_title_case(out["nombre"])
    out["rut"] = to_upper(out["rut"])
    out["carrera"] = normalize_carrera(out["carrera"])
    out["sede"] = to_title_case(out["sede"])
    out["email"] = to_lower(out["email"])
    out["estado_matricula"] = normalize_estado_matricula(out["estado_matricula"])

    out = out.dropna(subset=["id_estudiante"]).drop_duplicates()
    out = out.sort_values("id_estudiante").drop_duplicates(
        subset=["id_estudiante"], keep="first"
    )

    out["anio_ingreso"] = (
        out["anio_ingreso"]
        .fillna(out["anio_ingreso"].median())
        .clip(valid_years["min_ingreso"], valid_years["max_ingreso"])
        .round()
        .astype("Int64")
    )

    for column in ["carrera", "sede", "estado_matricula"]:
        if out[column].notna().any():
            out[column] = out[column].fillna(out[column].mode().iloc[0])

    out["nombre"] = out["nombre"].fillna(categorical_fill_values["nombre"])
    out["rut"] = out["rut"].fillna(categorical_fill_values["rut"])
    out["email"] = out["email"].fillna(
        out["id_estudiante"]
        .astype("Int64")
        .astype(str)
        .radd("sin_email_")
        .add("@correo.local")
    )

    out["id_estudiante"] = out["id_estudiante"].astype("Int64")
    return out


def clean_inscripciones(
    df: pd.DataFrame,
    categorical_fill_values: dict,
    semestre_range: dict,
    valid_years: dict,
) -> pd.DataFrame:
    """Limpia tabla de inscripciones."""
    out = normalize_columns(df)

    out = _pick_first_existing_column(
        out,
        ["anio", "ano", "aao", "ao"],
        "anio",
    )

    _validate_required_columns(
        out,
        [
            "id_inscripcion",
            "id_estudiante",
            "codigo_asignatura",
            "nombre_asignatura",
            "seccion",
            "semestre",
            "anio",
            "docente",
        ],
        "inscripciones",
    )

    out["id_inscripcion"] = pd.to_numeric(out["id_inscripcion"], errors="coerce")
    out["id_estudiante"] = pd.to_numeric(out["id_estudiante"], errors="coerce")
    out["semestre"] = pd.to_numeric(out["semestre"], errors="coerce")
    out["anio"] = pd.to_numeric(out["anio"], errors="coerce")

    out["codigo_asignatura"] = to_upper(out["codigo_asignatura"])
    out["nombre_asignatura"] = normalize_asignatura(out["nombre_asignatura"])
    out["seccion"] = to_upper(out["seccion"])
    out["docente"] = to_title_case(out["docente"])

    out = out.dropna(subset=["id_inscripcion", "id_estudiante"]).drop_duplicates()
    out = out.sort_values("id_inscripcion").drop_duplicates(
        subset=["id_inscripcion"], keep="first"
    )

    out["codigo_asignatura"] = out["codigo_asignatura"].fillna(
        categorical_fill_values["codigo_asignatura"]
    )
    if out["nombre_asignatura"].notna().any():
        out["nombre_asignatura"] = out["nombre_asignatura"].fillna(
            out["nombre_asignatura"].mode().iloc[0]
        )
    if out["seccion"].notna().any():
        out["seccion"] = out["seccion"].fillna(out["seccion"].mode().iloc[0])

    out["semestre"] = (
        out["semestre"]
        .fillna(out["semestre"].mode().iloc[0] if out["semestre"].notna().any() else 1)
        .clip(semestre_range["min"], semestre_range["max"])
        .round()
        .astype("Int64")
    )
    out["anio"] = (
        out["anio"]
        .fillna(out["anio"].mode().iloc[0] if out["anio"].notna().any() else valid_years["min_periodo"])
        .clip(valid_years["min_periodo"], valid_years["max_periodo"])
        .round()
        .astype("Int64")
    )
    out["docente"] = out["docente"].fillna(categorical_fill_values["docente"])

    out["id_inscripcion"] = out["id_inscripcion"].astype("Int64")
    out["id_estudiante"] = out["id_estudiante"].astype("Int64")
    return out


def clean_calificaciones(
    df: pd.DataFrame,
    grade_range: dict,
) -> pd.DataFrame:
    """Limpia tabla de calificaciones."""
    out = normalize_columns(df)

    _validate_required_columns(
        out,
        [
            "id_calificacion",
            "id_inscripcion",
            "numero_evaluacion",
            "nota",
            "ponderacion",
            "fecha",
            "tipo_evaluacion",
        ],
        "calificaciones",
    )

    out["id_calificacion"] = pd.to_numeric(out["id_calificacion"], errors="coerce")
    out["id_inscripcion"] = pd.to_numeric(out["id_inscripcion"], errors="coerce")
    out["numero_evaluacion"] = pd.to_numeric(
        out["numero_evaluacion"], errors="coerce"
    )
    out["nota"] = extract_numeric(out["nota"])
    out["ponderacion"] = pd.to_numeric(out["ponderacion"], errors="coerce")
    out["fecha"] = parse_mixed_date(out["fecha"])
    out["tipo_evaluacion"] = to_title_case(out["tipo_evaluacion"])

    out = out.dropna(subset=["id_calificacion", "id_inscripcion"]).drop_duplicates()
    out = out.sort_values("id_calificacion").drop_duplicates(
        subset=["id_calificacion"], keep="first"
    )

    if out["tipo_evaluacion"].notna().any():
        out["tipo_evaluacion"] = out["tipo_evaluacion"].fillna(
            out["tipo_evaluacion"].mode().iloc[0]
        )
    out["numero_evaluacion"] = (
        out["numero_evaluacion"]
        .fillna(
            out["numero_evaluacion"].median()
            if out["numero_evaluacion"].notna().any()
            else 1
        )
        .clip(1, 4)
        .round()
        .astype("Int64")
    )
    out["nota"] = (
        out["nota"]
        .fillna(out["nota"].median() if out["nota"].notna().any() else 4.0)
        .clip(grade_range["min"], grade_range["max"])
        .round(1)
    )
    out["ponderacion"] = (
        out["ponderacion"]
        .fillna(out["ponderacion"].median() if out["ponderacion"].notna().any() else 25)
        .clip(10, 40)
    )
    if out["fecha"].notna().any():
        out["fecha"] = out["fecha"].fillna(out["fecha"].mode().iloc[0])

    out["id_calificacion"] = out["id_calificacion"].astype("Int64")
    out["id_inscripcion"] = out["id_inscripcion"].astype("Int64")
    return out


def clean_asistencia(df: pd.DataFrame) -> pd.DataFrame:
    """Limpia tabla de asistencia."""
    out = normalize_columns(df)

    _validate_required_columns(
        out,
        [
            "id_asistencia",
            "id_inscripcion",
            "fecha",
            "estado_asistencia",
            "tipo_clase",
        ],
        "asistencia",
    )

    out["id_asistencia"] = pd.to_numeric(out["id_asistencia"], errors="coerce")
    out["id_inscripcion"] = pd.to_numeric(out["id_inscripcion"], errors="coerce")
    out["fecha"] = parse_mixed_date(out["fecha"])
    out["estado_asistencia"] = to_title_case(out["estado_asistencia"])
    out["tipo_clase"] = to_title_case(out["tipo_clase"])

    out = out.dropna(subset=["id_asistencia", "id_inscripcion"]).drop_duplicates()
    out = out.sort_values("id_asistencia").drop_duplicates(
        subset=["id_asistencia"], keep="first"
    )

    if out["fecha"].notna().any():
        out["fecha"] = out["fecha"].fillna(out["fecha"].mode().iloc[0])
    if out["estado_asistencia"].notna().any():
        out["estado_asistencia"] = out["estado_asistencia"].fillna(
            out["estado_asistencia"].mode().iloc[0]
        )
    if out["tipo_clase"].notna().any():
        out["tipo_clase"] = out["tipo_clase"].fillna(out["tipo_clase"].mode().iloc[0])

    out["id_asistencia"] = out["id_asistencia"].astype("Int64")
    out["id_inscripcion"] = out["id_inscripcion"].astype("Int64")
    return out


def compare_before_after_cleaning(
    estudiantes_raw: pd.DataFrame,
    inscripciones_raw: pd.DataFrame,
    calificaciones_raw: pd.DataFrame,
    asistencia_raw: pd.DataFrame,
    estudiantes_clean: pd.DataFrame,
    inscripciones_clean: pd.DataFrame,
    calificaciones_clean: pd.DataFrame,
    asistencia_clean: pd.DataFrame,
) -> pd.DataFrame:
    """Compara métricas antes y después de la limpieza."""
    pairs = [
        ("estudiantes", estudiantes_raw, estudiantes_clean),
        ("inscripciones", inscripciones_raw, inscripciones_clean),
        ("calificaciones", calificaciones_raw, calificaciones_clean),
        ("asistencia", asistencia_raw, asistencia_clean),
    ]

    rows = []
    for name, raw_df, clean_df in pairs:
        rows.append(
            {
                "dataset": name,
                "filas_raw": int(len(raw_df)),
                "filas_clean": int(len(clean_df)),
                "nulos_raw": int(raw_df.isna().sum().sum()),
                "nulos_clean": int(clean_df.isna().sum().sum()),
                "duplicados_raw": int(raw_df.duplicated().sum()),
                "duplicados_clean": int(clean_df.duplicated().sum()),
                "filas_eliminadas": int(len(raw_df) - len(clean_df)),
            }
        )
    return pd.DataFrame(rows)