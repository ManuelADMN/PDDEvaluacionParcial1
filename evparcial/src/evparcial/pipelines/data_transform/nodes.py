from __future__ import annotations

import numpy as np
import pandas as pd

from evparcial.utils import min_max_scale


def build_dataset_universitario(
    estudiantes_clean: pd.DataFrame,
    inscripciones_clean: pd.DataFrame,
    calificaciones_clean: pd.DataFrame,
    asistencia_clean: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    calificaciones_grouped = calificaciones_clean.groupby("id_inscripcion", dropna=False)
    calificaciones_agg = calificaciones_grouped.agg(
        nota_promedio=("nota", "mean"),
        nota_minima=("nota", "min"),
        nota_maxima=("nota", "max"),
        cantidad_evaluaciones=("id_calificacion", "nunique"),
        ponderacion_total=("ponderacion", "sum"),
    ).reset_index()

    weighted = calificaciones_grouped.apply(
        lambda g: np.average(g["nota"], weights=g["ponderacion"])
    )
    calificaciones_agg["nota_final_ponderada"] = (
        weighted.reset_index(drop=True).round(2)
    )

    asistencia_flags = asistencia_clean.assign(
        presente=(asistencia_clean["estado_asistencia"] == "Presente").astype(int),
        ausente=(asistencia_clean["estado_asistencia"] == "Ausente").astype(int),
        justificado=(asistencia_clean["estado_asistencia"] == "Justificado").astype(int),
        tardanza=(asistencia_clean["estado_asistencia"] == "Tardanza").astype(int),
    )

    asistencia_agg = (
        asistencia_flags.groupby("id_inscripcion", dropna=False)
        .agg(
            total_clases=("id_asistencia", "nunique"),
            presentes=("presente", "sum"),
            ausentes=("ausente", "sum"),
            justificados=("justificado", "sum"),
            tardanzas=("tardanza", "sum"),
        )
        .reset_index()
    )
    asistencia_agg["tasa_asistencia"] = (
        (asistencia_agg["presentes"] + asistencia_agg["justificados"])
        / asistencia_agg["total_clases"]
    ).round(4)

    carga_asignaturas = (
        inscripciones_clean.groupby(["id_estudiante", "anio", "semestre"], dropna=False)
        .size()
        .rename("carga_asignaturas")
        .reset_index()
    )

    dataset = (
        inscripciones_clean.merge(
            estudiantes_clean, on="id_estudiante", how="inner"
        )
        .merge(calificaciones_agg, on="id_inscripcion", how="left")
        .merge(asistencia_agg, on="id_inscripcion", how="left")
        .merge(
            carga_asignaturas,
            on=["id_estudiante", "anio", "semestre"],
            how="left",
        )
    )

    fill_zero_columns = [
        "nota_promedio",
        "nota_minima",
        "nota_maxima",
        "nota_final_ponderada",
        "cantidad_evaluaciones",
        "ponderacion_total",
        "total_clases",
        "presentes",
        "ausentes",
        "justificados",
        "tardanzas",
        "tasa_asistencia",
        "carga_asignaturas",
    ]
    for column in fill_zero_columns:
        dataset[column] = dataset[column].fillna(0)

    dataset["antiguedad_ingreso"] = (
        dataset["anio"] - dataset["anio_ingreso"]
    ).clip(lower=0)

    dataset["resultado_academico"] = pd.cut(
        dataset["nota_final_ponderada"],
        bins=[-np.inf, 3.9, 4.9, 5.9, np.inf],
        labels=["Critico", "En Riesgo", "Aceptable", "Sobresaliente"],
    ).astype("string")

    dataset["riesgo_academico"] = np.select(
        [
            (dataset["nota_final_ponderada"] < 4.0)
            | (dataset["tasa_asistencia"] < 0.70),
            (
                (dataset["nota_final_ponderada"] >= 4.0)
                & (dataset["nota_final_ponderada"] < 5.0)
            )
            | (
                (dataset["tasa_asistencia"] >= 0.70)
                & (dataset["tasa_asistencia"] < 0.85)
            ),
        ],
        ["Alto", "Medio"],
        default="Bajo",
    )

    dataset["aprobado"] = (dataset["nota_final_ponderada"] >= 4.0).astype(int)

    dataset = dataset.sort_values(
        ["anio", "semestre", "id_estudiante", "id_inscripcion"]
    ).reset_index(drop=True)

    resumen_asignaturas = (
        dataset.groupby(["anio", "semestre", "nombre_asignatura"], dropna=False)
        .agg(
            estudiantes_inscritos=("id_estudiante", "nunique"),
            nota_final_promedio=("nota_final_ponderada", "mean"),
            asistencia_promedio=("tasa_asistencia", "mean"),
            tasa_aprobacion=("aprobado", "mean"),
        )
        .reset_index()
        .sort_values(["anio", "semestre", "tasa_aprobacion"], ascending=[True, True, False])
    )
    resumen_asignaturas["nota_final_promedio"] = resumen_asignaturas[
        "nota_final_promedio"
    ].round(2)
    resumen_asignaturas["asistencia_promedio"] = resumen_asignaturas[
        "asistencia_promedio"
    ].round(4)
    resumen_asignaturas["tasa_aprobacion"] = resumen_asignaturas[
        "tasa_aprobacion"
    ].round(4)

    return dataset, resumen_asignaturas


def build_dataset_modelo(
    dataset_universitario: pd.DataFrame,
    numeric_columns_to_scale: list[str],
    categorical_columns_to_encode: list[str],
) -> pd.DataFrame:
    base = dataset_universitario.copy()

    scaled = min_max_scale(base, numeric_columns_to_scale)

    encoded = pd.get_dummies(
        scaled,
        columns=categorical_columns_to_encode,
        prefix=categorical_columns_to_encode,
        dtype=int,
    )

    return encoded