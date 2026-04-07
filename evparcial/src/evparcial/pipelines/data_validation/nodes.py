from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from evparcial.utils import dataframe_checksum, summary_row


def build_audit_report(
    estudiantes_raw: pd.DataFrame,
    inscripciones_raw: pd.DataFrame,
    calificaciones_raw: pd.DataFrame,
    asistencia_raw: pd.DataFrame,
    estudiantes_clean: pd.DataFrame,
    inscripciones_clean: pd.DataFrame,
    calificaciones_clean: pd.DataFrame,
    asistencia_clean: pd.DataFrame,
    dataset_universitario: pd.DataFrame,
    dataset_universitario_modelo: pd.DataFrame,
) -> pd.DataFrame:
    """Construye auditoría con métricas globales y checksums por dataset."""
    datasets = {
        "estudiantes_raw": estudiantes_raw,
        "inscripciones_raw": inscripciones_raw,
        "calificaciones_raw": calificaciones_raw,
        "asistencia_raw": asistencia_raw,
        "estudiantes_clean": estudiantes_clean,
        "inscripciones_clean": inscripciones_clean,
        "calificaciones_clean": calificaciones_clean,
        "asistencia_clean": asistencia_clean,
        "dataset_universitario": dataset_universitario,
        "dataset_universitario_modelo": dataset_universitario_modelo,
    }
    rows = [summary_row(df, name) for name, df in datasets.items()]
    return pd.DataFrame(rows)


def validate_final_outputs(
    estudiantes_clean: pd.DataFrame,
    inscripciones_clean: pd.DataFrame,
    calificaciones_clean: pd.DataFrame,
    asistencia_clean: pd.DataFrame,
    dataset_universitario: pd.DataFrame,
    expected_final_schema: dict,
) -> dict:
    """Valida integridad, esquema, nulos y duplicados del dataset final."""
    actual_schema = {column: str(dtype) for column, dtype in dataset_universitario.dtypes.items()}
    schema_matches = {
        column: {
            "esperado": expected_final_schema.get(column),
            "real": actual_schema.get(column),
            "coincide": expected_final_schema.get(column) == actual_schema.get(column),
        }
        for column in expected_final_schema
    }

    integrity = {
        "inscripciones_sin_estudiante": int(
            (~inscripciones_clean["id_estudiante"].isin(estudiantes_clean["id_estudiante"])).sum()
        ),
        "calificaciones_sin_inscripcion": int(
            (~calificaciones_clean["id_inscripcion"].isin(inscripciones_clean["id_inscripcion"])).sum()
        ),
        "asistencia_sin_inscripcion": int(
            (~asistencia_clean["id_inscripcion"].isin(inscripciones_clean["id_inscripcion"])).sum()
        ),
        "dataset_ids_inscripcion_duplicados": int(
            dataset_universitario["id_inscripcion"].duplicated().sum()
        ),
        "dataset_nulos_totales": int(dataset_universitario.isna().sum().sum()),
    }

    return {
        "row_counts": {
            "estudiantes_clean": int(len(estudiantes_clean)),
            "inscripciones_clean": int(len(inscripciones_clean)),
            "calificaciones_clean": int(len(calificaciones_clean)),
            "asistencia_clean": int(len(asistencia_clean)),
            "dataset_universitario": int(len(dataset_universitario)),
        },
        "checksums": {
            "dataset_universitario": dataframe_checksum(dataset_universitario),
        },
        "schema_validation": schema_matches,
        "integrity_validation": integrity,
    }


def generate_visual_reports(
    comparacion_limpieza: pd.DataFrame,
    dataset_universitario: pd.DataFrame,
    report_paths: dict,
) -> None:
    """Genera gráficos finales para el notebook, informe y entrega."""
    for path in report_paths.values():
        Path(path).parent.mkdir(parents=True, exist_ok=True)

    # Gráfico 1: Nulos antes y después
    chart_df = comparacion_limpieza[["dataset", "nulos_raw", "nulos_clean"]].copy()
    x = range(len(chart_df))
    width = 0.35

    plt.figure(figsize=(10, 6))
    plt.bar([i - width / 2 for i in x], chart_df["nulos_raw"], width=width, label="Antes")
    plt.bar([i + width / 2 for i in x], chart_df["nulos_clean"], width=width, label="Después")
    plt.xticks(list(x), chart_df["dataset"], rotation=0)
    plt.ylabel("Cantidad de nulos")
    plt.title("Comparación de nulos antes y después de la limpieza")
    plt.legend()
    plt.tight_layout()
    plt.savefig(report_paths["nulos_antes_despues"], dpi=200, bbox_inches="tight")
    plt.close()

    # Gráfico 2: Distribución de notas finales
    plt.figure(figsize=(10, 6))
    plt.hist(dataset_universitario["nota_final_ponderada"], bins=12)
    plt.xlabel("Nota final ponderada")
    plt.ylabel("Frecuencia")
    plt.title("Distribución de la nota final ponderada")
    plt.tight_layout()
    plt.savefig(report_paths["distribucion_notas_finales"], dpi=200, bbox_inches="tight")
    plt.close()

    # Gráfico 3: Asistencia promedio por carrera
    asistencia_carrera = (
        dataset_universitario.groupby("carrera", dropna=False)["tasa_asistencia"]
        .mean()
        .sort_values(ascending=False)
        .head(10)
    )
    plt.figure(figsize=(12, 6))
    plt.bar(asistencia_carrera.index.astype(str), asistencia_carrera.values)
    plt.xticks(rotation=30, ha="right")
    plt.ylabel("Tasa de asistencia promedio")
    plt.title("Top 10 carreras por asistencia promedio")
    plt.tight_layout()
    plt.savefig(report_paths["asistencia_por_carrera"], dpi=200, bbox_inches="tight")
    plt.close()
