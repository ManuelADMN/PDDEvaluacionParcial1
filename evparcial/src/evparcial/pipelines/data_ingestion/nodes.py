from __future__ import annotations

import pandas as pd

from evparcial.utils import profile_dataframe, summary_row


def build_initial_diagnostic(
    estudiantes_raw: pd.DataFrame,
    inscripciones_raw: pd.DataFrame,
    calificaciones_raw: pd.DataFrame,
    asistencia_raw: pd.DataFrame,
) -> pd.DataFrame:
    """Genera un diagnóstico inicial combinando resumen global y perfil por columna."""
    datasets = {
        "estudiantes_raw": estudiantes_raw,
        "inscripciones_raw": inscripciones_raw,
        "calificaciones_raw": calificaciones_raw,
        "asistencia_raw": asistencia_raw,
    }

    summary_rows = [summary_row(df, name) for name, df in datasets.items()]
    summary_df = pd.DataFrame(summary_rows)
    summary_df["tipo_registro"] = "resumen"

    profile_frames = [
        profile_dataframe(df, name).assign(
            filas=pd.NA,
            columnas=pd.NA,
            nulos_totales=pd.NA,
            duplicados_exactos=pd.NA,
            columnas_con_nulos=pd.NA,
            checksum=pd.NA,
            tipo_registro="detalle_columna",
        )
        for name, df in datasets.items()
    ]
    detail_df = pd.concat(profile_frames, ignore_index=True)

    return pd.concat([summary_df, detail_df], ignore_index=True, sort=False)
