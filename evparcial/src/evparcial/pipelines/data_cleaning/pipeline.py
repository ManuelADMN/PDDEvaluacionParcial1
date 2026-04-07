from __future__ import annotations

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    clean_asistencia,
    clean_calificaciones,
    clean_estudiantes,
    clean_inscripciones,
    compare_before_after_cleaning,
)


def create_pipeline(**kwargs) -> Pipeline:
    """Create data cleaning pipeline."""
    return pipeline(
        [
            node(
                func=clean_estudiantes,
                inputs=[
                    "estudiantes_raw",
                    "params:categorical_fill_values",
                    "params:valid_years",
                ],
                outputs="estudiantes_clean",
                name="clean_estudiantes_node",
            ),
            node(
                func=clean_inscripciones,
                inputs=[
                    "inscripciones_raw",
                    "params:categorical_fill_values",
                    "params:semestre_range",
                    "params:valid_years",
                ],
                outputs="inscripciones_clean",
                name="clean_inscripciones_node",
            ),
            node(
                func=clean_calificaciones,
                inputs=[
                    "calificaciones_raw",
                    "params:grade_range",
                ],
                outputs="calificaciones_clean",
                name="clean_calificaciones_node",
            ),
            node(
                func=clean_asistencia,
                inputs="asistencia_raw",
                outputs="asistencia_clean",
                name="clean_asistencia_node",
            ),
            node(
                func=compare_before_after_cleaning,
                inputs=[
                    "estudiantes_raw",
                    "inscripciones_raw",
                    "calificaciones_raw",
                    "asistencia_raw",
                    "estudiantes_clean",
                    "inscripciones_clean",
                    "calificaciones_clean",
                    "asistencia_clean",
                ],
                outputs="comparacion_limpieza",
                name="compare_before_after_cleaning_node",
            ),
        ]
    )