from __future__ import annotations

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import build_initial_diagnostic


def create_pipeline(**kwargs) -> Pipeline:
    """Create data ingestion pipeline."""
    return pipeline(
        [
            node(
                func=build_initial_diagnostic,
                inputs=[
                    "estudiantes_raw",
                    "inscripciones_raw",
                    "calificaciones_raw",
                    "asistencia_raw",
                ],
                outputs="diagnostico_inicial",
                name="build_initial_diagnostic_node",
            )
        ]
    )
