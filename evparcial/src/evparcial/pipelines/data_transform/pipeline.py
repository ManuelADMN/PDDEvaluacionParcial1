from __future__ import annotations

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import build_dataset_modelo, build_dataset_universitario


def create_pipeline(**kwargs) -> Pipeline:
    """Create transformation pipeline."""
    return pipeline(
        [
            node(
                func=build_dataset_universitario,
                inputs=[
                    "estudiantes_clean",
                    "inscripciones_clean",
                    "calificaciones_clean",
                    "asistencia_clean",
                ],
                outputs=[
                    "dataset_universitario",
                    "resumen_asignaturas",
                ],
                name="build_dataset_universitario_node",
            ),
            node(
                func=build_dataset_modelo,
                inputs=[
                    "dataset_universitario",
                    "params:numeric_columns_to_scale",
                    "params:categorical_columns_to_encode",
                ],
                outputs="dataset_universitario_modelo",
                name="build_dataset_modelo_node",
            ),
        ]
    )
