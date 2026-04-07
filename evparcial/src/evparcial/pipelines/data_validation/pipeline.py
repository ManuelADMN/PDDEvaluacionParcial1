from __future__ import annotations

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import build_audit_report, generate_visual_reports, validate_final_outputs


def create_pipeline(**kwargs) -> Pipeline:
    """Create validation pipeline."""
    return pipeline(
        [
            node(
                func=build_audit_report,
                inputs=[
                    "estudiantes_raw",
                    "inscripciones_raw",
                    "calificaciones_raw",
                    "asistencia_raw",
                    "estudiantes_clean",
                    "inscripciones_clean",
                    "calificaciones_clean",
                    "asistencia_clean",
                    "dataset_universitario",
                    "dataset_universitario_modelo",
                ],
                outputs="auditoria_datasets",
                name="build_audit_report_node",
            ),
            node(
                func=validate_final_outputs,
                inputs=[
                    "estudiantes_clean",
                    "inscripciones_clean",
                    "calificaciones_clean",
                    "asistencia_clean",
                    "dataset_universitario",
                    "params:expected_final_schema",
                ],
                outputs="reporte_validacion",
                name="validate_final_outputs_node",
            ),
            node(
                func=generate_visual_reports,
                inputs=[
                    "comparacion_limpieza",
                    "dataset_universitario",
                    "params:report_paths",
                ],
                outputs=None,
                name="generate_visual_reports_node",
            ),
        ]
    )
