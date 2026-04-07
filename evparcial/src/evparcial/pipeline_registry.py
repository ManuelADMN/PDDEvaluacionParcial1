from __future__ import annotations

from kedro.pipeline import Pipeline

from evparcial.pipelines import (
    data_cleaning,
    data_ingestion,
    data_transform,
    data_validation,
)


def register_pipelines() -> dict[str, Pipeline]:
    """Register all project pipelines."""
    ingestion = data_ingestion.create_pipeline()
    cleaning = data_cleaning.create_pipeline()
    transform = data_transform.create_pipeline()
    validation = data_validation.create_pipeline()

    return {
        "data_ingestion": ingestion,
        "data_cleaning": cleaning,
        "data_transform": transform,
        "data_validation": validation,
        "__default__": ingestion + cleaning + transform + validation,
    }