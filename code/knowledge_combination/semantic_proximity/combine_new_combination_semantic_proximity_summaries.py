"""Combine and validate New_Combination semantic-proximity summaries."""

from __future__ import annotations

import csv
import os
from pathlib import Path
from typing import Any


DEFAULT_PROJECT_ROOT = Path(
    "/xdisk/sebratt/jinyugao/research/projects/innovation_capacity"
)

BASE_YEAR = 1980
N_YEARS = 40
EXPECTED_EMBEDDING_DIM = 768
EXPECTED_SHARE_SCORED = 1.0
FLOAT_TOLERANCE = 1e-6

NEW_COMBINATION_SUBDIR = Path(
    "data/processed/knowledge_combination/semantic_proximity/new_combination"
)
SUMMARY_SUBDIR = NEW_COMBINATION_SUBDIR / "summary"
VALIDATION_SUBDIR = NEW_COMBINATION_SUBDIR / "validation"

SUMMARY_FILE_STEM = "semantic_proximity_new_combination_summary"
COMBINED_SUMMARY_FILE_NAME = (
    "semantic_proximity_new_combination_summary_by_year.csv"
)
YEAR_VALIDATION_FILE_NAME = (
    "semantic_proximity_new_combination_year_validation.csv"
)
VALIDATION_SUMMARY_FILE_NAME = (
    "semantic_proximity_new_combination_validation_summary.csv"
)

SCORE_COLUMNS = [
    "score_min",
    "score_mean",
    "score_std",
    "score_p01",
    "score_p05",
    "score_p10",
    "score_p25",
    "score_p50",
    "score_p75",
    "score_p90",
    "score_p95",
    "score_p99",
    "score_max",
]


def get_env_path(name: str, default: Path) -> Path:
    value = os.environ.get(name)
    return Path(value).expanduser() if value else default


def get_env_bool(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y"}


def expected_years() -> list[int]:
    return list(range(BASE_YEAR, BASE_YEAR + N_YEARS))


def summary_file(project_root: Path, year: int) -> Path:
    return project_root / SUMMARY_SUBDIR / f"{SUMMARY_FILE_STEM}_{year}.csv"


def output_paths(project_root: Path) -> list[Path]:
    return [
        project_root / SUMMARY_SUBDIR / COMBINED_SUMMARY_FILE_NAME,
        project_root / VALIDATION_SUBDIR / YEAR_VALIDATION_FILE_NAME,
        project_root / VALIDATION_SUBDIR / VALIDATION_SUMMARY_FILE_NAME,
    ]


def check_outputs(paths: list[Path], overwrite: bool) -> None:
    for path in paths:
        path.parent.mkdir(parents=True, exist_ok=True)

    existing = [path for path in paths if path.exists()]
    if existing and not overwrite:
        existing_text = "\n".join(str(path) for path in existing)
        raise FileExistsError(
            "Output file(s) already exist. Move them, delete them, or rerun "
            "with IC_NEW_COMBINATION_SEMANTIC_PROXIMITY_SUMMARY_OVERWRITE=1:\n"
            f"{existing_text}"
        )
    if overwrite:
        for path in existing:
            path.unlink()


def read_single_row_csv(path: Path) -> dict[str, str]:
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    if len(rows) != 1:
        raise ValueError(f"Expected exactly one data row in {path}, found {len(rows)}")
    return rows[0]


def int_value(row: dict[str, str], column: str) -> int:
    value = row.get(column, "")
    return 0 if value == "" else int(float(value))


def float_value(row: dict[str, str], column: str) -> float | None:
    value = row.get(column, "")
    return None if value == "" else float(value)


def bool_text(value: bool) -> str:
    return "true" if value else "false"


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        raise ValueError(f"No rows to write for {path}")
    fieldnames = list(rows[0])
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def score_columns_are_ordered(row: dict[str, str]) -> bool:
    ordered_columns = [
        "score_min",
        "score_p01",
        "score_p05",
        "score_p10",
        "score_p25",
        "score_p50",
        "score_p75",
        "score_p90",
        "score_p95",
        "score_p99",
        "score_max",
    ]
    values = [float_value(row, column) for column in ordered_columns]
    if any(value is None for value in values):
        return False
    numeric_values = [value for value in values if value is not None]
    return all(
        numeric_values[index] <= numeric_values[index + 1] + FLOAT_TOLERANCE
        for index in range(len(numeric_values) - 1)
    )


def score_range_is_valid(row: dict[str, str]) -> bool:
    values = [float_value(row, column) for column in SCORE_COLUMNS]
    if any(value is None for value in values):
        return False
    numeric_values = [value for value in values if value is not None]
    return all(
        -1.0 - FLOAT_TOLERANCE <= value <= 1.0 + FLOAT_TOLERANCE
        for value in numeric_values
    )


def collect_inputs(project_root: Path) -> tuple[list[dict[str, str]], list[dict[str, Any]]]:
    summary_rows: list[dict[str, str]] = []
    validation_rows: list[dict[str, Any]] = []

    for year in expected_years():
        path = summary_file(project_root, year)
        exists = path.exists()
        validation: dict[str, Any] = {
            "pyear": year,
            "summary_file_exists": bool_text(exists),
            "summary_pyear_matches_expected": "",
            "n_new_combination_rows": "",
            "n_scored_rows": "",
            "scored_rows_match_new_combination_rows": "",
            "n_missing_subject_embedding": "",
            "n_missing_object_embedding": "",
            "n_missing_any_embedding": "",
            "no_missing_embeddings": "",
            "share_scored_rows": "",
            "share_scored_rows_is_one": "",
            "score_range_valid": "",
            "score_quantiles_ordered": "",
            "embedding_dim": "",
            "embedding_dim_valid": "",
        }

        if exists:
            row = read_single_row_csv(path)
            summary_rows.append(row)

            pyear = int_value(row, "pyear")
            n_new_combination_rows = int_value(row, "n_new_combination_rows")
            n_scored_rows = int_value(row, "n_scored_rows")
            n_missing_subject_embedding = int_value(row, "n_missing_subject_embedding")
            n_missing_object_embedding = int_value(row, "n_missing_object_embedding")
            n_missing_any_embedding = int_value(row, "n_missing_any_embedding")
            share_scored_rows = float_value(row, "share_scored_rows")
            embedding_dim = int_value(row, "embedding_dim")

            validation.update(
                {
                    "summary_pyear_matches_expected": bool_text(pyear == year),
                    "n_new_combination_rows": n_new_combination_rows,
                    "n_scored_rows": n_scored_rows,
                    "scored_rows_match_new_combination_rows": bool_text(
                        n_scored_rows == n_new_combination_rows
                    ),
                    "n_missing_subject_embedding": n_missing_subject_embedding,
                    "n_missing_object_embedding": n_missing_object_embedding,
                    "n_missing_any_embedding": n_missing_any_embedding,
                    "no_missing_embeddings": bool_text(
                        n_missing_subject_embedding == 0
                        and n_missing_object_embedding == 0
                        and n_missing_any_embedding == 0
                    ),
                    "share_scored_rows": "" if share_scored_rows is None else share_scored_rows,
                    "share_scored_rows_is_one": bool_text(
                        share_scored_rows is not None
                        and abs(share_scored_rows - EXPECTED_SHARE_SCORED)
                        <= FLOAT_TOLERANCE
                    ),
                    "score_range_valid": bool_text(score_range_is_valid(row)),
                    "score_quantiles_ordered": bool_text(score_columns_are_ordered(row)),
                    "embedding_dim": embedding_dim,
                    "embedding_dim_valid": bool_text(
                        embedding_dim == EXPECTED_EMBEDDING_DIM
                    ),
                }
            )

        validation_rows.append(validation)

    return summary_rows, validation_rows


def build_validation_summary(validation_rows: list[dict[str, Any]]) -> dict[str, Any]:
    checks = [
        "summary_file_exists",
        "summary_pyear_matches_expected",
        "scored_rows_match_new_combination_rows",
        "no_missing_embeddings",
        "share_scored_rows_is_one",
        "score_range_valid",
        "score_quantiles_ordered",
        "embedding_dim_valid",
    ]
    summary: dict[str, Any] = {
        "expected_years": N_YEARS,
        "start_year": BASE_YEAR,
        "end_year": BASE_YEAR + N_YEARS - 1,
    }

    for check in checks:
        n_pass = sum(1 for row in validation_rows if row.get(check) == "true")
        summary[f"n_{check}_pass"] = n_pass
        summary[f"n_{check}_fail"] = N_YEARS - n_pass

    summary["all_checks_passed"] = bool_text(
        all(
            row.get(check) == "true"
            for row in validation_rows
            for check in checks
        )
    )
    return summary


def main() -> None:
    project_root = get_env_path("IC_PROJECT_ROOT", DEFAULT_PROJECT_ROOT)
    overwrite = get_env_bool(
        "IC_NEW_COMBINATION_SEMANTIC_PROXIMITY_SUMMARY_OVERWRITE",
        False,
    )
    outputs = output_paths(project_root)

    print(f"Project root: {project_root}")
    print(f"Overwrite outputs: {overwrite}")
    for output in outputs:
        print(f"Output: {output}")

    check_outputs(outputs, overwrite)
    summary_rows, validation_rows = collect_inputs(project_root)
    validation_summary = build_validation_summary(validation_rows)

    write_csv(project_root / SUMMARY_SUBDIR / COMBINED_SUMMARY_FILE_NAME, summary_rows)
    write_csv(project_root / VALIDATION_SUBDIR / YEAR_VALIDATION_FILE_NAME, validation_rows)
    write_csv(
        project_root / VALIDATION_SUBDIR / VALIDATION_SUMMARY_FILE_NAME,
        [validation_summary],
    )

    print(f"Yearly New_Combination semantic-proximity rows: {len(summary_rows):,}")
    print(f"All checks passed: {validation_summary['all_checks_passed']}")

    if validation_summary["all_checks_passed"] != "true":
        raise RuntimeError(
            "New_Combination semantic-proximity validation failed. See "
            f"{project_root / VALIDATION_SUBDIR / YEAR_VALIDATION_FILE_NAME}"
        )

    print("New_Combination semantic-proximity summary combination complete.")


if __name__ == "__main__":
    main()
