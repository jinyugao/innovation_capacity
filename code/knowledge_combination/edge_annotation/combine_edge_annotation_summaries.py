"""Combine and validate yearly edge-annotation summaries."""

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
PRIOR_WINDOW_YEARS = 5

EDGE_ANNOTATION_SUBDIR = Path("data/processed/knowledge_combination/edge_annotation")
SUMMARY_SUBDIR = EDGE_ANNOTATION_SUBDIR / "summary"
SELF_LOOP_SUMMARY_SUBDIR = EDGE_ANNOTATION_SUBDIR / "self_loop_summary"
VALIDATION_SUBDIR = EDGE_ANNOTATION_SUBDIR / "validation"

SUMMARY_FILE_STEM = "edge_annotation_summary"
SELF_LOOP_SUMMARY_FILE_STEM = "edge_annotation_self_loop_summary"

COMBINED_SUMMARY_FILE_NAME = "edge_annotation_summary_by_year.csv"
COMBINED_SELF_LOOP_FILE_NAME = "edge_annotation_self_loop_summary_by_year.csv"
YEAR_VALIDATION_FILE_NAME = "edge_annotation_year_validation.csv"
VALIDATION_SUMMARY_FILE_NAME = "edge_annotation_validation_summary.csv"

CATEGORIES = [
    "New_Node",
    "New_Combination",
    "New_Relation",
    "Repeated_Triple",
]

SHARE_TOLERANCE = 1e-9


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


def self_loop_summary_file(project_root: Path, year: int) -> Path:
    return (
        project_root
        / SELF_LOOP_SUMMARY_SUBDIR
        / f"{SELF_LOOP_SUMMARY_FILE_STEM}_{year}.csv"
    )


def output_paths(project_root: Path) -> list[Path]:
    return [
        project_root / SUMMARY_SUBDIR / COMBINED_SUMMARY_FILE_NAME,
        project_root / SELF_LOOP_SUMMARY_SUBDIR / COMBINED_SELF_LOOP_FILE_NAME,
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
            f"with IC_EDGE_ANNOTATION_SUMMARY_OVERWRITE=1:\n{existing_text}"
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


def collect_inputs(project_root: Path) -> tuple[list[dict[str, str]], list[dict[str, str]], list[dict[str, Any]]]:
    summary_rows: list[dict[str, str]] = []
    self_loop_rows: list[dict[str, str]] = []
    validation_rows: list[dict[str, Any]] = []

    for year in expected_years():
        summary_path = summary_file(project_root, year)
        self_loop_path = self_loop_summary_file(project_root, year)
        summary_exists = summary_path.exists()
        self_loop_exists = self_loop_path.exists()

        validation: dict[str, Any] = {
            "pyear": year,
            "summary_file_exists": bool_text(summary_exists),
            "self_loop_summary_file_exists": bool_text(self_loop_exists),
            "all_input_files_exist": bool_text(summary_exists and self_loop_exists),
            "n_annotated_rows": "",
            "n_category_rows": "",
            "category_count_matches_annotated_rows": "",
            "annotation_share_sum": "",
            "annotation_share_sum_close_to_one": "",
            "focal_row_accounting_matches": "",
            "n_prior_years_found": "",
            "prior_window_complete": "",
        }

        if summary_exists:
            row = read_single_row_csv(summary_path)
            summary_rows.append(row)

            n_annotated = int_value(row, "n_annotated_rows")
            n_category_rows = sum(int_value(row, f"n_{category}") for category in CATEGORIES)
            share_values = [
                float_value(row, f"share_{category}")
                for category in CATEGORIES
            ]
            share_sum = (
                None
                if any(value is None for value in share_values)
                else sum(value for value in share_values if value is not None)
            )
            n_focal = int_value(row, "n_focal_rows")
            n_invalid = int_value(row, "n_invalid_endpoint_rows")
            n_self_loop = int_value(row, "n_self_loop_rows")
            n_prior_years_found = int_value(row, "n_prior_years_found")

            validation.update(
                {
                    "n_annotated_rows": n_annotated,
                    "n_category_rows": n_category_rows,
                    "category_count_matches_annotated_rows": bool_text(
                        n_category_rows == n_annotated
                    ),
                    "annotation_share_sum": "" if share_sum is None else share_sum,
                    "annotation_share_sum_close_to_one": bool_text(
                        n_annotated == 0
                        or (
                            share_sum is not None
                            and abs(share_sum - 1.0) <= SHARE_TOLERANCE
                        )
                    ),
                    "focal_row_accounting_matches": bool_text(
                        n_invalid + n_self_loop + n_annotated == n_focal
                    ),
                    "n_prior_years_found": n_prior_years_found,
                    "prior_window_complete": bool_text(
                        n_prior_years_found == PRIOR_WINDOW_YEARS
                    ),
                }
            )

        if self_loop_exists:
            self_loop_rows.append(read_single_row_csv(self_loop_path))

        validation_rows.append(validation)

    return summary_rows, self_loop_rows, validation_rows


def build_validation_summary(validation_rows: list[dict[str, Any]]) -> dict[str, Any]:
    checks = [
        "all_input_files_exist",
        "category_count_matches_annotated_rows",
        "annotation_share_sum_close_to_one",
        "focal_row_accounting_matches",
        "prior_window_complete",
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
    overwrite = get_env_bool("IC_EDGE_ANNOTATION_SUMMARY_OVERWRITE", False)
    outputs = output_paths(project_root)

    print(f"Project root: {project_root}")
    print(f"Overwrite outputs: {overwrite}")
    for output in outputs:
        print(f"Output: {output}")

    check_outputs(outputs, overwrite)
    summary_rows, self_loop_rows, validation_rows = collect_inputs(project_root)
    validation_summary = build_validation_summary(validation_rows)

    write_csv(project_root / SUMMARY_SUBDIR / COMBINED_SUMMARY_FILE_NAME, summary_rows)
    write_csv(
        project_root / SELF_LOOP_SUMMARY_SUBDIR / COMBINED_SELF_LOOP_FILE_NAME,
        self_loop_rows,
    )
    write_csv(project_root / VALIDATION_SUBDIR / YEAR_VALIDATION_FILE_NAME, validation_rows)
    write_csv(
        project_root / VALIDATION_SUBDIR / VALIDATION_SUMMARY_FILE_NAME,
        [validation_summary],
    )

    print(f"Yearly edge-annotation summary rows: {len(summary_rows):,}")
    print(f"Yearly self-loop summary rows: {len(self_loop_rows):,}")
    print(f"All checks passed: {validation_summary['all_checks_passed']}")

    if validation_summary["all_checks_passed"] != "true":
        raise RuntimeError(
            "Edge-annotation validation failed. See "
            f"{project_root / VALIDATION_SUBDIR / YEAR_VALIDATION_FILE_NAME}"
        )

    print("Edge-annotation summary combination complete.")


if __name__ == "__main__":
    main()
