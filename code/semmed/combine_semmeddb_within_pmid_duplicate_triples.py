"""Combine yearly SemMedDB within-PMID duplicate diagnostic outputs."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from summarize_semmeddb_within_pmid_duplicate_triples import (
    BASE_YEAR,
    COLLISION_EXAMPLE_COLUMNS,
    COLLISION_EXAMPLE_OUTPUT_FILE,
    GROUP_SIZE_OUTPUT_FILE,
    N_YEARS,
    SUMMARY_OUTPUT_FILE,
    YEARLY_OUTPUT_DIR,
    add_overall_group_size_distribution,
    add_overall_summary,
)


OVERWRITE = False

SUMMARY_FILE_TEMPLATE = "semmeddb_within_pmid_duplicate_summary_{pyear}.csv"
GROUP_SIZE_FILE_TEMPLATE = (
    "semmeddb_within_pmid_duplicate_group_size_distribution_{pyear}.csv"
)
COLLISION_EXAMPLE_FILE_TEMPLATE = (
    "semmeddb_primary_cui_collision_examples_{pyear}.csv"
)


def yearly_file_paths(yearly_dir: Path, pyear: int) -> tuple[Path, Path, Path]:
    return (
        yearly_dir / SUMMARY_FILE_TEMPLATE.format(pyear=pyear),
        yearly_dir / GROUP_SIZE_FILE_TEMPLATE.format(pyear=pyear),
        yearly_dir / COLLISION_EXAMPLE_FILE_TEMPLATE.format(pyear=pyear),
    )


def check_inputs(yearly_dir: Path, years: list[int]) -> None:
    expected_files = [
        path
        for pyear in years
        for path in yearly_file_paths(yearly_dir, pyear)
    ]
    missing_files = [path for path in expected_files if not path.exists()]
    if missing_files:
        missing = "\n".join(str(path) for path in missing_files)
        raise FileNotFoundError(
            "Missing yearly diagnostic output file(s). Confirm that every "
            f"Slurm array task completed successfully:\n{missing}"
        )


def check_outputs(output_files: list[Path]) -> None:
    for path in output_files:
        path.parent.mkdir(parents=True, exist_ok=True)

    existing_files = [path for path in output_files if path.exists()]
    if existing_files and not OVERWRITE:
        existing = "\n".join(str(path) for path in existing_files)
        raise FileExistsError(
            "Combined output file(s) already exist. Set OVERWRITE = True to "
            f"replace them:\n{existing}"
        )
    if OVERWRITE:
        for path in existing_files:
            path.unlink()


def read_and_validate_yearly_tables(
    files_by_year: list[tuple[int, Path]],
    table_name: str,
    allow_empty: bool = False,
) -> pd.DataFrame:
    frames = []
    expected_columns: list[str] | None = None

    for pyear, path in files_by_year:
        table = pd.read_csv(path, dtype={"pyear": "string"})
        if "pyear" not in table.columns:
            raise ValueError(f"{path} does not contain the required pyear column.")

        if expected_columns is None:
            expected_columns = table.columns.tolist()
        elif table.columns.tolist() != expected_columns:
            raise ValueError(
                f"Column mismatch in {table_name} file {path}.\n"
                f"Expected: {expected_columns}\n"
                f"Found: {table.columns.tolist()}"
            )

        observed_years = set(table["pyear"].dropna().astype(str).unique())
        expected_year = {str(pyear)}
        if observed_years != expected_year and not (allow_empty and table.empty):
            raise ValueError(
                f"Unexpected pyear value(s) in {path}: "
                f"expected {expected_year}, found {observed_years}."
            )
        frames.append(table)

    if not frames:
        raise ValueError(f"No yearly {table_name} files were provided.")
    return pd.concat(frames, ignore_index=True)


def combine_yearly_outputs(
    yearly_dir: Path,
    years: list[int],
    summary_output_file: Path,
    group_size_output_file: Path,
    collision_example_output_file: Path,
) -> None:
    check_inputs(yearly_dir, years)
    output_files = [
        summary_output_file,
        group_size_output_file,
        collision_example_output_file,
    ]
    check_outputs(output_files)

    paths_by_year = {
        pyear: yearly_file_paths(yearly_dir, pyear) for pyear in years
    }
    summary = read_and_validate_yearly_tables(
        [(pyear, paths_by_year[pyear][0]) for pyear in years],
        "summary",
    )
    group_size_distribution = read_and_validate_yearly_tables(
        [(pyear, paths_by_year[pyear][1]) for pyear in years],
        "group-size distribution",
    )
    collision_examples = read_and_validate_yearly_tables(
        [(pyear, paths_by_year[pyear][2]) for pyear in years],
        "collision examples",
        allow_empty=True,
    )

    if collision_examples.columns.tolist() != COLLISION_EXAMPLE_COLUMNS:
        raise ValueError(
            "Collision-example columns do not match the expected schema.\n"
            f"Expected: {COLLISION_EXAMPLE_COLUMNS}\n"
            f"Found: {collision_examples.columns.tolist()}"
        )

    summary = add_overall_summary(summary)
    group_size_distribution = add_overall_group_size_distribution(
        group_size_distribution
    )

    summary.to_csv(summary_output_file, index=False)
    group_size_distribution.to_csv(group_size_output_file, index=False)
    collision_examples.to_csv(collision_example_output_file, index=False)

    print(
        f"Combined {len(years):,} years ({years[0]}-{years[-1]})."
    )
    print(f"Saved {len(summary):,} summary rows to {summary_output_file}")
    print(
        "Saved "
        f"{len(group_size_distribution):,} group-size rows to "
        f"{group_size_output_file}"
    )
    print(
        f"Saved {len(collision_examples):,} collision-example rows to "
        f"{collision_example_output_file}"
    )


def main() -> None:
    years = list(range(BASE_YEAR, BASE_YEAR + N_YEARS))
    combine_yearly_outputs(
        YEARLY_OUTPUT_DIR,
        years,
        SUMMARY_OUTPUT_FILE,
        GROUP_SIZE_OUTPUT_FILE,
        COLLISION_EXAMPLE_OUTPUT_FILE,
    )


if __name__ == "__main__":
    main()
