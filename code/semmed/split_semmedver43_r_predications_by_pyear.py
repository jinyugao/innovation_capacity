"""Split the filtered SemMedDB predication file into one file per publication year."""

from __future__ import annotations

from pathlib import Path

import pandas as pd


INPUT_FILE = Path(
    "/xdisk/sebratt/jinyugao/projects/innovation_capacity/data/interim/semmedVER43_R/"
    "semmedVER43_2024_R_predications_with_pyear_filtered.csv.gz"
)
OUTPUT_DIR = Path(
    "/xdisk/sebratt/jinyugao/projects/innovation_capacity/data/interim/semmedVER43_R/"
    "split_predications_with_pyear_filtered_by_pyear"
)

OUTPUT_FILE_PREFIX = "semmedVER43_R_predications_with_pyear_filtered"
CHUNK_SIZE = 100_000
OVERWRITE = False


def check_input(input_file: Path) -> None:
    if not input_file.exists():
        raise FileNotFoundError(f"Missing required input file: {input_file}")


def prepare_output_dir(output_dir: Path, overwrite: bool) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    existing_files = list(output_dir.glob(f"{OUTPUT_FILE_PREFIX}_*.csv.gz"))

    if existing_files and not overwrite:
        examples = "\n".join(str(path) for path in existing_files[:10])
        raise FileExistsError(
            "Split output files already exist. Set OVERWRITE = True to replace them:\n"
            f"{examples}"
        )

    if overwrite:
        for path in existing_files:
            path.unlink()


def clean_year_for_filename(year: object) -> str:
    year_text = str(year).strip()
    if year_text.endswith(".0"):
        year_text = year_text[:-2]
    return year_text


def split_by_pyear(input_file: Path, output_dir: Path, chunk_size: int) -> None:
    written_years: set[str] = set()
    total_rows = 0
    total_written_rows = 0
    missing_pyear_rows = 0

    reader = pd.read_csv(
        input_file,
        compression="gzip",
        chunksize=chunk_size,
        dtype={"PYEAR": "string"},
    )

    for chunk_number, chunk in enumerate(reader, start=1):
        chunk_rows = len(chunk)
        total_rows += chunk_rows

        missing_pyear = int(chunk["PYEAR"].isna().sum())
        missing_pyear_rows += missing_pyear

        for year, group_data in chunk.dropna(subset=["PYEAR"]).groupby("PYEAR"):
            year_text = clean_year_for_filename(year)
            output_file = output_dir / f"{OUTPUT_FILE_PREFIX}_{year_text}.csv.gz"
            write_header = year_text not in written_years

            group_data.to_csv(
                output_file,
                mode="a",
                index=False,
                compression="gzip",
                header=write_header,
            )
            written_years.add(year_text)
            total_written_rows += len(group_data)

        print(
            f"Chunk {chunk_number:,}: processed {chunk_rows:,} rows; "
            f"missing PYEAR {missing_pyear:,}."
        )

    print("Splitting complete.")
    print(f"Total rows processed: {total_rows:,}")
    print(f"Rows with missing PYEAR skipped: {missing_pyear_rows:,}")
    print(f"Rows written to yearly files: {total_written_rows:,}")
    print(f"Number of yearly files written: {len(written_years):,}")


def main() -> None:
    check_input(INPUT_FILE)
    prepare_output_dir(OUTPUT_DIR, OVERWRITE)
    split_by_pyear(INPUT_FILE, OUTPUT_DIR, CHUNK_SIZE)


if __name__ == "__main__":
    main()
