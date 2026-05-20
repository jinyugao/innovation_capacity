"""Extract detailed first- and last-author institution rows from OpenAlex."""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path

import pandas as pd


OPENALEX_DIR = Path("/xdisk/sebratt/jinyugao/data/products/openalex/flattened_snapshot_2025")

INPUT_FILE = OPENALEX_DIR / "openalex_authorships_institutions_workids_pmid.csv.gz"
FIRST_AUTHOR_OUTPUT_FILE = (
    OPENALEX_DIR / "openalex_first_author_institutions_pmid.csv.gz"
)
LAST_AUTHOR_OUTPUT_FILE = OPENALEX_DIR / "openalex_last_author_institutions_pmid.csv.gz"

CHUNK_SIZE = 1_000_000
OVERWRITE = False

AUTHOR_POSITIONS = {"first", "last"}
COUNTRY_COUNT_COLUMN = "author_work_country_count"
KEY_COLUMNS = ["pmid", "work_id", "author_position", "author_id"]
COUNTRY_COLUMN = "institution_country_code"


def check_input(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing required input file: {path}")


def check_outputs(paths: list[Path], overwrite: bool) -> None:
    for path in paths:
        path.parent.mkdir(parents=True, exist_ok=True)

    existing_files = [str(path) for path in paths if path.exists()]
    if existing_files and not overwrite:
        existing = "\n".join(existing_files)
        raise FileExistsError(
            "Output file(s) already exist. Set OVERWRITE = True to replace them:\n"
            f"{existing}"
        )

    if overwrite:
        for path in paths:
            if path.exists():
                path.unlink()


def normalize_author_position(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip().lower()


def normalize_key_value(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def build_author_country_sets(input_file: Path) -> dict[tuple[str, str, str, str], set[str]]:
    author_country_sets: dict[tuple[str, str, str, str], set[str]] = defaultdict(set)
    total_rows = 0
    kept_rows = 0

    reader = pd.read_csv(
        input_file,
        compression="gzip",
        chunksize=CHUNK_SIZE,
        dtype="string",
    )

    for chunk_number, chunk in enumerate(reader, start=1):
        total_rows += len(chunk)
        chunk["author_position_normalized"] = chunk["author_position"].map(
            normalize_author_position
        )
        chunk = chunk[chunk["author_position_normalized"].isin(AUTHOR_POSITIONS)]
        kept_rows += len(chunk)

        for row in chunk.itertuples(index=False):
            row_dict = row._asdict()
            key = tuple(normalize_key_value(row_dict[column]) for column in KEY_COLUMNS)
            country_code = normalize_key_value(row_dict.get(COUNTRY_COLUMN))
            if country_code:
                author_country_sets[key].add(country_code)

        print(
            f"Count pass chunk {chunk_number:,}: read {total_rows:,} rows; "
            f"kept {kept_rows:,} first/last author rows."
        )

    print(f"Built country-count lookup for {len(author_country_sets):,} author-work keys.")
    return author_country_sets


def add_country_counts(
    chunk: pd.DataFrame,
    author_country_sets: dict[tuple[str, str, str, str], set[str]],
) -> pd.DataFrame:
    country_counts = []

    for row in chunk.itertuples(index=False):
        row_dict = row._asdict()
        key = tuple(normalize_key_value(row_dict[column]) for column in KEY_COLUMNS)
        country_counts.append(len(author_country_sets.get(key, set())))

    chunk = chunk.copy()
    chunk[COUNTRY_COUNT_COLUMN] = country_counts
    return chunk


def write_first_last_author_tables(
    input_file: Path,
    author_country_sets: dict[tuple[str, str, str, str], set[str]],
) -> None:
    total_rows = 0
    first_rows = 0
    last_rows = 0
    wrote_first_header = False
    wrote_last_header = False

    reader = pd.read_csv(
        input_file,
        compression="gzip",
        chunksize=CHUNK_SIZE,
        dtype="string",
    )

    for chunk_number, chunk in enumerate(reader, start=1):
        total_rows += len(chunk)
        chunk["author_position_normalized"] = chunk["author_position"].map(
            normalize_author_position
        )
        chunk = chunk[chunk["author_position_normalized"].isin(AUTHOR_POSITIONS)]

        if chunk.empty:
            print(
                f"Write pass chunk {chunk_number:,}: read {total_rows:,} rows; "
                f"wrote {first_rows:,} first-author rows and {last_rows:,} "
                "last-author rows so far."
            )
            continue

        chunk = add_country_counts(chunk, author_country_sets)
        chunk = chunk.drop(columns=["author_position_normalized"])

        first_chunk = chunk[
            chunk["author_position"].map(normalize_author_position) == "first"
        ]
        if not first_chunk.empty:
            first_chunk.to_csv(
                FIRST_AUTHOR_OUTPUT_FILE,
                mode="a",
                index=False,
                compression="gzip",
                header=not wrote_first_header,
            )
            wrote_first_header = True
            first_rows += len(first_chunk)

        last_chunk = chunk[
            chunk["author_position"].map(normalize_author_position) == "last"
        ]
        if not last_chunk.empty:
            last_chunk.to_csv(
                LAST_AUTHOR_OUTPUT_FILE,
                mode="a",
                index=False,
                compression="gzip",
                header=not wrote_last_header,
            )
            wrote_last_header = True
            last_rows += len(last_chunk)

        print(
            f"Write pass chunk {chunk_number:,}: read {total_rows:,} rows; "
            f"wrote {first_rows:,} first-author rows and {last_rows:,} "
            "last-author rows."
        )

    if not wrote_first_header:
        pd.DataFrame(columns=[COUNTRY_COUNT_COLUMN]).to_csv(
            FIRST_AUTHOR_OUTPUT_FILE, index=False, compression="gzip"
        )
    if not wrote_last_header:
        pd.DataFrame(columns=[COUNTRY_COUNT_COLUMN]).to_csv(
            LAST_AUTHOR_OUTPUT_FILE, index=False, compression="gzip"
        )

    print(f"Saved first-author table to {FIRST_AUTHOR_OUTPUT_FILE}")
    print(f"Saved last-author table to {LAST_AUTHOR_OUTPUT_FILE}")
    print(f"Total rows read: {total_rows:,}")
    print(f"First-author rows written: {first_rows:,}")
    print(f"Last-author rows written: {last_rows:,}")


def main() -> None:
    check_input(INPUT_FILE)
    check_outputs([FIRST_AUTHOR_OUTPUT_FILE, LAST_AUTHOR_OUTPUT_FILE], OVERWRITE)
    author_country_sets = build_author_country_sets(INPUT_FILE)
    write_first_last_author_tables(INPUT_FILE, author_country_sets)


if __name__ == "__main__":
    main()
