"""Keep only PMID-linked rows from the OpenAlex authorship-institution-work table."""

from __future__ import annotations

from pathlib import Path

import pandas as pd


OPENALEX_DIR = Path("/xdisk/sebratt/jinyugao/data/products/openalex/flattened_snapshot_2025")

INPUT_FILE = OPENALEX_DIR / "openalex_authorships_institutions_workids.csv.gz"
OUTPUT_FILE = OPENALEX_DIR / "openalex_authorships_institutions_workids_pmid.csv.gz"

CHUNK_SIZE = 1_000_000
OVERWRITE = False


def check_input(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing required input file: {path}")


def check_output(path: Path, overwrite: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not overwrite:
        raise FileExistsError(
            "Output file already exists. Set OVERWRITE = True to replace it:\n"
            f"{path}"
        )
    if path.exists() and overwrite:
        path.unlink()


def filter_pmid_rows(input_file: Path, output_file: Path) -> None:
    total_rows = 0
    kept_rows = 0
    wrote_header = False

    reader = pd.read_csv(
        input_file,
        compression="gzip",
        chunksize=CHUNK_SIZE,
        dtype="string",
    )

    for chunk_number, chunk in enumerate(reader, start=1):
        total_rows += len(chunk)
        chunk = chunk[chunk["pmid"].notna() & (chunk["pmid"].str.strip() != "")]
        kept_rows += len(chunk)

        if chunk.empty:
            print(
                f"Chunk {chunk_number:,}: read {total_rows:,} total rows; "
                f"kept {kept_rows:,} PMID rows so far."
            )
            continue

        chunk.to_csv(
            output_file,
            mode="a",
            index=False,
            compression="gzip",
            header=not wrote_header,
        )
        wrote_header = True

        print(
            f"Chunk {chunk_number:,}: read {total_rows:,} total rows; "
            f"wrote {kept_rows:,} PMID rows."
        )

    if not wrote_header:
        pd.DataFrame(columns=["pmid"]).to_csv(
            output_file, index=False, compression="gzip"
        )

    print(f"Saved PMID-only table to {output_file}")
    print(f"Total rows read: {total_rows:,}")
    print(f"Total PMID rows written: {kept_rows:,}")


def main() -> None:
    check_input(INPUT_FILE)
    check_output(OUTPUT_FILE, OVERWRITE)
    filter_pmid_rows(INPUT_FILE, OUTPUT_FILE)


if __name__ == "__main__":
    main()
