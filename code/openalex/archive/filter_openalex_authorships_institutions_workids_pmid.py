"""Archived filter from the former two-step PMID authorship workflow.

The active builder now writes the PMID-linked Parquet directly, so this extra
read-and-write pass is no longer part of the active pipeline.
"""

from __future__ import annotations

import os
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


DEFAULT_INSTITUTION_DIR = Path(
    "/xdisk/sebratt/jinyugao/research/data/processed/openalex/institution"
)
INSTITUTION_DIR = Path(
    os.environ.get("IC_OPENALEX_INSTITUTION_DIR", DEFAULT_INSTITUTION_DIR)
).expanduser()

INPUT_FILE = INSTITUTION_DIR / "openalex_authorships_institutions_workids.parquet"
OUTPUT_FILE = INSTITUTION_DIR / "openalex_authorships_institutions_workids_pmid.parquet"

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

    parquet_file = pq.ParquetFile(input_file)
    schema = parquet_file.schema_arrow
    if "pmid" not in schema.names:
        raise ValueError(f"Input parquet is missing required column 'pmid': {input_file}")

    with pq.ParquetWriter(output_file, schema, compression="zstd") as writer:
        for chunk_number, batch in enumerate(
            parquet_file.iter_batches(batch_size=CHUNK_SIZE),
            start=1,
        ):
            table = pa.Table.from_batches([batch], schema=schema)
            total_rows += table.num_rows
            pmid = table.column("pmid")
            valid_pmid = pc.fill_null(
                pc.not_equal(pc.utf8_trim_whitespace(pmid), ""),
                False,
            )
            filtered = table.filter(valid_pmid)
            kept_rows += filtered.num_rows
            if filtered.num_rows:
                writer.write_table(filtered)

            print(
                f"Chunk {chunk_number:,}: read {total_rows:,} total rows; "
                f"wrote {kept_rows:,} PMID rows."
            )

    print(f"Saved PMID-only table to {output_file}")
    print(f"Total rows read: {total_rows:,}")
    print(f"Total PMID rows written: {kept_rows:,}")


def main() -> None:
    print(f"Institution directory: {INSTITUTION_DIR}")
    check_input(INPUT_FILE)
    check_output(OUTPUT_FILE, OVERWRITE)
    filter_pmid_rows(INPUT_FILE, OUTPUT_FILE)


if __name__ == "__main__":
    main()
