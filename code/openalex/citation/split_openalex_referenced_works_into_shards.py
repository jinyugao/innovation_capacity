"""Split OpenAlex referenced-works edges into resumable shard files.

This is the first step in the OpenAlex citation-count pipeline. It only splits
the large referenced-works CSV into smaller consecutive shards so later Slurm
array jobs can process one shard at a time.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


OPENALEX_DIR = Path("/xdisk/sebratt/jinyugao/data/products/openalex/flattened_snapshot_2025")
INPUT_FILE = OPENALEX_DIR / "openalex_works_referenced_works.csv.gz"
PIPELINE_DIR = OPENALEX_DIR / "citation_pipeline"
SHARD_DIR = PIPELINE_DIR / "reference_shards"
MANIFEST_FILE = PIPELINE_DIR / "openalex_referenced_works_shard_manifest.csv"

CHUNK_SIZE = 1_000_000
ROWS_PER_SHARD = 5_000_000
OVERWRITE = False

OUTPUT_COLUMNS = ["work_id", "referenced_work_id"]


def check_input(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing required input file: {path}")


def prepare_outputs() -> None:
    PIPELINE_DIR.mkdir(parents=True, exist_ok=True)
    SHARD_DIR.mkdir(parents=True, exist_ok=True)

    existing_shards = sorted(SHARD_DIR.glob("openalex_works_referenced_works_shard_*.csv.gz"))
    existing_outputs = existing_shards + ([MANIFEST_FILE] if MANIFEST_FILE.exists() else [])

    if existing_outputs and not OVERWRITE:
        existing = "\n".join(str(path) for path in existing_outputs[:20])
        if len(existing_outputs) > 20:
            existing += f"\n... and {len(existing_outputs) - 20} more"
        raise FileExistsError(
            "Shard output(s) already exist. Set OVERWRITE = True to replace them:\n"
            f"{existing}"
        )

    if OVERWRITE:
        for path in existing_outputs:
            path.unlink()


def shard_file(shard_id: int) -> Path:
    return SHARD_DIR / f"openalex_works_referenced_works_shard_{shard_id:04d}.csv.gz"


def write_part(part: pd.DataFrame, output_file: Path, write_header: bool) -> None:
    part.to_csv(
        output_file,
        mode="a",
        index=False,
        compression="gzip",
        header=write_header,
    )


def split_referenced_works() -> None:
    current_shard_id = 0
    current_shard_rows = 0
    current_shard_has_header = False
    total_rows = 0
    manifest_rows = []

    reader = pd.read_csv(
        INPUT_FILE,
        compression="gzip",
        usecols=OUTPUT_COLUMNS,
        chunksize=CHUNK_SIZE,
        dtype="string",
    )

    for chunk_number, chunk in enumerate(reader, start=1):
        chunk = chunk.reindex(columns=OUTPUT_COLUMNS)
        chunk_offset = 0
        total_rows += len(chunk)

        while chunk_offset < len(chunk):
            room_in_shard = ROWS_PER_SHARD - current_shard_rows
            part = chunk.iloc[chunk_offset : chunk_offset + room_in_shard]
            output_file = shard_file(current_shard_id)

            write_part(part, output_file, write_header=not current_shard_has_header)
            current_shard_has_header = True
            current_shard_rows += len(part)
            chunk_offset += len(part)

            if current_shard_rows == ROWS_PER_SHARD:
                manifest_rows.append(
                    {
                        "shard_id": current_shard_id,
                        "shard_file": str(output_file),
                        "n_rows": current_shard_rows,
                    }
                )
                print(
                    f"Finished shard {current_shard_id:04d}: "
                    f"{current_shard_rows:,} rows."
                )
                current_shard_id += 1
                current_shard_rows = 0
                current_shard_has_header = False

        print(f"Input chunk {chunk_number:,}: read {total_rows:,} total rows.")

    if current_shard_has_header:
        output_file = shard_file(current_shard_id)
        manifest_rows.append(
            {
                "shard_id": current_shard_id,
                "shard_file": str(output_file),
                "n_rows": current_shard_rows,
            }
        )
        print(f"Finished shard {current_shard_id:04d}: {current_shard_rows:,} rows.")

    manifest = pd.DataFrame(manifest_rows)
    manifest.to_csv(MANIFEST_FILE, index=False)

    n_shards = len(manifest)
    max_shard_id = int(manifest["shard_id"].max()) if n_shards else -1
    print(f"Saved shard manifest to {MANIFEST_FILE}")
    print(f"Total referenced-work rows split: {total_rows:,}")
    print(f"Total shards written: {n_shards:,}")
    if max_shard_id >= 0:
        print(f"Use Slurm array range: 0-{max_shard_id}")


def main() -> None:
    check_input(INPUT_FILE)
    prepare_outputs()
    split_referenced_works()


if __name__ == "__main__":
    main()
