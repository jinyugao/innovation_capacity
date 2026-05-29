"""Build citation and reference-count partials for one OpenAlex reference shard.

This script is intended for a Slurm array job. Each task processes one physical
referenced-works shard and writes one partial count table. The partial tables are
combined by combine_openalex_citation_partial_counts.py.
"""

from __future__ import annotations

import os
from collections import defaultdict
from pathlib import Path

import pandas as pd
from pandas.errors import EmptyDataError


OPENALEX_DIR = Path("/xdisk/sebratt/jinyugao/data/products/openalex/flattened_snapshot_2025")
WORKS_PUBLICATION_YEAR_FILE = OPENALEX_DIR / "openalex_works_publication_year.csv.gz"
PIPELINE_DIR = OPENALEX_DIR / "citation_pipeline"
SHARD_DIR = PIPELINE_DIR / "reference_shards"
PARTIAL_DIR = PIPELINE_DIR / "partial_counts"
MANIFEST_FILE = PIPELINE_DIR / "openalex_referenced_works_shard_manifest.csv"

MAX_CITING_PUBLICATION_YEAR = 2024
CHUNK_SIZE = 1_000_000
OVERWRITE = False

OUTPUT_COLUMNS = [
    "work_id",
    "n_references",
    "citation_C3",
    "citation_C5",
    "citation_C10",
    "citation_through_2024",
]


def get_shard_id() -> int:
    task_id = os.environ.get("SLURM_ARRAY_TASK_ID")
    if task_id is None:
        raise RuntimeError("SLURM_ARRAY_TASK_ID not found.")
    return int(task_id)


def shard_file(shard_id: int) -> Path:
    return SHARD_DIR / f"openalex_works_referenced_works_shard_{shard_id:04d}.csv.gz"


def partial_file(shard_id: int) -> Path:
    return PARTIAL_DIR / f"openalex_citation_partial_counts_shard_{shard_id:04d}.csv.gz"


def check_inputs(paths: list[Path]) -> None:
    missing_files = [str(path) for path in paths if not path.exists()]
    if missing_files:
        missing = "\n".join(missing_files)
        raise FileNotFoundError(f"Missing required input file(s):\n{missing}")


def check_output(path: Path, overwrite: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not overwrite:
        raise FileExistsError(
            "Output file already exists. Set OVERWRITE = True to replace it:\n"
            f"{path}"
        )
    if path.exists() and overwrite:
        path.unlink()


def shard_id_in_manifest(shard_id: int) -> bool:
    if not MANIFEST_FILE.exists():
        return True
    manifest = pd.read_csv(MANIFEST_FILE, usecols=["shard_id"])
    return shard_id in set(manifest["shard_id"].astype(int))


def load_work_year_lookup(path: Path) -> dict[str, int]:
    work_year: dict[str, int] = {}
    total_rows = 0
    kept_rows = 0

    reader = pd.read_csv(
        path,
        compression="gzip",
        usecols=["work_id", "publication_year"],
        chunksize=CHUNK_SIZE,
        dtype={"work_id": "string", "publication_year": "Int64"},
    )

    for chunk_number, chunk in enumerate(reader, start=1):
        total_rows += len(chunk)
        chunk = chunk.dropna(subset=["work_id", "publication_year"])
        chunk = chunk[chunk["publication_year"] <= MAX_CITING_PUBLICATION_YEAR]
        kept_rows += len(chunk)

        for row in chunk.itertuples(index=False):
            work_year[str(row.work_id)] = int(row.publication_year)

        print(
            f"Publication-year chunk {chunk_number:,}: read {total_rows:,} rows; "
            f"kept {kept_rows:,} works through {MAX_CITING_PUBLICATION_YEAR}."
        )

    print(f"Loaded publication-year lookup for {len(work_year):,} works.")
    return work_year


def add_counts(counter: defaultdict[str, int], counts: pd.Series) -> None:
    for key, value in counts.items():
        counter[str(key)] += int(value)


def update_counts_from_shard(
    shard_path: Path,
    work_year: dict[str, int],
) -> tuple[
    defaultdict[str, int],
    defaultdict[str, int],
    defaultdict[str, int],
    defaultdict[str, int],
    defaultdict[str, int],
]:
    n_references: defaultdict[str, int] = defaultdict(int)
    citation_c3: defaultdict[str, int] = defaultdict(int)
    citation_c5: defaultdict[str, int] = defaultdict(int)
    citation_c10: defaultdict[str, int] = defaultdict(int)
    citation_all: defaultdict[str, int] = defaultdict(int)

    total_edges = 0
    kept_reference_edges = 0
    kept_citation_edges = 0
    missing_citing_year = 0
    missing_cited_year = 0
    negative_year_diff = 0

    try:
        reader = pd.read_csv(
            shard_path,
            compression="gzip",
            usecols=["work_id", "referenced_work_id"],
            chunksize=CHUNK_SIZE,
            dtype={"work_id": "string", "referenced_work_id": "string"},
        )
    except EmptyDataError:
        return n_references, citation_c3, citation_c5, citation_c10, citation_all

    for chunk_number, chunk in enumerate(reader, start=1):
        total_edges += len(chunk)
        chunk = chunk.dropna(subset=["work_id", "referenced_work_id"]).copy()
        chunk["work_id"] = chunk["work_id"].astype("string").str.strip()
        chunk["referenced_work_id"] = (
            chunk["referenced_work_id"].astype("string").str.strip()
        )

        chunk["citing_year"] = chunk["work_id"].map(work_year)
        missing_citing_year += int(chunk["citing_year"].isna().sum())
        chunk = chunk.dropna(subset=["citing_year"])
        kept_reference_edges += len(chunk)

        if not chunk.empty:
            add_counts(n_references, chunk.groupby("work_id", sort=False).size())

        chunk["cited_year"] = chunk["referenced_work_id"].map(work_year)
        missing_cited_year += int(chunk["cited_year"].isna().sum())
        chunk = chunk.dropna(subset=["cited_year"])

        if not chunk.empty:
            chunk["year_diff"] = (
                chunk["citing_year"].astype("int64")
                - chunk["cited_year"].astype("int64")
            )
            negative_year_diff += int((chunk["year_diff"] < 0).sum())
            chunk = chunk[chunk["year_diff"] >= 0]

        kept_citation_edges += len(chunk)
        if not chunk.empty:
            add_counts(citation_all, chunk.groupby("referenced_work_id", sort=False).size())
            add_counts(
                citation_c3,
                chunk[chunk["year_diff"] <= 3]
                .groupby("referenced_work_id", sort=False)
                .size(),
            )
            add_counts(
                citation_c5,
                chunk[chunk["year_diff"] <= 5]
                .groupby("referenced_work_id", sort=False)
                .size(),
            )
            add_counts(
                citation_c10,
                chunk[chunk["year_diff"] <= 10]
                .groupby("referenced_work_id", sort=False)
                .size(),
            )

        print(
            f"Shard chunk {chunk_number:,}: read {total_edges:,} edges; "
            f"kept {kept_reference_edges:,} reference edges; "
            f"kept {kept_citation_edges:,} citation edges."
        )

    print(f"Finished shard {shard_path.name}.")
    print(f"Total edges read: {total_edges:,}")
    print(f"Reference edges with citing year through 2024: {kept_reference_edges:,}")
    print(f"Citation edges with both years and nonnegative YearDiff: {kept_citation_edges:,}")
    print(f"Edges missing citing year or after 2024: {missing_citing_year:,}")
    print(f"Edges missing cited year or cited work after 2024: {missing_cited_year:,}")
    print(f"Edges with negative YearDiff: {negative_year_diff:,}")

    return n_references, citation_c3, citation_c5, citation_c10, citation_all


def build_partial_output(
    n_references: defaultdict[str, int],
    citation_c3: defaultdict[str, int],
    citation_c5: defaultdict[str, int],
    citation_c10: defaultdict[str, int],
    citation_all: defaultdict[str, int],
) -> pd.DataFrame:
    work_ids = sorted(
        set(n_references)
        | set(citation_c3)
        | set(citation_c5)
        | set(citation_c10)
        | set(citation_all)
    )

    rows = []
    for work_id in work_ids:
        rows.append(
            {
                "work_id": work_id,
                "n_references": n_references.get(work_id, 0),
                "citation_C3": citation_c3.get(work_id, 0),
                "citation_C5": citation_c5.get(work_id, 0),
                "citation_C10": citation_c10.get(work_id, 0),
                "citation_through_2024": citation_all.get(work_id, 0),
            }
        )
    return pd.DataFrame(rows, columns=OUTPUT_COLUMNS)


def main() -> None:
    shard_id = get_shard_id()

    if not shard_id_in_manifest(shard_id):
        print(f"Shard {shard_id:04d} is not in the manifest; skipping.")
        return

    shard_path = shard_file(shard_id)
    output_file = partial_file(shard_id)
    check_inputs([WORKS_PUBLICATION_YEAR_FILE, shard_path])
    check_output(output_file, OVERWRITE)

    print(f"Building partial citation counts for shard {shard_id:04d}: {shard_path}")
    work_year = load_work_year_lookup(WORKS_PUBLICATION_YEAR_FILE)
    (
        n_references,
        citation_c3,
        citation_c5,
        citation_c10,
        citation_all,
    ) = update_counts_from_shard(shard_path, work_year)
    output = build_partial_output(
        n_references=n_references,
        citation_c3=citation_c3,
        citation_c5=citation_c5,
        citation_c10=citation_c10,
        citation_all=citation_all,
    )
    output.to_csv(output_file, index=False, compression="gzip")
    print(f"Saved partial citation counts to {output_file}")
    print(f"Rows written: {len(output):,}")


if __name__ == "__main__":
    main()
