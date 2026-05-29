"""Combine OpenAlex citation partial counts into work-level citation tables."""

from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path

import pandas as pd


OPENALEX_DIR = Path("/xdisk/sebratt/jinyugao/data/products/openalex/flattened_snapshot_2025")
WORKS_PUBLICATION_YEAR_FILE = OPENALEX_DIR / "openalex_works_publication_year.csv.gz"
WORKS_IDS_FILE = OPENALEX_DIR / "openalex_works_ids.csv.gz"
PIPELINE_DIR = OPENALEX_DIR / "citation_pipeline"
PARTIAL_DIR = PIPELINE_DIR / "partial_counts"
MANIFEST_FILE = PIPELINE_DIR / "openalex_referenced_works_shard_manifest.csv"

OUTPUT_FILE = OPENALEX_DIR / "openalex_work_citations_reference_counts_through_2024.csv.gz"
OUTPUT_PMID_FILE = (
    OPENALEX_DIR / "openalex_work_citations_reference_counts_through_2024_pmid.csv.gz"
)

MAX_CITING_PUBLICATION_YEAR = 2024
CHUNK_SIZE = 1_000_000
OVERWRITE = False
WRITE_PMID_SUBSET = True

COUNT_COLUMNS = [
    "n_references",
    "citation_C3",
    "citation_C5",
    "citation_C10",
    "citation_through_2024",
]
OUTPUT_COLUMNS = [
    "work_id",
    "pmid",
    "publication_year",
    "n_references",
    "citation_C3",
    "citation_C5",
    "citation_C10",
    "citation_through_2024",
    "has_complete_C3_window",
    "has_complete_C5_window",
    "has_complete_C10_window",
]


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


def partial_file(shard_id: int) -> Path:
    return PARTIAL_DIR / f"openalex_citation_partial_counts_shard_{shard_id:04d}.csv.gz"


def expected_partial_files() -> list[Path]:
    if not MANIFEST_FILE.exists():
        return sorted(PARTIAL_DIR.glob("openalex_citation_partial_counts_shard_*.csv.gz"))

    manifest = pd.read_csv(MANIFEST_FILE, usecols=["shard_id"])
    return [partial_file(int(shard_id)) for shard_id in manifest["shard_id"]]


def validate_partial_files(paths: list[Path]) -> None:
    if not paths:
        raise FileNotFoundError(f"No partial count files found in {PARTIAL_DIR}.")

    missing = [str(path) for path in paths if not path.exists()]
    if missing:
        preview = "\n".join(missing[:20])
        if len(missing) > 20:
            preview += f"\n... and {len(missing) - 20} more"
        raise FileNotFoundError(
            "Missing expected partial count file(s). Re-run failed array task(s):\n"
            f"{preview}"
        )


def extract_pmid(value: object) -> str | None:
    if pd.isna(value):
        return None

    value_text = str(value).strip()
    match = re.search(r"(\d+)$", value_text)
    if not match:
        return None
    return match.group(1)


def load_pmid_lookup(path: Path) -> dict[str, str]:
    if not path.exists():
        print(f"PMID lookup file not found, skipping PMID subset: {path}")
        return {}

    pmid_lookup: dict[str, str] = {}
    total_rows = 0
    kept_rows = 0

    reader = pd.read_csv(
        path,
        compression="gzip",
        usecols=["work_id", "pmid"],
        chunksize=CHUNK_SIZE,
        dtype="string",
    )

    for chunk_number, chunk in enumerate(reader, start=1):
        total_rows += len(chunk)
        chunk = chunk.dropna(subset=["work_id", "pmid"]).copy()
        chunk["pmid"] = chunk["pmid"].map(extract_pmid)
        chunk = chunk.dropna(subset=["pmid"])
        kept_rows += len(chunk)

        for row in chunk.itertuples(index=False):
            pmid_lookup[str(row.work_id)] = str(row.pmid)

        print(
            f"PMID chunk {chunk_number:,}: read {total_rows:,} rows; "
            f"kept {kept_rows:,} PMID-linked works."
        )

    print(f"Loaded PMID lookup for {len(pmid_lookup):,} works.")
    return pmid_lookup


def add_partial_counts(
    counters: dict[str, defaultdict[str, int]],
    partial_file_path: Path,
) -> None:
    reader = pd.read_csv(
        partial_file_path,
        compression="gzip",
        chunksize=CHUNK_SIZE,
        dtype={"work_id": "string"},
    )

    file_rows = 0
    for chunk in reader:
        file_rows += len(chunk)
        chunk = chunk.dropna(subset=["work_id"])
        for column in COUNT_COLUMNS:
            chunk[column] = pd.to_numeric(chunk[column], errors="coerce").fillna(0)
            grouped = chunk.groupby("work_id", sort=False)[column].sum()
            grouped = grouped[grouped != 0]
            for work_id, value in grouped.items():
                counters[column][str(work_id)] += int(value)

    print(f"Loaded {file_rows:,} rows from {partial_file_path.name}.")


def load_combined_counts(partial_files: list[Path]) -> dict[str, defaultdict[str, int]]:
    counters = {column: defaultdict(int) for column in COUNT_COLUMNS}

    for file_number, path in enumerate(partial_files, start=1):
        add_partial_counts(counters, path)
        print(f"Combined partial file {file_number:,}/{len(partial_files):,}.")

    for column in COUNT_COLUMNS:
        print(f"{column}: {len(counters[column]):,} work IDs with positive counts.")
    return counters


def build_output_chunk(
    chunk: pd.DataFrame,
    pmid_lookup: dict[str, str],
    counters: dict[str, defaultdict[str, int]],
) -> pd.DataFrame:
    chunk = chunk.dropna(subset=["work_id", "publication_year"]).copy()
    chunk = chunk[chunk["publication_year"] <= MAX_CITING_PUBLICATION_YEAR]
    chunk["work_id"] = chunk["work_id"].astype("string")
    chunk["publication_year"] = chunk["publication_year"].astype("int64")

    work_ids = chunk["work_id"].astype(str)
    chunk["pmid"] = work_ids.map(pmid_lookup).astype("string")
    for column in COUNT_COLUMNS:
        chunk[column] = work_ids.map(counters[column]).fillna(0).astype("int64")

    chunk["has_complete_C3_window"] = (
        chunk["publication_year"] + 3 <= MAX_CITING_PUBLICATION_YEAR
    )
    chunk["has_complete_C5_window"] = (
        chunk["publication_year"] + 5 <= MAX_CITING_PUBLICATION_YEAR
    )
    chunk["has_complete_C10_window"] = (
        chunk["publication_year"] + 10 <= MAX_CITING_PUBLICATION_YEAR
    )

    return chunk.reindex(columns=OUTPUT_COLUMNS)


def write_outputs(
    pmid_lookup: dict[str, str],
    counters: dict[str, defaultdict[str, int]],
) -> None:
    wrote_header = False
    wrote_pmid_header = False
    total_rows = 0
    total_pmid_rows = 0

    reader = pd.read_csv(
        WORKS_PUBLICATION_YEAR_FILE,
        compression="gzip",
        usecols=["work_id", "publication_year"],
        chunksize=CHUNK_SIZE,
        dtype={"work_id": "string", "publication_year": "Int64"},
    )

    for chunk_number, chunk in enumerate(reader, start=1):
        output_chunk = build_output_chunk(chunk, pmid_lookup, counters)
        if output_chunk.empty:
            continue

        output_chunk.to_csv(
            OUTPUT_FILE,
            mode="a",
            index=False,
            compression="gzip",
            header=not wrote_header,
        )
        wrote_header = True
        total_rows += len(output_chunk)

        if WRITE_PMID_SUBSET:
            pmid_chunk = output_chunk.dropna(subset=["pmid"])
            if not pmid_chunk.empty:
                pmid_chunk.to_csv(
                    OUTPUT_PMID_FILE,
                    mode="a",
                    index=False,
                    compression="gzip",
                    header=not wrote_pmid_header,
                )
                wrote_pmid_header = True
                total_pmid_rows += len(pmid_chunk)

        print(
            f"Output chunk {chunk_number:,}: wrote {total_rows:,} work rows; "
            f"wrote {total_pmid_rows:,} PMID-linked rows."
        )

    if not wrote_header:
        pd.DataFrame(columns=OUTPUT_COLUMNS).to_csv(
            OUTPUT_FILE, index=False, compression="gzip"
        )
    if WRITE_PMID_SUBSET and not wrote_pmid_header:
        pd.DataFrame(columns=OUTPUT_COLUMNS).to_csv(
            OUTPUT_PMID_FILE, index=False, compression="gzip"
        )

    print(f"Saved work-level citation table to {OUTPUT_FILE}")
    if WRITE_PMID_SUBSET:
        print(f"Saved PMID-linked citation table to {OUTPUT_PMID_FILE}")


def main() -> None:
    input_paths = [WORKS_PUBLICATION_YEAR_FILE, WORKS_IDS_FILE]
    check_inputs(input_paths)
    check_output(OUTPUT_FILE, OVERWRITE)
    if WRITE_PMID_SUBSET:
        check_output(OUTPUT_PMID_FILE, OVERWRITE)

    partial_files = expected_partial_files()
    validate_partial_files(partial_files)
    counters = load_combined_counts(partial_files)
    pmid_lookup = load_pmid_lookup(WORKS_IDS_FILE) if WRITE_PMID_SUBSET else {}
    write_outputs(pmid_lookup, counters)


if __name__ == "__main__":
    main()
