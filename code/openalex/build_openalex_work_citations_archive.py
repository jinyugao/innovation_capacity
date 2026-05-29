"""Build OpenAlex work-level citation windows and reference counts.

The citation unit is the OpenAlex work. Citation counts are calculated from the
full OpenAlex referenced-works table, not only PMID-linked works. Window counts
follow the MAG-style inclusive YearDiff definition:

    YearDiff = citing_publication_year - cited_publication_year
    C3  includes 0 <= YearDiff <= 3
    C5  includes 0 <= YearDiff <= 5
    C10 includes 0 <= YearDiff <= 10

Because the 2025 snapshot is incomplete, citing works are limited to publication
year <= 2024. C3/C5/C10 are raw observed counts through 2024, and separate
complete-window flags are included for downstream filtering.
"""

from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path

import pandas as pd
from pandas.errors import EmptyDataError


OPENALEX_DIR = Path(
    "/xdisk/sebratt/jinyugao/data/products/openalex/flattened_snapshot_2025"
)
WORKS_PUBLICATION_YEAR_FILE = OPENALEX_DIR / "openalex_works_publication_year.csv.gz"
WORKS_REFERENCED_WORKS_FILE = OPENALEX_DIR / "openalex_works_referenced_works.csv.gz"
WORKS_IDS_FILE = OPENALEX_DIR / "openalex_works_ids.csv.gz"

OUTPUT_FILE = OPENALEX_DIR / "openalex_work_citations_reference_counts_through_2024.csv.gz"
OUTPUT_PMID_FILE = (
    OPENALEX_DIR / "openalex_work_citations_reference_counts_through_2024_pmid.csv.gz"
)

MAX_CITING_PUBLICATION_YEAR = 2024
CHUNK_SIZE = 1_000_000
OVERWRITE = False
WRITE_PMID_SUBSET = True

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


def extract_pmid(value: object) -> str | None:
    if pd.isna(value):
        return None

    value_text = str(value).strip()
    match = re.search(r"(\d+)$", value_text)
    if not match:
        return None
    return match.group(1)


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


def add_counts(counter: defaultdict[str, int], counts: pd.Series) -> None:
    for key, value in counts.items():
        counter[str(key)] += int(value)


def update_counts_from_references(
    referenced_works_file: Path,
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
            referenced_works_file,
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
            add_counts(
                citation_all,
                chunk.groupby("referenced_work_id", sort=False).size(),
            )
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
            f"Reference chunk {chunk_number:,}: read {total_edges:,} edges; "
            f"kept {kept_reference_edges:,} reference edges; "
            f"kept {kept_citation_edges:,} citation edges."
        )

    print("Finished scanning referenced works.")
    print(f"Total edges read: {total_edges:,}")
    print(f"Reference edges with citing year through 2024: {kept_reference_edges:,}")
    print(f"Citation edges with both years and nonnegative YearDiff: {kept_citation_edges:,}")
    print(f"Edges missing citing year or after 2024: {missing_citing_year:,}")
    print(f"Edges missing cited year or cited work after 2024: {missing_cited_year:,}")
    print(f"Edges with negative YearDiff: {negative_year_diff:,}")

    return n_references, citation_c3, citation_c5, citation_c10, citation_all


def build_output_chunk(
    chunk: pd.DataFrame,
    pmid_lookup: dict[str, str],
    n_references: defaultdict[str, int],
    citation_c3: defaultdict[str, int],
    citation_c5: defaultdict[str, int],
    citation_c10: defaultdict[str, int],
    citation_all: defaultdict[str, int],
) -> pd.DataFrame:
    chunk = chunk.dropna(subset=["work_id", "publication_year"]).copy()
    chunk = chunk[chunk["publication_year"] <= MAX_CITING_PUBLICATION_YEAR]
    chunk["work_id"] = chunk["work_id"].astype("string")
    chunk["publication_year"] = chunk["publication_year"].astype("int64")

    work_ids = chunk["work_id"].astype(str)
    chunk["pmid"] = work_ids.map(pmid_lookup).astype("string")
    chunk["n_references"] = work_ids.map(n_references).fillna(0).astype("int64")
    chunk["citation_C3"] = work_ids.map(citation_c3).fillna(0).astype("int64")
    chunk["citation_C5"] = work_ids.map(citation_c5).fillna(0).astype("int64")
    chunk["citation_C10"] = work_ids.map(citation_c10).fillna(0).astype("int64")
    chunk["citation_through_2024"] = (
        work_ids.map(citation_all).fillna(0).astype("int64")
    )
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
    publication_year_file: Path,
    output_file: Path,
    output_pmid_file: Path,
    pmid_lookup: dict[str, str],
    n_references: defaultdict[str, int],
    citation_c3: defaultdict[str, int],
    citation_c5: defaultdict[str, int],
    citation_c10: defaultdict[str, int],
    citation_all: defaultdict[str, int],
) -> None:
    wrote_header = False
    wrote_pmid_header = False
    total_rows = 0
    total_pmid_rows = 0

    reader = pd.read_csv(
        publication_year_file,
        compression="gzip",
        usecols=["work_id", "publication_year"],
        chunksize=CHUNK_SIZE,
        dtype={"work_id": "string", "publication_year": "Int64"},
    )

    for chunk_number, chunk in enumerate(reader, start=1):
        output_chunk = build_output_chunk(
            chunk,
            pmid_lookup,
            n_references,
            citation_c3,
            citation_c5,
            citation_c10,
            citation_all,
        )
        if output_chunk.empty:
            continue

        output_chunk.to_csv(
            output_file,
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
                    output_pmid_file,
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
            output_file, index=False, compression="gzip"
        )
    if WRITE_PMID_SUBSET and not wrote_pmid_header:
        pd.DataFrame(columns=OUTPUT_COLUMNS).to_csv(
            output_pmid_file, index=False, compression="gzip"
        )

    print(f"Saved work-level citation table to {output_file}")
    if WRITE_PMID_SUBSET:
        print(f"Saved PMID-linked citation table to {output_pmid_file}")


def main() -> None:
    input_paths = [WORKS_PUBLICATION_YEAR_FILE, WORKS_REFERENCED_WORKS_FILE]
    if WRITE_PMID_SUBSET:
        input_paths.append(WORKS_IDS_FILE)
    check_inputs(input_paths)
    check_output(OUTPUT_FILE, OVERWRITE)
    if WRITE_PMID_SUBSET:
        check_output(OUTPUT_PMID_FILE, OVERWRITE)

    work_year = load_work_year_lookup(WORKS_PUBLICATION_YEAR_FILE)
    pmid_lookup = load_pmid_lookup(WORKS_IDS_FILE) if WRITE_PMID_SUBSET else {}
    (
        n_references,
        citation_c3,
        citation_c5,
        citation_c10,
        citation_all,
    ) = update_counts_from_references(WORKS_REFERENCED_WORKS_FILE, work_year)
    write_outputs(
        publication_year_file=WORKS_PUBLICATION_YEAR_FILE,
        output_file=OUTPUT_FILE,
        output_pmid_file=OUTPUT_PMID_FILE,
        pmid_lookup=pmid_lookup,
        n_references=n_references,
        citation_c3=citation_c3,
        citation_c5=citation_c5,
        citation_c10=citation_c10,
        citation_all=citation_all,
    )


if __name__ == "__main__":
    main()
