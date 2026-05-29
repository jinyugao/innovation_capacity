"""Build a PMID-country full-counting table from OpenAlex authorship countries.

Each output row is one PMID-country pair. A country receives one full-counting
credit for a PMID if at least one author affiliation on that PMID is associated
with the country. Multiple authors or institutions in the same country do not
create duplicate PMID-country rows.
"""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path

import pandas as pd


OPENALEX_DIR = Path("/xdisk/sebratt/jinyugao/data/products/openalex/flattened_snapshot_2025")
INPUT_FILE = OPENALEX_DIR / "openalex_authorships_institutions_workids_pmid.csv.gz"
OUTPUT_FILE = OPENALEX_DIR / "openalex_pmid_country_full_counting.csv.gz"

CHUNK_SIZE = 1_000_000
OVERWRITE = False

INPUT_COLUMNS = [
    "pmid",
    "work_id",
    "author_id",
    "institution_id",
    "institution_country_code",
    "institution_country",
]
OUTPUT_COLUMNS = [
    "pmid",
    "institution_country_code",
    "institution_country",
    "pmid_country_weight",
    "n_work_ids_for_pmid_country",
    "n_authors_for_pmid_country",
    "n_institutions_for_pmid_country",
    "n_authorship_institution_rows_for_pmid_country",
]


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


def normalize_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def build_pmid_country_counts(input_file: Path) -> dict[tuple[str, str], dict[str, object]]:
    counts: dict[tuple[str, str], dict[str, object]] = {}
    country_names: dict[tuple[str, str], set[str]] = defaultdict(set)
    work_ids: dict[tuple[str, str], set[str]] = defaultdict(set)
    author_ids: dict[tuple[str, str], set[str]] = defaultdict(set)
    institution_ids: dict[tuple[str, str], set[str]] = defaultdict(set)
    row_counts: defaultdict[tuple[str, str], int] = defaultdict(int)

    total_rows = 0
    kept_rows = 0

    reader = pd.read_csv(
        input_file,
        compression="gzip",
        usecols=INPUT_COLUMNS,
        chunksize=CHUNK_SIZE,
        dtype="string",
    )

    for chunk_number, chunk in enumerate(reader, start=1):
        total_rows += len(chunk)
        chunk = chunk.dropna(subset=["pmid", "institution_country_code"]).copy()
        chunk["pmid"] = chunk["pmid"].map(normalize_text)
        chunk["institution_country_code"] = chunk["institution_country_code"].map(
            normalize_text
        )
        chunk = chunk[
            (chunk["pmid"] != "") & (chunk["institution_country_code"] != "")
        ]
        kept_rows += len(chunk)

        for row in chunk.itertuples(index=False):
            row_dict = row._asdict()
            pmid = normalize_text(row_dict["pmid"])
            country_code = normalize_text(row_dict["institution_country_code"])
            key = (pmid, country_code)

            country_name = normalize_text(row_dict.get("institution_country"))
            work_id = normalize_text(row_dict.get("work_id"))
            author_id = normalize_text(row_dict.get("author_id"))
            institution_id = normalize_text(row_dict.get("institution_id"))

            if country_name:
                country_names[key].add(country_name)
            if work_id:
                work_ids[key].add(work_id)
            if author_id:
                author_ids[key].add(author_id)
            if institution_id:
                institution_ids[key].add(institution_id)
            row_counts[key] += 1

        print(
            f"Chunk {chunk_number:,}: read {total_rows:,} rows; "
            f"kept {kept_rows:,} rows with PMID and country code; "
            f"current PMID-country pairs {len(row_counts):,}."
        )

    for key in row_counts:
        country_name_values = sorted(country_names.get(key, set()))
        counts[key] = {
            "pmid": key[0],
            "institution_country_code": key[1],
            "institution_country": "; ".join(country_name_values),
            "pmid_country_weight": 1,
            "n_work_ids_for_pmid_country": len(work_ids.get(key, set())),
            "n_authors_for_pmid_country": len(author_ids.get(key, set())),
            "n_institutions_for_pmid_country": len(institution_ids.get(key, set())),
            "n_authorship_institution_rows_for_pmid_country": row_counts[key],
        }

    print(f"Total rows read: {total_rows:,}")
    print(f"Rows with PMID and country code: {kept_rows:,}")
    print(f"Unique PMID-country pairs: {len(counts):,}")
    return counts


def write_pmid_country_table(
    counts: dict[tuple[str, str], dict[str, object]],
    output_file: Path,
) -> None:
    rows = [counts[key] for key in sorted(counts)]
    output = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)
    output.to_csv(output_file, index=False, compression="gzip")
    print(f"Saved PMID-country full-counting table to {output_file}")
    print(f"Rows written: {len(output):,}")


def main() -> None:
    check_input(INPUT_FILE)
    check_output(OUTPUT_FILE, OVERWRITE)
    counts = build_pmid_country_counts(INPUT_FILE)
    write_pmid_country_table(counts, OUTPUT_FILE)


if __name__ == "__main__":
    main()
