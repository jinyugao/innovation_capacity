"""Build a PMID-linked OpenAlex authorship-institution-work table."""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd


OPENALEX_DIR = Path("/xdisk/sebratt/jinyugao/data/products/openalex/flattened_snapshot_2025")

WORKS_IDS_FILE = OPENALEX_DIR / "openalex_works_ids.csv.gz"
AUTHORSHIPS_FILE = OPENALEX_DIR / "openalex_works_authorships.csv.gz"
INSTITUTIONS_FILE = OPENALEX_DIR / "openalex_institutions.csv.gz"
INSTITUTIONS_GEO_FILE = OPENALEX_DIR / "openalex_institutions_geo.csv.gz"

OUTPUT_FILE = OPENALEX_DIR / "openalex_authorships_institutions_workids.csv.gz"

CHUNK_SIZE = 1_000_000
OVERWRITE = False

WORKS_IDS_COLUMNS = ["work_id", "doi", "pmid", "pmcid"]
AUTHORSHIPS_COLUMNS = [
    "work_id",
    "author_position",
    "author_id",
    "institution_id",
    "raw_affiliation_string",
]
OUTPUT_COLUMNS = [
    "work_id",
    "pmid",
    "pmid_raw",
    "doi",
    "pmcid",
    "author_position",
    "author_id",
    "institution_id",
    "institution_ror",
    "institution_display_name",
    "institution_country_code",
    "institution_country",
    "institution_type",
    "raw_affiliation_string",
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


def load_pmid_work_ids(works_ids_file: Path) -> pd.DataFrame:
    chunks = []
    total_rows = 0
    kept_rows = 0

    reader = pd.read_csv(
        works_ids_file,
        compression="gzip",
        usecols=WORKS_IDS_COLUMNS,
        chunksize=CHUNK_SIZE,
        dtype="string",
    )

    for chunk_number, chunk in enumerate(reader, start=1):
        total_rows += len(chunk)
        chunk = chunk.dropna(subset=["pmid"]).copy()
        chunk["pmid_raw"] = chunk["pmid"]
        chunk["pmid"] = chunk["pmid"].map(extract_pmid)
        chunk = chunk.dropna(subset=["pmid", "work_id"])
        chunk = chunk.drop_duplicates(subset=["work_id"])
        kept_rows += len(chunk)
        chunks.append(chunk)

        print(
            f"Works IDs chunk {chunk_number:,}: read {total_rows:,} total rows; "
            f"kept {kept_rows:,} PMID-linked work rows."
        )

    if not chunks:
        raise ValueError(f"No PMID-linked works found in {works_ids_file}.")

    works_ids = pd.concat(chunks, ignore_index=True)
    works_ids = works_ids.drop_duplicates(subset=["work_id"])
    print(f"Loaded {len(works_ids):,} unique PMID-linked works.")
    return works_ids


def load_institution_lookup(
    institutions_file: Path,
    institutions_geo_file: Path,
) -> pd.DataFrame:
    institutions = pd.read_csv(
        institutions_file,
        compression="gzip",
        usecols=["institution_id", "ror", "display_name", "country_code", "type"],
        dtype="string",
    ).rename(
        columns={
            "ror": "institution_ror",
            "display_name": "institution_display_name",
            "country_code": "institution_country_code",
            "type": "institution_type",
        }
    )

    institutions_geo = pd.read_csv(
        institutions_geo_file,
        compression="gzip",
        usecols=["institution_id", "country"],
        dtype="string",
    ).rename(columns={"country": "institution_country"})

    institution_lookup = institutions.merge(
        institutions_geo.drop_duplicates(subset=["institution_id"]),
        on="institution_id",
        how="left",
    )
    institution_lookup = institution_lookup.drop_duplicates(subset=["institution_id"])
    print(f"Loaded {len(institution_lookup):,} institution rows.")
    return institution_lookup


def build_authorship_table(
    works_ids: pd.DataFrame,
    institution_lookup: pd.DataFrame,
    authorships_file: Path,
    output_file: Path,
) -> None:
    work_ids = set(works_ids["work_id"])
    total_rows = 0
    kept_rows = 0
    wrote_header = False

    reader = pd.read_csv(
        authorships_file,
        compression="gzip",
        usecols=AUTHORSHIPS_COLUMNS,
        chunksize=CHUNK_SIZE,
        dtype="string",
    )

    for chunk_number, chunk in enumerate(reader, start=1):
        total_rows += len(chunk)
        chunk = chunk[chunk["work_id"].isin(work_ids)].copy()

        if chunk.empty:
            print(
                f"Authorships chunk {chunk_number:,}: read {total_rows:,} total rows; "
                f"kept {kept_rows:,} rows so far."
            )
            continue

        chunk = chunk.merge(works_ids, on="work_id", how="left")
        chunk = chunk.merge(institution_lookup, on="institution_id", how="left")
        chunk = chunk.reindex(columns=OUTPUT_COLUMNS)
        kept_rows += len(chunk)

        chunk.to_csv(
            output_file,
            mode="a",
            index=False,
            compression="gzip",
            header=not wrote_header,
        )
        wrote_header = True

        print(
            f"Authorships chunk {chunk_number:,}: read {total_rows:,} total rows; "
            f"wrote {kept_rows:,} PMID-linked authorship-institution rows."
        )

    if not wrote_header:
        pd.DataFrame(columns=OUTPUT_COLUMNS).to_csv(
            output_file, index=False, compression="gzip"
        )

    print(f"Saved authorship-institution-work table to {output_file}")
    print(f"Total authorship rows read: {total_rows:,}")
    print(f"Total rows written: {kept_rows:,}")


def main() -> None:
    check_inputs(
        [
            WORKS_IDS_FILE,
            AUTHORSHIPS_FILE,
            INSTITUTIONS_FILE,
            INSTITUTIONS_GEO_FILE,
        ]
    )
    check_output(OUTPUT_FILE, OVERWRITE)

    works_ids = load_pmid_work_ids(WORKS_IDS_FILE)
    institution_lookup = load_institution_lookup(INSTITUTIONS_FILE, INSTITUTIONS_GEO_FILE)
    build_authorship_table(
        works_ids=works_ids,
        institution_lookup=institution_lookup,
        authorships_file=AUTHORSHIPS_FILE,
        output_file=OUTPUT_FILE,
    )


if __name__ == "__main__":
    main()
