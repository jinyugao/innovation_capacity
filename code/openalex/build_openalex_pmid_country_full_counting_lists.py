"""Build one full-counting country-list row per OpenAlex PMID.

The input is the reusable long-format PMID-country full-counting table. The
output contains one row per PMID with sorted country-code and country-name lists.

Default NA parsing is disabled so the ISO country code "NA" for Namibia is
preserved as a valid country code instead of being interpreted as missing.
"""

from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path

import pandas as pd


OPENALEX_DIR = Path("/xdisk/sebratt/jinyugao/data/products/openalex/flattened_snapshot_2025")
INPUT_FILE = OPENALEX_DIR / "openalex_pmid_country_full_counting.csv.gz"
OUTPUT_FILE = OPENALEX_DIR / "openalex_pmid_country_full_counting_lists.csv.gz"

CHUNK_SIZE = 1_000_000
OVERWRITE = False
LIST_SEPARATOR = ";"

INPUT_COLUMNS = [
    "pmid",
    "institution_country_code",
    "institution_country",
]
OUTPUT_COLUMNS = [
    "pmid",
    "pmid_country_codes_full_counting",
    "pmid_country_full_counting",
    "n_countries_for_pmid",
]


def check_input(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing required input file: {path}")


def check_output(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not OVERWRITE:
        raise FileExistsError(
            "Output file already exists. Set OVERWRITE = True to replace it:\n"
            f"{path}"
        )
    if path.exists() and OVERWRITE:
        path.unlink()


def normalize_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def normalize_pmid(value: object) -> str:
    text = normalize_text(value)
    if not text:
        return ""

    if text.endswith(".0") and text[:-2].isdigit():
        return text[:-2]

    match = re.search(r"(\d+)(?:/)?$", text)
    if match:
        return match.group(1)

    return text


def build_pmid_country_lists(
    input_file: Path,
) -> tuple[dict[str, set[str]], dict[str, set[str]]]:
    country_codes_by_pmid: defaultdict[str, set[str]] = defaultdict(set)
    country_names_by_pmid: defaultdict[str, set[str]] = defaultdict(set)
    total_rows = 0
    kept_rows = 0

    reader = pd.read_csv(
        input_file,
        compression="gzip",
        usecols=INPUT_COLUMNS,
        chunksize=CHUNK_SIZE,
        dtype="string",
        keep_default_na=False,
        na_filter=False,
    )

    for chunk_number, chunk in enumerate(reader, start=1):
        total_rows += len(chunk)

        for row in chunk.itertuples(index=False):
            pmid = normalize_pmid(row.pmid)
            country_code = normalize_text(row.institution_country_code)
            country_name = normalize_text(row.institution_country)

            if not pmid or not country_code:
                continue

            country_codes_by_pmid[pmid].add(country_code)
            if country_name:
                country_names_by_pmid[pmid].add(country_name)
            kept_rows += 1

        print(
            f"Chunk {chunk_number:,}: read {total_rows:,} PMID-country rows; "
            f"kept {kept_rows:,}; unique PMIDs {len(country_codes_by_pmid):,}."
        )

    print(f"Total PMID-country rows read: {total_rows:,}")
    print(f"Rows with normalized PMID and country code: {kept_rows:,}")
    print(f"Unique PMIDs: {len(country_codes_by_pmid):,}")
    return country_codes_by_pmid, country_names_by_pmid


def pmid_sort_key(value: str) -> tuple[int, int | str]:
    if value.isdigit():
        return 0, int(value)
    return 1, value


def write_pmid_country_lists(
    country_codes_by_pmid: dict[str, set[str]],
    country_names_by_pmid: dict[str, set[str]],
    output_file: Path,
) -> None:
    rows = []
    for pmid in sorted(country_codes_by_pmid, key=pmid_sort_key):
        country_codes = sorted(country_codes_by_pmid[pmid])
        country_names = sorted(country_names_by_pmid.get(pmid, set()))
        rows.append(
            {
                "pmid": pmid,
                "pmid_country_codes_full_counting": LIST_SEPARATOR.join(country_codes),
                "pmid_country_full_counting": LIST_SEPARATOR.join(country_names),
                "n_countries_for_pmid": len(country_codes),
            }
        )

    output = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)
    output.to_csv(output_file, index=False, compression="gzip")
    print(f"Saved PMID country full-counting lists to {output_file}")
    print(f"Rows written: {len(output):,}")


def main() -> None:
    check_input(INPUT_FILE)
    check_output(OUTPUT_FILE)
    country_codes_by_pmid, country_names_by_pmid = build_pmid_country_lists(INPUT_FILE)
    write_pmid_country_lists(
        country_codes_by_pmid,
        country_names_by_pmid,
        OUTPUT_FILE,
    )


if __name__ == "__main__":
    main()
