"""Flatten selected OpenAlex snapshot tables into reusable CSV.gz files."""

from __future__ import annotations

import csv
import glob
import gzip
import json
import os
from pathlib import Path


SNAPSHOT_DIR = Path("/contrib/datasets/openalex-snapshot")
OUTPUT_DIR = Path("/xdisk/sebratt/jinyugao/data/products/openalex/flattened_snapshot_2025")

FILES_PER_ENTITY = int(os.environ.get("OPENALEX_DEMO_FILES_PER_ENTITY", "0"))

OUTPUT_FILES = {
    "institutions": OUTPUT_DIR / "openalex_institutions.csv.gz",
    "institutions_geo": OUTPUT_DIR / "openalex_institutions_geo.csv.gz",
    "works_ids": OUTPUT_DIR / "openalex_works_ids.csv.gz",
    "works_authorships": OUTPUT_DIR / "openalex_works_authorships.csv.gz",
    "works_referenced_works": OUTPUT_DIR / "openalex_works_referenced_works.csv.gz",
    "works_publication_year": OUTPUT_DIR / "openalex_works_publication_year.csv.gz",
}

INSTITUTIONS_COLUMNS = [
    "institution_id",
    "ror",
    "display_name",
    "country_code",
    "type",
    "works_count",
    "cited_by_count",
    "updated_date",
]

INSTITUTIONS_GEO_COLUMNS = [
    "institution_id",
    "city",
    "geonames_city_id",
    "region",
    "country_code",
    "country",
    "latitude",
    "longitude",
]

WORKS_IDS_COLUMNS = [
    "work_id",
    "openalex",
    "doi",
    "mag",
    "pmid",
    "pmcid",
]

WORKS_AUTHORSHIPS_COLUMNS = [
    "work_id",
    "author_position",
    "author_id",
    "institution_id",
    "raw_affiliation_string",
]

WORKS_REFERENCED_WORKS_COLUMNS = [
    "work_id",
    "referenced_work_id",
]

WORKS_PUBLICATION_YEAR_COLUMNS = [
    "work_id",
    "publication_year",
]


def init_writer(file_handle, columns: list[str]) -> csv.DictWriter:
    writer = csv.DictWriter(file_handle, fieldnames=columns, extrasaction="ignore")
    writer.writeheader()
    return writer


def iter_snapshot_files(entity: str):
    pattern = SNAPSHOT_DIR / "data" / entity / "*" / "*.gz"
    files_done = 0

    for jsonl_file in glob.glob(str(pattern)):
        print(jsonl_file)
        yield Path(jsonl_file)

        files_done += 1
        if FILES_PER_ENTITY and files_done >= FILES_PER_ENTITY:
            break


def flatten_institutions() -> None:
    with gzip.open(
        OUTPUT_FILES["institutions"], "wt", encoding="utf-8", newline=""
    ) as institutions_csv, gzip.open(
        OUTPUT_FILES["institutions_geo"], "wt", encoding="utf-8", newline=""
    ) as institutions_geo_csv:
        institutions_writer = init_writer(institutions_csv, INSTITUTIONS_COLUMNS)
        institutions_geo_writer = init_writer(
            institutions_geo_csv, INSTITUTIONS_GEO_COLUMNS
        )

        seen_institution_ids = set()

        for jsonl_file in iter_snapshot_files("institutions"):
            with gzip.open(jsonl_file, "rt", encoding="utf-8") as institutions_jsonl:
                for line in institutions_jsonl:
                    if not line.strip():
                        continue

                    institution = json.loads(line)
                    institution_id = institution.get("id")

                    if not institution_id or institution_id in seen_institution_ids:
                        continue

                    seen_institution_ids.add(institution_id)

                    institutions_writer.writerow(
                        {
                            "institution_id": institution_id,
                            "ror": institution.get("ror"),
                            "display_name": institution.get("display_name"),
                            "country_code": institution.get("country_code"),
                            "type": institution.get("type"),
                            "works_count": institution.get("works_count"),
                            "cited_by_count": institution.get("cited_by_count"),
                            "updated_date": institution.get("updated_date"),
                        }
                    )

                    institution_geo = institution.get("geo") or {}
                    if institution_geo:
                        institutions_geo_writer.writerow(
                            {
                                "institution_id": institution_id,
                                "city": institution_geo.get("city"),
                                "geonames_city_id": institution_geo.get(
                                    "geonames_city_id"
                                ),
                                "region": institution_geo.get("region"),
                                "country_code": institution_geo.get("country_code"),
                                "country": institution_geo.get("country"),
                                "latitude": institution_geo.get("latitude"),
                                "longitude": institution_geo.get("longitude"),
                            }
                        )

        print(f"Flattened {len(seen_institution_ids):,} institutions.")


def flatten_works() -> None:
    with gzip.open(
        OUTPUT_FILES["works_ids"], "wt", encoding="utf-8", newline=""
    ) as works_ids_csv, gzip.open(
        OUTPUT_FILES["works_authorships"], "wt", encoding="utf-8", newline=""
    ) as works_authorships_csv, gzip.open(
        OUTPUT_FILES["works_referenced_works"], "wt", encoding="utf-8", newline=""
    ) as works_referenced_works_csv, gzip.open(
        OUTPUT_FILES["works_publication_year"], "wt", encoding="utf-8", newline=""
    ) as works_publication_year_csv:
        works_ids_writer = init_writer(works_ids_csv, WORKS_IDS_COLUMNS)
        works_authorships_writer = init_writer(
            works_authorships_csv, WORKS_AUTHORSHIPS_COLUMNS
        )
        works_referenced_works_writer = init_writer(
            works_referenced_works_csv, WORKS_REFERENCED_WORKS_COLUMNS
        )
        works_publication_year_writer = init_writer(
            works_publication_year_csv, WORKS_PUBLICATION_YEAR_COLUMNS
        )

        works_seen = 0
        authorship_rows = 0
        referenced_work_rows = 0

        for jsonl_file in iter_snapshot_files("works"):
            with gzip.open(jsonl_file, "rt", encoding="utf-8") as works_jsonl:
                for line in works_jsonl:
                    if not line.strip():
                        continue

                    work = json.loads(line)
                    work_id = work.get("id")

                    if not work_id:
                        continue

                    works_seen += 1

                    works_publication_year_writer.writerow(
                        {
                            "work_id": work_id,
                            "publication_year": work.get("publication_year"),
                        }
                    )

                    ids = work.get("ids") or {}
                    if ids:
                        works_ids_writer.writerow(
                            {
                                "work_id": work_id,
                                "openalex": ids.get("openalex"),
                                "doi": ids.get("doi"),
                                "mag": ids.get("mag"),
                                "pmid": ids.get("pmid"),
                                "pmcid": ids.get("pmcid"),
                            }
                        )

                    for authorship in work.get("authorships") or []:
                        author_id = (authorship.get("author") or {}).get("id")
                        if not author_id:
                            continue

                        institutions = authorship.get("institutions") or []
                        institution_ids = [
                            institution.get("id")
                            for institution in institutions
                            if institution and institution.get("id")
                        ]
                        institution_ids = institution_ids or [None]

                        for institution_id in institution_ids:
                            works_authorships_writer.writerow(
                                {
                                    "work_id": work_id,
                                    "author_position": authorship.get(
                                        "author_position"
                                    ),
                                    "author_id": author_id,
                                    "institution_id": institution_id,
                                    "raw_affiliation_string": authorship.get(
                                        "raw_affiliation_string"
                                    ),
                                }
                            )
                            authorship_rows += 1

                    for referenced_work in work.get("referenced_works") or []:
                        if referenced_work:
                            works_referenced_works_writer.writerow(
                                {
                                    "work_id": work_id,
                                    "referenced_work_id": referenced_work,
                                }
                            )
                            referenced_work_rows += 1

        print(f"Flattened {works_seen:,} works.")
        print(f"Wrote {authorship_rows:,} authorship rows.")
        print(f"Wrote {referenced_work_rows:,} referenced-work rows.")


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    flatten_institutions()
    flatten_works()
    print("OpenAlex flattening complete.")


if __name__ == "__main__":
    main()
