"""Flatten the OpenAlex snapshot into CSV.gz tables with robust null handling."""

from __future__ import annotations

import csv
import glob
import gzip
import json
import os
from contextlib import ExitStack
from pathlib import Path
from typing import Any


SNAPSHOT_DIR = Path("/contrib/datasets/openalex-snapshot")
OUTPUT_DIR = Path(
    "/xdisk/sebratt/jinyugao/data/products/openalex/flattened_snapshot_2025_all"
)

FILES_PER_ENTITY = int(os.environ.get("OPENALEX_DEMO_FILES_PER_ENTITY", "0"))


TABLES = {
    "authors": {
        "authors": [
            "id",
            "orcid",
            "display_name",
            "display_name_alternatives",
            "works_count",
            "cited_by_count",
            "last_known_institution",
            "works_api_url",
            "updated_date",
        ],
        "authors_ids": [
            "author_id",
            "openalex",
            "orcid",
            "scopus",
            "twitter",
            "wikipedia",
            "mag",
        ],
        "authors_counts_by_year": [
            "author_id",
            "year",
            "works_count",
            "cited_by_count",
            "oa_works_count",
        ],
    },
    "concepts": {
        "concepts": [
            "id",
            "wikidata",
            "display_name",
            "level",
            "description",
            "works_count",
            "cited_by_count",
            "image_url",
            "image_thumbnail_url",
            "works_api_url",
            "updated_date",
        ],
        "concepts_ancestors": ["concept_id", "ancestor_id"],
        "concepts_counts_by_year": [
            "concept_id",
            "year",
            "works_count",
            "cited_by_count",
            "oa_works_count",
        ],
        "concepts_ids": [
            "concept_id",
            "openalex",
            "wikidata",
            "wikipedia",
            "umls_aui",
            "umls_cui",
            "mag",
        ],
        "concepts_related_concepts": ["concept_id", "related_concept_id", "score"],
    },
    "topics": {
        "topics": [
            "id",
            "display_name",
            "subfield_id",
            "subfield_display_name",
            "field_id",
            "field_display_name",
            "domain_id",
            "domain_display_name",
            "description",
            "keywords",
            "works_api_url",
            "wikipedia_id",
            "works_count",
            "cited_by_count",
            "updated_date",
            "siblings",
        ],
    },
    "institutions": {
        "institutions": [
            "id",
            "ror",
            "display_name",
            "country_code",
            "type",
            "homepage_url",
            "image_url",
            "image_thumbnail_url",
            "display_name_acronyms",
            "display_name_alternatives",
            "works_count",
            "cited_by_count",
            "works_api_url",
            "updated_date",
        ],
        "institutions_ids": [
            "institution_id",
            "openalex",
            "ror",
            "grid",
            "wikipedia",
            "wikidata",
            "mag",
        ],
        "institutions_geo": [
            "institution_id",
            "city",
            "geonames_city_id",
            "region",
            "country_code",
            "country",
            "latitude",
            "longitude",
        ],
        "institutions_associated_institutions": [
            "institution_id",
            "associated_institution_id",
            "relationship",
        ],
        "institutions_counts_by_year": [
            "institution_id",
            "year",
            "works_count",
            "cited_by_count",
            "oa_works_count",
        ],
    },
    "publishers": {
        "publishers": [
            "id",
            "display_name",
            "alternate_titles",
            "country_codes",
            "hierarchy_level",
            "parent_publisher",
            "works_count",
            "cited_by_count",
            "sources_api_url",
            "updated_date",
        ],
        "publishers_counts_by_year": [
            "publisher_id",
            "year",
            "works_count",
            "cited_by_count",
            "oa_works_count",
        ],
        "publishers_ids": ["publisher_id", "openalex", "ror", "wikidata"],
    },
    "sources": {
        "sources": [
            "id",
            "issn_l",
            "issn",
            "display_name",
            "publisher",
            "works_count",
            "cited_by_count",
            "is_oa",
            "is_in_doaj",
            "homepage_url",
            "works_api_url",
            "updated_date",
        ],
        "sources_ids": [
            "source_id",
            "openalex",
            "issn_l",
            "issn",
            "mag",
            "wikidata",
            "fatcat",
        ],
        "sources_counts_by_year": [
            "source_id",
            "year",
            "works_count",
            "cited_by_count",
            "oa_works_count",
        ],
    },
    "works": {
        "works": [
            "id",
            "doi",
            "title",
            "display_name",
            "publication_year",
            "publication_date",
            "type",
            "cited_by_count",
            "is_retracted",
            "is_paratext",
            "cited_by_api_url",
            "abstract_inverted_index",
            "language",
        ],
        "works_publication_year": ["work_id", "publication_year"],
        "works_primary_locations": [
            "work_id",
            "source_id",
            "landing_page_url",
            "pdf_url",
            "is_oa",
            "version",
            "license",
        ],
        "works_locations": [
            "work_id",
            "source_id",
            "landing_page_url",
            "pdf_url",
            "is_oa",
            "version",
            "license",
        ],
        "works_best_oa_locations": [
            "work_id",
            "source_id",
            "landing_page_url",
            "pdf_url",
            "is_oa",
            "version",
            "license",
        ],
        "works_authorships": [
            "work_id",
            "author_position",
            "author_id",
            "institution_id",
            "raw_affiliation_string",
        ],
        "works_biblio": ["work_id", "volume", "issue", "first_page", "last_page"],
        "works_topics": ["work_id", "topic_id", "score"],
        "works_concepts": ["work_id", "concept_id", "score"],
        "works_ids": ["work_id", "openalex", "doi", "mag", "pmid", "pmcid"],
        "works_mesh": [
            "work_id",
            "descriptor_ui",
            "descriptor_name",
            "qualifier_ui",
            "qualifier_name",
            "is_major_topic",
        ],
        "works_open_access": [
            "work_id",
            "is_oa",
            "oa_status",
            "oa_url",
            "any_repository_has_fulltext",
        ],
        "works_referenced_works": ["work_id", "referenced_work_id"],
        "works_related_works": ["work_id", "related_work_id"],
    },
}


def json_string(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False) if value is not None else ""


def init_writers(stack: ExitStack, entity: str) -> dict[str, csv.DictWriter]:
    writers = {}
    for table_name, columns in TABLES[entity].items():
        file_path = OUTPUT_DIR / f"openalex_{table_name}.csv.gz"
        file_handle = stack.enter_context(
            gzip.open(file_path, "wt", encoding="utf-8", newline="")
        )
        writer = csv.DictWriter(
            file_handle, fieldnames=columns, extrasaction="ignore"
        )
        writer.writeheader()
        writers[table_name] = writer
    return writers


def iter_snapshot_records(entity: str):
    pattern = SNAPSHOT_DIR / "data" / entity / "*" / "*.gz"
    files_done = 0

    for jsonl_file_name in glob.glob(str(pattern)):
        print(jsonl_file_name)
        with gzip.open(jsonl_file_name, "rt", encoding="utf-8") as jsonl_file:
            for line in jsonl_file:
                if line.strip():
                    yield json.loads(line)

        files_done += 1
        if FILES_PER_ENTITY and files_done >= FILES_PER_ENTITY:
            break


def write_counts_by_year(
    writer: csv.DictWriter,
    id_field: str,
    entity_id: str,
    counts_by_year: list[dict[str, Any]] | None,
) -> None:
    for count_by_year in counts_by_year or []:
        row = dict(count_by_year)
        row[id_field] = entity_id
        writer.writerow(row)


def flatten_authors() -> None:
    with ExitStack() as stack:
        writers = init_writers(stack, "authors")
        seen_ids = set()

        for author in iter_snapshot_records("authors"):
            author_id = author.get("id")
            if not author_id or author_id in seen_ids:
                continue
            seen_ids.add(author_id)

            author_row = dict(author)
            author_row["display_name_alternatives"] = json_string(
                author.get("display_name_alternatives")
            )
            author_row["last_known_institution"] = (
                author.get("last_known_institution") or {}
            ).get("id")
            writers["authors"].writerow(author_row)

            if author_ids := author.get("ids"):
                row = dict(author_ids)
                row["author_id"] = author_id
                writers["authors_ids"].writerow(row)

            write_counts_by_year(
                writers["authors_counts_by_year"],
                "author_id",
                author_id,
                author.get("counts_by_year"),
            )

        print(f"Flattened {len(seen_ids):,} authors.")


def flatten_concepts() -> None:
    with ExitStack() as stack:
        writers = init_writers(stack, "concepts")
        seen_ids = set()

        for concept in iter_snapshot_records("concepts"):
            concept_id = concept.get("id")
            if not concept_id or concept_id in seen_ids:
                continue
            seen_ids.add(concept_id)

            writers["concepts"].writerow(concept)

            for ancestor in concept.get("ancestors") or []:
                if ancestor_id := ancestor.get("id"):
                    writers["concepts_ancestors"].writerow(
                        {"concept_id": concept_id, "ancestor_id": ancestor_id}
                    )

            write_counts_by_year(
                writers["concepts_counts_by_year"],
                "concept_id",
                concept_id,
                concept.get("counts_by_year"),
            )

            if concept_ids := concept.get("ids"):
                row = dict(concept_ids)
                row["concept_id"] = concept_id
                row["umls_aui"] = json_string(row.get("umls_aui"))
                row["umls_cui"] = json_string(row.get("umls_cui"))
                writers["concepts_ids"].writerow(row)

            for related_concept in concept.get("related_concepts") or []:
                if related_concept_id := related_concept.get("id"):
                    writers["concepts_related_concepts"].writerow(
                        {
                            "concept_id": concept_id,
                            "related_concept_id": related_concept_id,
                            "score": related_concept.get("score"),
                        }
                    )

        print(f"Flattened {len(seen_ids):,} concepts.")


def flatten_topics() -> None:
    with ExitStack() as stack:
        writers = init_writers(stack, "topics")
        seen_ids = set()

        for topic in iter_snapshot_records("topics"):
            topic_id = topic.get("id")
            if not topic_id or topic_id in seen_ids:
                continue
            seen_ids.add(topic_id)

            row = dict(topic)
            for key in ["subfield", "field", "domain"]:
                nested = topic.get(key) or {}
                row[f"{key}_id"] = nested.get("id")
                row[f"{key}_display_name"] = nested.get("display_name")
            row["keywords"] = json_string(topic.get("keywords"))
            row["siblings"] = json_string(topic.get("siblings"))
            row["wikipedia_id"] = (topic.get("ids") or {}).get("wikipedia")
            row["updated_date"] = topic.get("updated_date") or topic.get("updated")
            writers["topics"].writerow(row)

        print(f"Flattened {len(seen_ids):,} topics.")


def flatten_institutions() -> None:
    with ExitStack() as stack:
        writers = init_writers(stack, "institutions")
        seen_ids = set()

        for institution in iter_snapshot_records("institutions"):
            institution_id = institution.get("id")
            if not institution_id or institution_id in seen_ids:
                continue
            seen_ids.add(institution_id)

            row = dict(institution)
            row["display_name_acronyms"] = json_string(
                institution.get("display_name_acronyms")
            )
            row["display_name_alternatives"] = json_string(
                institution.get("display_name_alternatives")
            )
            writers["institutions"].writerow(row)

            if institution_ids := institution.get("ids"):
                row = dict(institution_ids)
                row["institution_id"] = institution_id
                writers["institutions_ids"].writerow(row)

            if geo := institution.get("geo"):
                row = dict(geo)
                row["institution_id"] = institution_id
                writers["institutions_geo"].writerow(row)

            associated_institutions = institution.get(
                "associated_institutions",
                institution.get("associated_insitutions") or [],
            )
            for associated_institution in associated_institutions or []:
                if associated_institution_id := associated_institution.get("id"):
                    writers["institutions_associated_institutions"].writerow(
                        {
                            "institution_id": institution_id,
                            "associated_institution_id": associated_institution_id,
                            "relationship": associated_institution.get(
                                "relationship"
                            ),
                        }
                    )

            write_counts_by_year(
                writers["institutions_counts_by_year"],
                "institution_id",
                institution_id,
                institution.get("counts_by_year"),
            )

        print(f"Flattened {len(seen_ids):,} institutions.")


def flatten_publishers() -> None:
    with ExitStack() as stack:
        writers = init_writers(stack, "publishers")
        seen_ids = set()

        for publisher in iter_snapshot_records("publishers"):
            publisher_id = publisher.get("id")
            if not publisher_id or publisher_id in seen_ids:
                continue
            seen_ids.add(publisher_id)

            row = dict(publisher)
            row["alternate_titles"] = json_string(publisher.get("alternate_titles"))
            row["country_codes"] = json_string(publisher.get("country_codes"))
            row["parent_publisher"] = (publisher.get("parent_publisher") or {}).get(
                "id"
            )
            writers["publishers"].writerow(row)

            write_counts_by_year(
                writers["publishers_counts_by_year"],
                "publisher_id",
                publisher_id,
                publisher.get("counts_by_year"),
            )

            if publisher_ids := publisher.get("ids"):
                row = dict(publisher_ids)
                row["publisher_id"] = publisher_id
                writers["publishers_ids"].writerow(row)

        print(f"Flattened {len(seen_ids):,} publishers.")


def flatten_sources() -> None:
    with ExitStack() as stack:
        writers = init_writers(stack, "sources")
        seen_ids = set()

        for source in iter_snapshot_records("sources"):
            source_id = source.get("id")
            if not source_id or source_id in seen_ids:
                continue
            seen_ids.add(source_id)

            row = dict(source)
            row["issn"] = json_string(source.get("issn"))
            writers["sources"].writerow(row)

            if source_ids := source.get("ids"):
                row = dict(source_ids)
                row["source_id"] = source_id
                row["issn"] = json_string(row.get("issn"))
                writers["sources_ids"].writerow(row)

            write_counts_by_year(
                writers["sources_counts_by_year"],
                "source_id",
                source_id,
                source.get("counts_by_year"),
            )

        print(f"Flattened {len(seen_ids):,} sources.")


def source_id_from_location(location: dict[str, Any] | None) -> str | None:
    return ((location or {}).get("source") or {}).get("id")


def write_location(
    writer: csv.DictWriter,
    work_id: str,
    location: dict[str, Any] | None,
) -> None:
    source_id = source_id_from_location(location)
    if source_id:
        writer.writerow(
            {
                "work_id": work_id,
                "source_id": source_id,
                "landing_page_url": location.get("landing_page_url"),
                "pdf_url": location.get("pdf_url"),
                "is_oa": location.get("is_oa"),
                "version": location.get("version"),
                "license": location.get("license"),
            }
        )


def flatten_works() -> None:
    with ExitStack() as stack:
        writers = init_writers(stack, "works")
        works_seen = 0

        for work in iter_snapshot_records("works"):
            work_id = work.get("id")
            if not work_id:
                continue
            works_seen += 1

            row = dict(work)
            row["abstract_inverted_index"] = json_string(
                work.get("abstract_inverted_index")
            )
            writers["works"].writerow(row)

            writers["works_publication_year"].writerow(
                {"work_id": work_id, "publication_year": work.get("publication_year")}
            )

            write_location(
                writers["works_primary_locations"],
                work_id,
                work.get("primary_location"),
            )
            for location in work.get("locations") or []:
                write_location(writers["works_locations"], work_id, location)
            write_location(
                writers["works_best_oa_locations"],
                work_id,
                work.get("best_oa_location"),
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
                    writers["works_authorships"].writerow(
                        {
                            "work_id": work_id,
                            "author_position": authorship.get("author_position"),
                            "author_id": author_id,
                            "institution_id": institution_id,
                            "raw_affiliation_string": authorship.get(
                                "raw_affiliation_string"
                            ),
                        }
                    )

            if biblio := work.get("biblio"):
                row = dict(biblio)
                row["work_id"] = work_id
                writers["works_biblio"].writerow(row)

            for topic in work.get("topics") or []:
                if topic_id := topic.get("id"):
                    writers["works_topics"].writerow(
                        {
                            "work_id": work_id,
                            "topic_id": topic_id,
                            "score": topic.get("score"),
                        }
                    )

            for concept in work.get("concepts") or []:
                if concept_id := concept.get("id"):
                    writers["works_concepts"].writerow(
                        {
                            "work_id": work_id,
                            "concept_id": concept_id,
                            "score": concept.get("score"),
                        }
                    )

            if ids := work.get("ids"):
                row = dict(ids)
                row["work_id"] = work_id
                writers["works_ids"].writerow(row)

            for mesh in work.get("mesh") or []:
                row = dict(mesh)
                row["work_id"] = work_id
                writers["works_mesh"].writerow(row)

            if open_access := work.get("open_access"):
                row = dict(open_access)
                row["work_id"] = work_id
                writers["works_open_access"].writerow(row)

            for referenced_work in work.get("referenced_works") or []:
                if referenced_work:
                    writers["works_referenced_works"].writerow(
                        {"work_id": work_id, "referenced_work_id": referenced_work}
                    )

            for related_work in work.get("related_works") or []:
                if related_work:
                    writers["works_related_works"].writerow(
                        {"work_id": work_id, "related_work_id": related_work}
                    )

        print(f"Flattened {works_seen:,} works.")


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    flatten_topics()
    flatten_authors()
    flatten_concepts()
    flatten_institutions()
    flatten_publishers()
    flatten_sources()
    flatten_works()
    print("OpenAlex full flattening complete.")


if __name__ == "__main__":
    main()
