"""Build a PMID-linked OpenAlex authorship-institution-work table."""

from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path

import duckdb


DEFAULT_FLATTENED_DIR = Path(
    "/xdisk/sebratt/jinyugao/research/data/processed/openalex/flattened_snapshot_2025"
)
DEFAULT_INSTITUTION_DIR = Path(
    "/xdisk/sebratt/jinyugao/research/data/processed/openalex/institution"
)
FLATTENED_DIR = Path(
    os.environ.get("IC_OPENALEX_FLATTENED_DIR", DEFAULT_FLATTENED_DIR)
).expanduser()
INSTITUTION_DIR = Path(
    os.environ.get("IC_OPENALEX_INSTITUTION_DIR", DEFAULT_INSTITUTION_DIR)
).expanduser()

WORKS_IDS_FILE = FLATTENED_DIR / "openalex_works_ids.csv.gz"
AUTHORSHIPS_FILE = FLATTENED_DIR / "openalex_works_authorships.csv.gz"
INSTITUTIONS_FILE = FLATTENED_DIR / "openalex_institutions.csv.gz"

OUTPUT_FILE = INSTITUTION_DIR / "openalex_authorships_institutions_workids_pmid.parquet"

OVERWRITE = (
    os.environ.get("IC_OPENALEX_INSTITUTION_OVERWRITE", "0").strip().lower()
    in {
        "1",
        "true",
        "yes",
        "on",
    }
)
DUCKDB_THREADS = int(os.environ.get("IC_DUCKDB_THREADS", "4"))
DUCKDB_MEMORY_LIMIT = os.environ.get("IC_DUCKDB_MEMORY_LIMIT", "4GB")


def sql_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def check_inputs(paths: list[Path]) -> None:
    missing_files = [str(path) for path in paths if not path.exists()]
    if missing_files:
        missing = "\n".join(missing_files)
        raise FileNotFoundError(f"Missing required input file(s):\n{missing}")


def check_output(path: Path, overwrite: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not overwrite:
        raise FileExistsError(
            "Output file already exists. Set IC_OPENALEX_INSTITUTION_OVERWRITE=1 "
            f"to replace it:\n{path}"
        )


def make_temporary_output(path: Path) -> Path:
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    os.close(descriptor)
    temporary_path = Path(temporary_name)
    temporary_path.unlink()
    return temporary_path


def int_scalar(con: duckdb.DuckDBPyConnection, query: str) -> int:
    return int(con.execute(query).fetchone()[0])


def configure_duckdb(
    con: duckdb.DuckDBPyConnection,
    temp_dir: Path,
) -> None:
    con.execute(f"SET threads = {DUCKDB_THREADS}")
    con.execute(f"SET memory_limit = {sql_literal(DUCKDB_MEMORY_LIMIT)}")
    con.execute(f"SET temp_directory = {sql_literal(temp_dir)}")
    con.execute("SET preserve_insertion_order = false")


def create_lookup_tables(
    con: duckdb.DuckDBPyConnection,
    works_ids_file: Path,
    institutions_file: Path,
) -> None:
    """Load join keys while treating only empty CSV fields as null values."""
    con.execute(
        f"""
        CREATE TEMP TABLE pmid_works AS
        WITH cleaned AS (
            SELECT
                nullif(trim(CAST(work_id AS VARCHAR)), '') AS work_id,
                nullif(
                    regexp_extract(
                        trim(CAST(pmid AS VARCHAR)),
                        '([0-9]+)$',
                        1
                    ),
                    ''
                ) AS pmid,
                nullif(trim(CAST(pmid AS VARCHAR)), '') AS pmid_raw,
                nullif(trim(CAST(doi AS VARCHAR)), '') AS doi,
                nullif(trim(CAST(pmcid AS VARCHAR)), '') AS pmcid
            FROM read_csv(
                {sql_literal(works_ids_file)},
                header = true,
                all_varchar = true,
                nullstr = '',
                delim = ',',
                quote = '"',
                escape = '"'
            )
        ),
        ranked AS (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY work_id
                    ORDER BY pmid, pmid_raw, doi, pmcid
                ) AS work_row_number
            FROM cleaned
            WHERE work_id IS NOT NULL AND pmid IS NOT NULL
        )
        SELECT
            work_id,
            pmid,
            pmid_raw,
            doi,
            pmcid
        FROM ranked
        WHERE work_row_number = 1
        """
    )

    con.execute(
        f"""
        CREATE TEMP TABLE institution_lookup AS
        WITH cleaned AS (
            SELECT
                nullif(trim(CAST(institution_id AS VARCHAR)), '')
                    AS institution_id,
                nullif(trim(CAST(ror AS VARCHAR)), '') AS institution_ror,
                nullif(trim(CAST(display_name AS VARCHAR)), '')
                    AS institution_display_name,
                CASE
                    WHEN coalesce(
                        nullif(trim(CAST(country_code AS VARCHAR)), ''),
                        nullif(trim(CAST(geo_country_code AS VARCHAR)), '')
                    ) IS NULL
                    AND lower(trim(CAST(country AS VARCHAR))) = 'namibia'
                        THEN 'NA'
                    ELSE upper(
                        coalesce(
                            nullif(trim(CAST(country_code AS VARCHAR)), ''),
                            nullif(trim(CAST(geo_country_code AS VARCHAR)), '')
                        )
                    )
                END AS institution_country_code,
                nullif(trim(CAST(country AS VARCHAR)), '')
                    AS institution_country,
                nullif(trim(CAST("type" AS VARCHAR)), '') AS institution_type
            FROM read_csv(
                {sql_literal(institutions_file)},
                header = true,
                all_varchar = true,
                nullstr = '',
                delim = ',',
                quote = '"',
                escape = '"'
            )
        ),
        ranked AS (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY institution_id
                    ORDER BY
                        institution_country_code NULLS LAST,
                        institution_display_name NULLS LAST,
                        institution_ror NULLS LAST
                ) AS institution_row_number
            FROM cleaned
            WHERE institution_id IS NOT NULL
        )
        SELECT
            institution_id,
            institution_ror,
            institution_display_name,
            institution_country_code,
            institution_country,
            institution_type
        FROM ranked
        WHERE institution_row_number = 1
        """
    )


def print_lookup_diagnostics(con: duckdb.DuckDBPyConnection) -> None:
    pmid_work_rows = int_scalar(con, "SELECT COUNT(*) FROM pmid_works")
    unique_pmids = int_scalar(con, "SELECT COUNT(DISTINCT pmid) FROM pmid_works")
    pmids_with_multiple_work_ids = int_scalar(
        con,
        """
        SELECT COUNT(*)
        FROM (
            SELECT pmid
            FROM pmid_works
            GROUP BY pmid
            HAVING COUNT(*) > 1
        )
        """,
    )
    institution_rows = int_scalar(con, "SELECT COUNT(*) FROM institution_lookup")
    namibia_institutions = int_scalar(
        con,
        """
        SELECT COUNT(*)
        FROM institution_lookup
        WHERE lower(institution_country) = 'namibia'
        """,
    )
    namibia_code_rows = int_scalar(
        con,
        """
        SELECT COUNT(*)
        FROM institution_lookup
        WHERE
            lower(institution_country) = 'namibia'
            AND institution_country_code = 'NA'
        """,
    )

    if namibia_institutions > 0 and namibia_code_rows != namibia_institutions:
        raise RuntimeError(
            "Namibia country-code validation failed: "
            f"{namibia_code_rows:,} of {namibia_institutions:,} Namibia "
            "institutions have country code NA."
        )

    print(f"Unique PMID-linked OpenAlex works: {pmid_work_rows:,}")
    print(f"Unique PMIDs represented: {unique_pmids:,}")
    print(
        "PMIDs linked to multiple OpenAlex work IDs: "
        f"{pmids_with_multiple_work_ids:,}"
    )
    print(f"Unique institutions loaded: {institution_rows:,}")
    print(f"Namibia institutions: {namibia_institutions:,}")
    print(f"Namibia institutions coded NA: {namibia_code_rows:,}")


def build_authorship_table(
    con: duckdb.DuckDBPyConnection,
    authorships_file: Path,
    output_file: Path,
) -> None:
    copy_result = con.execute(
        f"""
        COPY (
            SELECT
                works.work_id,
                works.pmid,
                works.pmid_raw,
                works.doi,
                works.pmcid,
                nullif(trim(CAST(authorships.author_position AS VARCHAR)), '')
                    AS author_position,
                nullif(trim(CAST(authorships.author_id AS VARCHAR)), '')
                    AS author_id,
                nullif(trim(CAST(authorships.institution_id AS VARCHAR)), '')
                    AS institution_id,
                institutions.institution_ror,
                institutions.institution_display_name,
                institutions.institution_country_code,
                institutions.institution_country,
                institutions.institution_type,
                nullif(
                    trim(CAST(authorships.raw_affiliation_string AS VARCHAR)),
                    ''
                ) AS raw_affiliation_string
            FROM read_csv(
                {sql_literal(authorships_file)},
                header = true,
                all_varchar = true,
                nullstr = '',
                delim = ',',
                quote = '"',
                escape = '"'
            ) AS authorships
            INNER JOIN pmid_works AS works
                ON nullif(trim(CAST(authorships.work_id AS VARCHAR)), '')
                    = works.work_id
            LEFT JOIN institution_lookup AS institutions
                ON nullif(trim(CAST(authorships.institution_id AS VARCHAR)), '')
                    = institutions.institution_id
        ) TO {sql_literal(output_file)} (
            FORMAT PARQUET,
            COMPRESSION ZSTD,
            ROW_GROUP_SIZE 100000
        )
        """
    ).fetchone()
    rows_written = int(copy_result[0]) if copy_result else 0

    diagnostics = con.execute(
        f"""
        SELECT
            COUNT(*) AS output_rows,
            COUNT(DISTINCT pmid) AS unique_pmids,
            COUNT(*) FILTER (
                WHERE institution_country_code IS NOT NULL
            ) AS rows_with_country,
            COUNT(DISTINCT pmid) FILTER (
                WHERE institution_country_code IS NOT NULL
            ) AS pmids_with_country,
            COUNT(*) FILTER (
                WHERE institution_country_code = 'NA'
            ) AS namibia_rows
        FROM read_parquet({sql_literal(output_file)})
        """
    ).fetchone()

    if diagnostics is None:
        raise RuntimeError("Could not validate the temporary authorship output.")

    (
        output_rows,
        unique_pmids,
        rows_with_country,
        pmids_with_country,
        namibia_rows,
    ) = (int(value) for value in diagnostics)
    if rows_written != output_rows:
        raise RuntimeError(
            "DuckDB COPY row count does not match output validation: "
            f"{rows_written:,} versus {output_rows:,}."
        )

    country_coverage = pmids_with_country / unique_pmids if unique_pmids else 0.0
    print(f"PMID-linked authorship-institution rows written: {output_rows:,}")
    print(f"Rows with a country code: {rows_with_country:,}")
    print(f"Unique PMIDs in output: {unique_pmids:,}")
    print(f"Unique PMIDs with at least one country code: {pmids_with_country:,}")
    print(
        "PMID country coverage among PMIDs with authorship rows: "
        f"{country_coverage:.6f}"
    )
    print(f"Output rows with Namibia country code NA: {namibia_rows:,}")


def main() -> None:
    print(f"Flattened OpenAlex directory: {FLATTENED_DIR}")
    print(f"Institution output directory: {INSTITUTION_DIR}")
    print(f"Output parquet: {OUTPUT_FILE}")
    print(f"Overwrite output: {OVERWRITE}")
    print(f"DuckDB threads: {DUCKDB_THREADS}")
    print(f"DuckDB memory limit: {DUCKDB_MEMORY_LIMIT}")
    check_inputs(
        [
            WORKS_IDS_FILE,
            AUTHORSHIPS_FILE,
            INSTITUTIONS_FILE,
        ]
    )
    check_output(OUTPUT_FILE, OVERWRITE)

    temp_dir = Path(
        tempfile.mkdtemp(
            prefix="duckdb_openalex_institution_",
            dir=INSTITUTION_DIR,
        )
    )
    temporary_output = make_temporary_output(OUTPUT_FILE)
    print(f"Using DuckDB temp directory: {temp_dir}")
    print(f"Writing temporary output: {temporary_output}")

    con = duckdb.connect(database=":memory:")
    try:
        configure_duckdb(con, temp_dir)
        create_lookup_tables(con, WORKS_IDS_FILE, INSTITUTIONS_FILE)
        print_lookup_diagnostics(con)
        build_authorship_table(con, AUTHORSHIPS_FILE, temporary_output)
        os.replace(temporary_output, OUTPUT_FILE)
    finally:
        con.close()
        temporary_output.unlink(missing_ok=True)
        shutil.rmtree(temp_dir, ignore_errors=True)

    print(f"Saved authorship-institution-work table to {OUTPUT_FILE}")
    print("OpenAlex PMID-linked institution construction complete.")


if __name__ == "__main__":
    main()
