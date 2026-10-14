"""Build a Parquet table with one full-counting row per PMID-country pair."""

from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path

import duckdb


DEFAULT_INSTITUTION_DIR = Path(
    "/xdisk/sebratt/jinyugao/research/data/processed/openalex/institution"
)
INSTITUTION_DIR = Path(
    os.environ.get("IC_OPENALEX_INSTITUTION_DIR", DEFAULT_INSTITUTION_DIR)
).expanduser()

INPUT_FILE = INSTITUTION_DIR / "openalex_authorships_institutions_workids_pmid.parquet"
OUTPUT_FILE = INSTITUTION_DIR / "openalex_pmid_country_full_counting.parquet"

OVERWRITE = os.environ.get("IC_OPENALEX_INSTITUTION_OVERWRITE", "0").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
DUCKDB_THREADS = int(os.environ.get("IC_DUCKDB_THREADS", "4"))
DUCKDB_MEMORY_LIMIT = os.environ.get("IC_DUCKDB_MEMORY_LIMIT", "4GB")


def sql_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def check_input(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing required input file: {path}")


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


def build_pmid_country_table(
    con: duckdb.DuckDBPyConnection,
    input_file: Path,
    output_file: Path,
) -> None:
    con.execute(
        f"""
        CREATE TEMP VIEW authorships_clean AS
        SELECT
            trim(CAST(pmid AS VARCHAR)) AS pmid,
            nullif(trim(CAST(work_id AS VARCHAR)), '') AS work_id,
            nullif(trim(CAST(author_id AS VARCHAR)), '') AS author_id,
            nullif(trim(CAST(institution_id AS VARCHAR)), '') AS institution_id,
            CASE
                WHEN nullif(
                    trim(CAST(institution_country_code AS VARCHAR)),
                    ''
                ) IS NULL
                AND lower(trim(CAST(institution_country AS VARCHAR))) = 'namibia'
                    THEN 'NA'
                ELSE upper(
                    nullif(
                        trim(CAST(institution_country_code AS VARCHAR)),
                        ''
                    )
                )
            END AS institution_country_code,
            nullif(trim(CAST(institution_country AS VARCHAR)), '')
                AS institution_country
        FROM read_parquet({sql_literal(input_file)})
        """
    )

    input_rows, kept_rows = (
        int(value)
        for value in con.execute(
            """
            SELECT
                COUNT(*) AS input_rows,
                COUNT(*) FILTER (
                    WHERE
                        pmid IS NOT NULL
                        AND pmid != ''
                        AND institution_country_code IS NOT NULL
                        AND institution_country_code != ''
                ) AS kept_rows
            FROM authorships_clean
            """,
        ).fetchone()
    )

    con.execute(
        f"""
        COPY (
            SELECT
                pmid,
                institution_country_code,
                string_agg(
                    DISTINCT institution_country,
                    '; ' ORDER BY institution_country
                ) FILTER (
                    WHERE institution_country IS NOT NULL
                ) AS institution_country,
                CAST(1 AS BIGINT) AS pmid_country_weight,
                COUNT(DISTINCT work_id) AS n_work_ids_for_pmid_country,
                COUNT(DISTINCT author_id) AS n_authors_for_pmid_country,
                COUNT(DISTINCT institution_id) AS n_institutions_for_pmid_country,
                COUNT(*) AS n_authorship_institution_rows_for_pmid_country
            FROM authorships_clean
            WHERE
                pmid IS NOT NULL
                AND pmid != ''
                AND institution_country_code IS NOT NULL
                AND institution_country_code != ''
            GROUP BY pmid, institution_country_code
        ) TO {sql_literal(output_file)} (
            FORMAT PARQUET,
            COMPRESSION ZSTD,
            ROW_GROUP_SIZE 100000
        )
        """
    )

    output_rows, lowercase_country_codes, namibia_rows = (
        int(value)
        for value in con.execute(
            f"""
            SELECT
                COUNT(*) AS output_rows,
                COUNT(*) FILTER (
                    WHERE institution_country_code
                        != upper(institution_country_code)
                ) AS lowercase_country_codes,
                COUNT(*) FILTER (
                    WHERE institution_country_code = 'NA'
                ) AS namibia_rows
            FROM read_parquet({sql_literal(output_file)})
            """
        ).fetchone()
    )
    duplicate_output_keys = int_scalar(
        con,
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT pmid, institution_country_code
            FROM read_parquet({sql_literal(output_file)})
            GROUP BY pmid, institution_country_code
            HAVING COUNT(*) > 1
        )
        """,
    )
    if duplicate_output_keys:
        raise RuntimeError(
            "PMID-country output contains duplicate keys: "
            f"{duplicate_output_keys:,}."
        )
    if lowercase_country_codes:
        raise RuntimeError(
            "PMID-country output contains non-uppercase country codes: "
            f"{lowercase_country_codes:,}."
        )

    print(f"Input authorship rows: {input_rows:,}")
    print(f"Rows with PMID and country code: {kept_rows:,}")
    print(f"Unique PMID-country rows written: {output_rows:,}")
    print(f"PMID-country rows with Namibia country code NA: {namibia_rows:,}")


def main() -> None:
    print(f"Institution directory: {INSTITUTION_DIR}")
    print(f"Input parquet: {INPUT_FILE}")
    print(f"Output parquet: {OUTPUT_FILE}")
    print(f"Overwrite output: {OVERWRITE}")
    print(f"DuckDB threads: {DUCKDB_THREADS}")
    print(f"DuckDB memory limit: {DUCKDB_MEMORY_LIMIT}")
    check_input(INPUT_FILE)
    check_output(OUTPUT_FILE, OVERWRITE)

    temp_dir = Path(
        tempfile.mkdtemp(
            prefix="duckdb_openalex_full_counting_",
            dir=INSTITUTION_DIR,
        )
    )
    temporary_output = make_temporary_output(OUTPUT_FILE)
    print(f"Using DuckDB temp directory: {temp_dir}")
    print(f"Writing temporary output: {temporary_output}")

    con = duckdb.connect(database=":memory:")
    try:
        configure_duckdb(con, temp_dir)
        build_pmid_country_table(con, INPUT_FILE, temporary_output)
        os.replace(temporary_output, OUTPUT_FILE)
    finally:
        con.close()
        temporary_output.unlink(missing_ok=True)
        shutil.rmtree(temp_dir, ignore_errors=True)

    print(f"Saved PMID-country full-counting table to {OUTPUT_FILE}")
    print("OpenAlex PMID-country full counting complete.")


if __name__ == "__main__":
    main()
