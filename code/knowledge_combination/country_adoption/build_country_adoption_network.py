"""Build country adoption networks for exact-triple transitions.

The network direction is adopter country -> source country. Source countries
come from focal-year PMIDs whose exact directed typed triples are adopted or
continued in the following five-year window. Adopter countries come from future
PMIDs that use the same exact directed typed triples.
"""

from __future__ import annotations

import csv
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any

try:
    import duckdb
except ImportError as exc:  # pragma: no cover - exercised on HPC if missing.
    raise SystemExit(
        "Missing required Python package: duckdb. Install it in the HPC "
        "environment before running this script."
    ) from exc


DEFAULT_PROJECT_ROOT = Path(
    "/xdisk/sebratt/jinyugao/research/projects/innovation_capacity"
)
DEFAULT_OPENALEX_PROCESSED_DIR = Path(
    "/home/u23/jinyugao/research/data/processed/openalex"
)

BASE_YEAR = 1980
N_YEARS = 39
FUTURE_WINDOW_YEARS = 5

EDGE_ANNOTATION_SUBDIR = Path("data/processed/knowledge_combination/edge_annotation")
EDGE_TRANSITION_SUBDIR = Path("data/processed/knowledge_combination/edge_transition")
OUTPUT_SUBDIR = Path("data/processed/knowledge_combination/country_adoption")
SUMMARY_SUBDIR = OUTPUT_SUBDIR / "summary"

EDGE_ANNOTATION_FILE_STEM = "edge_annotation"
EDGE_TRANSITION_FILE_STEM = "edge_transition"
OUTPUT_FILE_STEM = "country_adoption"
SUMMARY_FILE_STEM = "country_adoption_summary"

COUNTRY_LIST_FILE_NAME = "openalex_pmid_country_full_counting_lists.csv.gz"
COUNTRY_LONG_FILE_NAME = "openalex_pmid_country_full_counting.csv.gz"

PMID_COLUMN = "PMID"
SUBJECT_CUI_COLUMN = "subject_cui_primary"
PREDICATE_COLUMN = "PREDICATE"
OBJECT_CUI_COLUMN = "object_cui_primary"
EDGE_ANNOTATION_COLUMN = "edge_annotation"
TRANSITION_COLUMN = "future_five_year_transition"

CATEGORY_NEW_NODE = "New_Node"
CATEGORY_NEW_COMBINATION = "New_Combination"
CATEGORY_NEW_RELATION = "New_Relation"
CATEGORY_REPEATED_TRIPLE = "Repeated_Triple"
CATEGORY_POOLED_NEW_KNOWLEDGE = "Pooled_New_Knowledge"

TRANSITION_ADOPTED = "Adopted"
TRANSITION_CONTINUED = "Continued"

NETWORK_CATEGORIES = [
    CATEGORY_NEW_NODE,
    CATEGORY_NEW_COMBINATION,
    CATEGORY_NEW_RELATION,
    CATEGORY_REPEATED_TRIPLE,
    CATEGORY_POOLED_NEW_KNOWLEDGE,
]
NEW_KNOWLEDGE_CATEGORIES = [
    CATEGORY_NEW_NODE,
    CATEGORY_NEW_COMBINATION,
    CATEGORY_NEW_RELATION,
]
OUTPUT_CATEGORY_SLUGS = {
    CATEGORY_NEW_NODE: "new_node",
    CATEGORY_NEW_COMBINATION: "new_combination",
    CATEGORY_NEW_RELATION: "new_relation",
    CATEGORY_REPEATED_TRIPLE: "repeated_triple",
    CATEGORY_POOLED_NEW_KNOWLEDGE: "pooled_new_knowledge",
}

NETWORK_COLUMNS = [
    "focal_year",
    "edge_annotation",
    "future_five_year_transition",
    "adopter_country_code",
    "adopter_country",
    "source_country_code",
    "source_country",
    "is_domestic_adoption",
    "weight_unique_exact_triples",
    "weight_unique_source_pmids",
    "weight_unique_future_pmids",
    "weight_source_pmid_x_future_pmid",
]


def get_env_path(name: str, default: Path) -> Path:
    value = os.environ.get(name)
    return Path(value).expanduser() if value else default


def get_env_bool(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y"}


def sql_literal(value: Path | str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def safe_share(numerator: int, denominator: int) -> float | None:
    if denominator == 0:
        return None
    return numerator / denominator


def get_focal_year() -> int:
    task_id = os.environ.get("SLURM_ARRAY_TASK_ID")
    if task_id is None:
        focal_year = os.environ.get("IC_FOCAL_YEAR")
        if focal_year is None:
            raise RuntimeError("Set SLURM_ARRAY_TASK_ID or IC_FOCAL_YEAR.")
        return int(focal_year)

    task_index = int(task_id)
    if task_index < 0 or task_index >= N_YEARS:
        raise ValueError(
            f"SLURM_ARRAY_TASK_ID={task_index} is out of range. "
            f"Expected 0-{N_YEARS - 1}."
        )
    return BASE_YEAR + task_index


def configure_duckdb(con: duckdb.DuckDBPyConnection, temp_dir: Path) -> None:
    threads = os.environ.get("IC_DUCKDB_THREADS")
    memory_limit = os.environ.get("IC_DUCKDB_MEMORY_LIMIT")

    con.execute(f"SET temp_directory={sql_literal(temp_dir)}")
    if threads:
        con.execute(f"PRAGMA threads={int(threads)}")
    if memory_limit:
        con.execute(f"PRAGMA memory_limit={sql_literal(memory_limit)}")


def scalar(
    con: duckdb.DuckDBPyConnection,
    query: str,
    params: list[Any] | None = None,
) -> Any:
    return con.execute(query, params or []).fetchone()[0]


def int_scalar(
    con: duckdb.DuckDBPyConnection,
    query: str,
    params: list[Any] | None = None,
) -> int:
    value = scalar(con, query, params)
    return 0 if value is None else int(value)


def edge_transition_file(project_root: Path, year: int) -> Path:
    return (
        project_root
        / EDGE_TRANSITION_SUBDIR
        / f"{EDGE_TRANSITION_FILE_STEM}_{year}.parquet"
    )


def edge_annotation_file(project_root: Path, year: int) -> Path:
    return (
        project_root
        / EDGE_ANNOTATION_SUBDIR
        / f"{EDGE_ANNOTATION_FILE_STEM}_{year}.parquet"
    )


def output_file(project_root: Path, year: int, category: str) -> Path:
    slug = OUTPUT_CATEGORY_SLUGS[category]
    return (
        project_root
        / OUTPUT_SUBDIR
        / slug
        / f"{OUTPUT_FILE_STEM}_{slug}_{year}.parquet"
    )


def summary_file(project_root: Path, year: int) -> Path:
    return project_root / SUMMARY_SUBDIR / f"{SUMMARY_FILE_STEM}_{year}.csv"


def future_years_for_focal_year(focal_year: int) -> list[int]:
    return list(range(focal_year + 1, focal_year + FUTURE_WINDOW_YEARS + 1))


def parquet_relation(paths: list[Path] | Path) -> str:
    if isinstance(paths, Path):
        return f"read_parquet({sql_literal(paths)})"
    path_list = ", ".join(sql_literal(path) for path in paths)
    return f"read_parquet([{path_list}])"


def normalize_pmid_sql(column: str) -> str:
    text = f"trim(CAST({column} AS VARCHAR))"
    trailing_digits = f"regexp_extract({text}, '([0-9]+)/*$', 1)"
    return f"""
        CASE
            WHEN {text} IS NULL OR {text} = '' THEN ''
            WHEN regexp_matches({text}, '^[0-9]+\\.0$')
                THEN regexp_extract({text}, '^([0-9]+)\\.0$', 1)
            WHEN {trailing_digits} != '' THEN {trailing_digits}
            ELSE {text}
        END
    """


def check_inputs(
    project_root: Path,
    openalex_processed_dir: Path,
    focal_year: int,
) -> list[Path]:
    transition_file = edge_transition_file(project_root, focal_year)
    future_annotation_files = [
        edge_annotation_file(project_root, year)
        for year in future_years_for_focal_year(focal_year)
    ]
    country_files = [
        openalex_processed_dir / COUNTRY_LIST_FILE_NAME,
        openalex_processed_dir / COUNTRY_LONG_FILE_NAME,
    ]
    input_files = [transition_file, *future_annotation_files, *country_files]
    missing = [path for path in input_files if not path.exists()]
    if missing:
        missing_text = "\n".join(str(path) for path in missing)
        raise FileNotFoundError(f"Missing required input file(s):\n{missing_text}")
    return future_annotation_files


def check_outputs(output_files: list[Path], overwrite: bool) -> None:
    for path in output_files:
        path.parent.mkdir(parents=True, exist_ok=True)

    existing = [path for path in output_files if path.exists()]
    if existing and not overwrite:
        existing_text = "\n".join(str(path) for path in existing)
        raise FileExistsError(
            "Output file(s) already exist. Move them, delete them, or rerun "
            f"with IC_COUNTRY_ADOPTION_OVERWRITE=1:\n{existing_text}"
        )
    if overwrite:
        for path in existing:
            path.unlink()


def write_single_row_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        raise ValueError("Cannot write an empty summary.")
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def create_country_tables(
    con: duckdb.DuckDBPyConnection,
    country_list_file: Path,
    country_long_file: Path,
) -> None:
    pmid_expr = normalize_pmid_sql("pmid")
    con.execute(
        f"""
        CREATE TEMP TABLE pmid_country_lists AS
        SELECT
            {pmid_expr} AS pmid_normalized,
            trim(pmid_country_codes_full_counting)
                AS pmid_country_codes_full_counting,
            TRY_CAST(n_countries_for_pmid AS BIGINT) AS n_countries_for_pmid
        FROM read_csv_auto({sql_literal(country_list_file)}, all_varchar=true)
        WHERE
            {pmid_expr} != ''
            AND pmid_country_codes_full_counting IS NOT NULL
            AND trim(pmid_country_codes_full_counting) != ''
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE pmid_country_expanded AS
        SELECT DISTINCT
            pmid_normalized,
            trim(country_code) AS country_code
        FROM pmid_country_lists,
            UNNEST(string_split(pmid_country_codes_full_counting, ';'))
                AS countries(country_code)
        WHERE trim(country_code) != ''
        """
    )
    con.execute(
        f"""
        CREATE TEMP TABLE country_name_lookup AS
        SELECT
            trim(institution_country_code) AS country_code,
            MIN(trim(institution_country)) AS country_name
        FROM read_csv_auto({sql_literal(country_long_file)}, all_varchar=true)
        WHERE
            institution_country_code IS NOT NULL
            AND trim(institution_country_code) != ''
            AND institution_country IS NOT NULL
            AND trim(institution_country) != ''
        GROUP BY trim(institution_country_code)
        """
    )


def create_source_tables(
    con: duckdb.DuckDBPyConnection,
    focal_year: int,
    transition_file: Path,
) -> None:
    focal_pmid = normalize_pmid_sql(f"ET.{PMID_COLUMN}")
    con.execute(
        f"""
        CREATE TEMP TABLE source_transition_rows AS
        SELECT
            row_number() OVER () AS source_row_id,
            ET.{PMID_COLUMN},
            {focal_pmid} AS pmid_normalized,
            ET.{EDGE_ANNOTATION_COLUMN},
            ET.{TRANSITION_COLUMN},
            ET.{SUBJECT_CUI_COLUMN},
            ET.{PREDICATE_COLUMN},
            ET.{OBJECT_CUI_COLUMN},
            concat_ws(
                '\t',
                ET.{SUBJECT_CUI_COLUMN},
                ET.{PREDICATE_COLUMN},
                ET.{OBJECT_CUI_COLUMN}
            ) AS exact_triple_key
        FROM {parquet_relation(transition_file)} AS ET
        WHERE (
            ET.{EDGE_ANNOTATION_COLUMN} IN (
                {sql_literal(CATEGORY_NEW_NODE)},
                {sql_literal(CATEGORY_NEW_COMBINATION)},
                {sql_literal(CATEGORY_NEW_RELATION)}
            )
            AND ET.{TRANSITION_COLUMN} = {sql_literal(TRANSITION_ADOPTED)}
        )
        OR (
            ET.{EDGE_ANNOTATION_COLUMN} = {sql_literal(CATEGORY_REPEATED_TRIPLE)}
            AND ET.{TRANSITION_COLUMN} = {sql_literal(TRANSITION_CONTINUED)}
        )
        """
    )
    con.execute(
        f"""
        CREATE TEMP TABLE source_country_rows AS
        SELECT
            {focal_year} AS focal_year,
            S.source_row_id,
            S.{EDGE_ANNOTATION_COLUMN},
            S.{TRANSITION_COLUMN},
            S.exact_triple_key,
            S.pmid_normalized AS source_pmid,
            C.country_code AS source_country_code,
            COALESCE(N.country_name, C.country_code) AS source_country
        FROM source_transition_rows AS S
        INNER JOIN pmid_country_expanded AS C
            ON S.pmid_normalized = C.pmid_normalized
        LEFT JOIN country_name_lookup AS N
            ON C.country_code = N.country_code
        WHERE C.country_code != ''
        """
    )
    con.execute(
        f"""
        CREATE TEMP TABLE source_country_rows_for_network AS
        SELECT
            focal_year,
            source_row_id,
            {EDGE_ANNOTATION_COLUMN},
            {TRANSITION_COLUMN},
            exact_triple_key,
            source_pmid,
            source_country_code,
            source_country
        FROM source_country_rows

        UNION ALL

        SELECT
            focal_year,
            source_row_id,
            {sql_literal(CATEGORY_POOLED_NEW_KNOWLEDGE)}
                AS {EDGE_ANNOTATION_COLUMN},
            {sql_literal(TRANSITION_ADOPTED)} AS {TRANSITION_COLUMN},
            exact_triple_key,
            source_pmid,
            source_country_code,
            source_country
        FROM source_country_rows
        WHERE {EDGE_ANNOTATION_COLUMN} IN (
            {sql_literal(CATEGORY_NEW_NODE)},
            {sql_literal(CATEGORY_NEW_COMBINATION)},
            {sql_literal(CATEGORY_NEW_RELATION)}
        )
        """
    )
    con.execute(
        f"""
        CREATE TEMP TABLE source_triple_country_counts AS
        SELECT
            focal_year,
            {EDGE_ANNOTATION_COLUMN},
            {TRANSITION_COLUMN},
            exact_triple_key,
            source_country_code,
            source_country,
            COUNT(DISTINCT source_pmid) AS n_source_pmids
        FROM source_country_rows_for_network
        GROUP BY
            focal_year,
            {EDGE_ANNOTATION_COLUMN},
            {TRANSITION_COLUMN},
            exact_triple_key,
            source_country_code,
            source_country
        """
    )


def create_future_tables(
    con: duckdb.DuckDBPyConnection,
    future_annotation_files: list[Path],
) -> None:
    future_pmid = normalize_pmid_sql(f"FA.{PMID_COLUMN}")
    con.execute(
        f"""
        CREATE TEMP TABLE future_edges AS
        SELECT
            row_number() OVER () AS future_row_id,
            FA.{PMID_COLUMN},
            {future_pmid} AS pmid_normalized,
            FA.{SUBJECT_CUI_COLUMN},
            FA.{PREDICATE_COLUMN},
            FA.{OBJECT_CUI_COLUMN},
            concat_ws(
                '\t',
                FA.{SUBJECT_CUI_COLUMN},
                FA.{PREDICATE_COLUMN},
                FA.{OBJECT_CUI_COLUMN}
            ) AS exact_triple_key
        FROM {parquet_relation(future_annotation_files)} AS FA
        WHERE
            FA.{SUBJECT_CUI_COLUMN} IS NOT NULL
            AND FA.{PREDICATE_COLUMN} IS NOT NULL
            AND FA.{OBJECT_CUI_COLUMN} IS NOT NULL
            AND trim(FA.{SUBJECT_CUI_COLUMN}) != ''
            AND trim(FA.{PREDICATE_COLUMN}) != ''
            AND trim(FA.{OBJECT_CUI_COLUMN}) != ''
            AND FA.{SUBJECT_CUI_COLUMN} != FA.{OBJECT_CUI_COLUMN}
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE future_adopter_country AS
        SELECT DISTINCT
            F.future_row_id,
            F.PMID,
            F.pmid_normalized,
            F.exact_triple_key,
            C.country_code AS adopter_country_code,
            COALESCE(N.country_name, C.country_code) AS adopter_country
        FROM future_edges AS F
        INNER JOIN pmid_country_expanded AS C
            ON F.pmid_normalized = C.pmid_normalized
        LEFT JOIN country_name_lookup AS N
            ON C.country_code = N.country_code
        WHERE C.country_code != ''
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE future_triple_country_counts AS
        SELECT
            exact_triple_key,
            adopter_country_code,
            adopter_country,
            COUNT(DISTINCT pmid_normalized) AS n_future_pmids
        FROM future_adopter_country
        GROUP BY
            exact_triple_key,
            adopter_country_code,
            adopter_country
        """
    )


def create_network_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TEMP TABLE country_adoption_triple_country AS
        SELECT
            S.focal_year,
            S.edge_annotation,
            S.future_five_year_transition,
            F.adopter_country_code,
            F.adopter_country,
            S.source_country_code,
            S.source_country,
            S.exact_triple_key,
            S.n_source_pmids,
            F.n_future_pmids,
            S.n_source_pmids * F.n_future_pmids
                AS weight_source_pmid_x_future_pmid
        FROM source_triple_country_counts AS S
        INNER JOIN future_triple_country_counts AS F
            ON S.exact_triple_key = F.exact_triple_key
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE source_pmid_contributors AS
        SELECT
            S.focal_year,
            S.edge_annotation,
            S.future_five_year_transition,
            F.adopter_country_code,
            F.adopter_country,
            S.source_country_code,
            S.source_country,
            S.source_pmid
        FROM source_country_rows_for_network AS S
        INNER JOIN (
            SELECT DISTINCT exact_triple_key, adopter_country_code, adopter_country
            FROM future_triple_country_counts
        ) AS F
            ON S.exact_triple_key = F.exact_triple_key
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE future_pmid_contributors AS
        SELECT
            S.focal_year,
            S.edge_annotation,
            S.future_five_year_transition,
            F.adopter_country_code,
            F.adopter_country,
            S.source_country_code,
            S.source_country,
            F.pmid_normalized AS future_pmid
        FROM (
            SELECT DISTINCT
                focal_year,
                edge_annotation,
                future_five_year_transition,
                exact_triple_key,
                source_country_code,
                source_country
            FROM source_triple_country_counts
        ) AS S
        INNER JOIN future_adopter_country AS F
            ON S.exact_triple_key = F.exact_triple_key
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE country_adoption_network_core AS
        SELECT
            focal_year,
            edge_annotation,
            future_five_year_transition,
            adopter_country_code,
            adopter_country,
            source_country_code,
            source_country,
            COUNT(DISTINCT exact_triple_key) AS weight_unique_exact_triples,
            SUM(weight_source_pmid_x_future_pmid)
                AS weight_source_pmid_x_future_pmid
        FROM country_adoption_triple_country
        GROUP BY
            focal_year,
            edge_annotation,
            future_five_year_transition,
            adopter_country_code,
            adopter_country,
            source_country_code,
            source_country
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE source_pmid_network_counts AS
        SELECT
            focal_year,
            edge_annotation,
            future_five_year_transition,
            adopter_country_code,
            adopter_country,
            source_country_code,
            source_country,
            COUNT(DISTINCT source_pmid) AS weight_unique_source_pmids
        FROM source_pmid_contributors
        GROUP BY
            focal_year,
            edge_annotation,
            future_five_year_transition,
            adopter_country_code,
            adopter_country,
            source_country_code,
            source_country
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE future_pmid_network_counts AS
        SELECT
            focal_year,
            edge_annotation,
            future_five_year_transition,
            adopter_country_code,
            adopter_country,
            source_country_code,
            source_country,
            COUNT(DISTINCT future_pmid) AS weight_unique_future_pmids
        FROM future_pmid_contributors
        GROUP BY
            focal_year,
            edge_annotation,
            future_five_year_transition,
            adopter_country_code,
            adopter_country,
            source_country_code,
            source_country
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE country_adoption_network AS
        SELECT
            C.focal_year,
            C.edge_annotation,
            C.future_five_year_transition,
            C.adopter_country_code,
            C.adopter_country,
            C.source_country_code,
            C.source_country,
            C.weight_unique_exact_triples,
            S.weight_unique_source_pmids,
            F.weight_unique_future_pmids,
            C.weight_source_pmid_x_future_pmid,
            C.adopter_country_code = C.source_country_code
                AS is_domestic_adoption
        FROM country_adoption_network_core AS C
        INNER JOIN source_pmid_network_counts AS S
            USING (
                focal_year,
                edge_annotation,
                future_five_year_transition,
                adopter_country_code,
                adopter_country,
                source_country_code,
                source_country
            )
        INNER JOIN future_pmid_network_counts AS F
            USING (
                focal_year,
                edge_annotation,
                future_five_year_transition,
                adopter_country_code,
                adopter_country,
                source_country_code,
                source_country
            )
        """
    )


def write_network_output(
    con: duckdb.DuckDBPyConnection,
    output_path: Path,
    category: str,
) -> None:
    columns = ", ".join(NETWORK_COLUMNS)
    con.execute(
        f"""
        COPY (
            SELECT {columns}
            FROM country_adoption_network
            WHERE edge_annotation = {sql_literal(category)}
            ORDER BY
                adopter_country_code,
                source_country_code,
                edge_annotation
        )
        TO {sql_literal(output_path)}
        (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )


def collect_summary_rows(
    con: duckdb.DuckDBPyConnection,
    focal_year: int,
    future_years: list[int],
) -> list[dict[str, Any]]:
    n_source_transition_rows = int_scalar(
        con,
        "SELECT COUNT(*) FROM source_transition_rows",
    )
    n_source_rows_with_country = int_scalar(
        con,
        """
        SELECT COUNT(DISTINCT source_row_id)
        FROM source_country_rows
        """,
    )
    n_source_country_rows = int_scalar(con, "SELECT COUNT(*) FROM source_country_rows")
    n_source_country_rows_na_code = int_scalar(
        con,
        "SELECT COUNT(*) FROM source_country_rows WHERE source_country_code = 'NA'",
    )
    n_future_rows_scanned = int_scalar(con, "SELECT COUNT(*) FROM future_edges")
    n_future_rows_with_country = int_scalar(
        con,
        """
        SELECT COUNT(DISTINCT future_row_id)
        FROM future_adopter_country
        """,
    )
    n_future_country_rows = int_scalar(con, "SELECT COUNT(*) FROM future_adopter_country")
    n_future_country_rows_na_code = int_scalar(
        con,
        "SELECT COUNT(*) FROM future_adopter_country WHERE adopter_country_code = 'NA'",
    )
    n_future_rows_matched = int_scalar(
        con,
        f"""
        SELECT COUNT(DISTINCT F.future_row_id)
        FROM future_adopter_country AS F
        INNER JOIN (
            SELECT DISTINCT exact_triple_key
            FROM source_triple_country_counts
            WHERE edge_annotation != {sql_literal(CATEGORY_POOLED_NEW_KNOWLEDGE)}
        ) AS S
            ON F.exact_triple_key = S.exact_triple_key
        """,
    )

    rows: list[dict[str, Any]] = []
    for category in NETWORK_CATEGORIES:
        if category == CATEGORY_POOLED_NEW_KNOWLEDGE:
            source_transition_filter = (
                f"WHERE {EDGE_ANNOTATION_COLUMN} IN ("
                f"{sql_literal(CATEGORY_NEW_NODE)}, "
                f"{sql_literal(CATEGORY_NEW_COMBINATION)}, "
                f"{sql_literal(CATEGORY_NEW_RELATION)})"
            )
            transition_type = TRANSITION_ADOPTED
        else:
            source_transition_filter = (
                f"WHERE {EDGE_ANNOTATION_COLUMN} = {sql_literal(category)}"
            )
            transition_type = (
                TRANSITION_CONTINUED
                if category == CATEGORY_REPEATED_TRIPLE
                else TRANSITION_ADOPTED
            )
        category_filter = f"WHERE edge_annotation = {sql_literal(category)}"

        n_source_exact_triples = int_scalar(
            con,
            f"""
            SELECT COUNT(DISTINCT exact_triple_key)
            FROM source_transition_rows
            {source_transition_filter}
            """,
        )
        n_source_rows_with_country_category = int_scalar(
            con,
            f"""
            SELECT COUNT(DISTINCT source_row_id)
            FROM source_country_rows_for_network
            {category_filter}
            """,
        )
        n_source_triple_country_rows = int_scalar(
            con,
            f"""
            SELECT COUNT(*)
            FROM source_triple_country_counts
            {category_filter}
            """,
        )
        n_country_edges = int_scalar(
            con,
            f"SELECT COUNT(*) FROM country_adoption_network {category_filter}",
        )
        n_domestic_country_edges = int_scalar(
            con,
            f"""
            SELECT COUNT(*)
            FROM country_adoption_network
            {category_filter}
                AND is_domestic_adoption
            """,
        )
        totals = con.execute(
            f"""
            SELECT
                COALESCE(SUM(weight_unique_exact_triples), 0),
                COALESCE(SUM(weight_unique_source_pmids), 0),
                COALESCE(SUM(weight_unique_future_pmids), 0),
                COALESCE(SUM(weight_source_pmid_x_future_pmid), 0)
            FROM country_adoption_network
            {category_filter}
            """
        ).fetchone()
        domestic_totals = con.execute(
            f"""
            SELECT
                COALESCE(SUM(weight_unique_exact_triples), 0),
                COALESCE(SUM(weight_source_pmid_x_future_pmid), 0)
            FROM country_adoption_network
            {category_filter}
                AND is_domestic_adoption
            """
        ).fetchone()
        rows.append(
            {
                "focal_year": focal_year,
                "future_years_found": "|".join(str(year) for year in future_years),
                "n_future_years_found": len(future_years),
                "network_category": category,
                "transition_type": transition_type,
                "n_source_transition_rows": n_source_transition_rows,
                "n_source_rows_with_country": n_source_rows_with_country,
                "source_row_country_match_rate": safe_share(
                    n_source_rows_with_country,
                    n_source_transition_rows,
                ),
                "n_source_exact_triples": n_source_exact_triples,
                "n_source_rows_with_country_in_category": (
                    n_source_rows_with_country_category
                ),
                "n_source_triple_country_rows": n_source_triple_country_rows,
                "n_source_country_rows": n_source_country_rows,
                "n_source_country_rows_na_code": n_source_country_rows_na_code,
                "n_future_rows_scanned": n_future_rows_scanned,
                "n_future_rows_with_country": n_future_rows_with_country,
                "future_row_country_match_rate": safe_share(
                    n_future_rows_with_country,
                    n_future_rows_scanned,
                ),
                "n_future_country_rows": n_future_country_rows,
                "n_future_country_rows_na_code": n_future_country_rows_na_code,
                "n_future_rows_matched_to_source_exact_triples": n_future_rows_matched,
                "n_country_edges": n_country_edges,
                "n_domestic_country_edges": n_domestic_country_edges,
                "share_domestic_country_edges": safe_share(
                    n_domestic_country_edges,
                    n_country_edges,
                ),
                "total_weight_unique_exact_triples": int(totals[0]),
                "total_weight_unique_source_pmids": int(totals[1]),
                "total_weight_unique_future_pmids": int(totals[2]),
                "total_weight_source_pmid_x_future_pmid": int(totals[3]),
                "domestic_weight_unique_exact_triples": int(domestic_totals[0]),
                "domestic_weight_source_pmid_x_future_pmid": int(domestic_totals[1]),
                "n_output_rows": n_country_edges,
            }
        )

    return rows


def build_country_adoption_network(
    project_root: Path,
    openalex_processed_dir: Path,
    focal_year: int,
    overwrite: bool,
) -> None:
    future_years = future_years_for_focal_year(focal_year)
    transition_file = edge_transition_file(project_root, focal_year)
    future_annotation_files = check_inputs(
        project_root,
        openalex_processed_dir,
        focal_year,
    )
    country_list_file = openalex_processed_dir / COUNTRY_LIST_FILE_NAME
    country_long_file = openalex_processed_dir / COUNTRY_LONG_FILE_NAME
    network_output_files = [
        output_file(project_root, focal_year, category)
        for category in NETWORK_CATEGORIES
    ]
    year_summary_file = summary_file(project_root, focal_year)
    check_outputs([*network_output_files, year_summary_file], overwrite)

    temp_parent = project_root / OUTPUT_SUBDIR
    temp_parent.mkdir(parents=True, exist_ok=True)
    temp_dir = Path(tempfile.mkdtemp(prefix="duckdb_country_adoption_", dir=temp_parent))
    print(f"Using DuckDB temp directory: {temp_dir}")

    con = duckdb.connect(database=":memory:")
    try:
        configure_duckdb(con, temp_dir)
        print(f"Focal year: {focal_year}")
        print(f"Future years: {future_years}")
        print(f"Edge transition input: {transition_file}")
        print(
            "Future edge-annotation inputs:\n"
            + "\n".join(str(path) for path in future_annotation_files)
        )
        print(f"OpenAlex country list input: {country_list_file}")
        print(f"OpenAlex country long input: {country_long_file}")

        create_country_tables(con, country_list_file, country_long_file)
        print(
            "PMID-country list rows: "
            f"{int_scalar(con, 'SELECT COUNT(*) FROM pmid_country_lists'):,}"
        )
        print(
            "PMID-country expanded rows: "
            f"{int_scalar(con, 'SELECT COUNT(*) FROM pmid_country_expanded'):,}"
        )

        create_source_tables(con, focal_year, transition_file)
        print(
            "Source transition rows: "
            f"{int_scalar(con, 'SELECT COUNT(*) FROM source_transition_rows'):,}"
        )
        print(
            "Source country rows: "
            f"{int_scalar(con, 'SELECT COUNT(*) FROM source_country_rows'):,}"
        )
        print(
            "Source triple-country count rows, including pooled category: "
            f"{int_scalar(con, 'SELECT COUNT(*) FROM source_triple_country_counts'):,}"
        )

        create_future_tables(con, future_annotation_files)
        print(
            "Future rows scanned: "
            f"{int_scalar(con, 'SELECT COUNT(*) FROM future_edges'):,}"
        )
        print(
            "Future adopter-country rows: "
            f"{int_scalar(con, 'SELECT COUNT(*) FROM future_adopter_country'):,}"
        )

        create_network_tables(con)
        print(
            "Country adoption triple-country rows before network aggregation: "
            f"{int_scalar(con, 'SELECT COUNT(*) FROM country_adoption_triple_country'):,}"
        )
        print(
            "Country adoption network rows: "
            f"{int_scalar(con, 'SELECT COUNT(*) FROM country_adoption_network'):,}"
        )

        for category, path in zip(NETWORK_CATEGORIES, network_output_files):
            write_network_output(con, path, category)
            print(f"Saved {category} country network to {path}")

        summary_rows = collect_summary_rows(con, focal_year, future_years)
        write_single_row_csv(year_summary_file, summary_rows)
        print(f"Saved country adoption summary to {year_summary_file}")
    finally:
        con.close()
        shutil.rmtree(temp_dir, ignore_errors=True)


def main() -> None:
    project_root = get_env_path("IC_PROJECT_ROOT", DEFAULT_PROJECT_ROOT)
    openalex_processed_dir = get_env_path(
        "IC_OPENALEX_PROCESSED_DIR",
        DEFAULT_OPENALEX_PROCESSED_DIR,
    )
    focal_year = get_focal_year()
    overwrite = get_env_bool("IC_COUNTRY_ADOPTION_OVERWRITE", False)

    print(f"Project root: {project_root}")
    print(f"OpenAlex processed directory: {openalex_processed_dir}")
    print(f"Focal year: {focal_year}")
    print(f"Overwrite outputs: {overwrite}")
    build_country_adoption_network(
        project_root,
        openalex_processed_dir,
        focal_year,
        overwrite,
    )
    print("Country adoption network construction complete.")


if __name__ == "__main__":
    main()
