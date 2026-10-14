"""Compare 2010 OpenAlex citation counts with SciSciNet paper metrics.

The comparison is performed out of core with DuckDB. OpenAlex work IDs are
normalized to SciSciNet paper IDs by extracting the W-prefixed identifier.

Both inputs are restricted to the same comparison year before joining. The 2010
cohort has complete C3, C5, and C10 windows through the project's 2024 cutoff.

SciSciNet citation_count and cited_by_count are retained in the matched
row-level output for exploratory use, but they are excluded from formal
agreement metrics because their citation cutoff does not match 2024.
"""

from __future__ import annotations

import math
from pathlib import Path

import duckdb
import pandas as pd


OPENALEX_DIR = Path("/xdisk/sebratt/jinyugao/data/products/openalex/flattened_snapshot_2025")
PROJECT_CITATION_FILE = (
    OPENALEX_DIR / "openalex_work_citations_reference_counts_through_2024.csv.gz"
)
SCISCINET_FILE = Path(
    "/xdisk/sebratt/jinyugao/data/source/sciscinet_v2/sciscinet_papers.parquet"
)

COMPARISON_YEAR = 2010
OUTPUT_DIR = (
    OPENALEX_DIR / "citation_validation_sciscinet" / f"year_{COMPARISON_YEAR}"
)
MATCHED_OUTPUT_FILE = OUTPUT_DIR / "openalex_sciscinet_citation_comparison.parquet"
OVERALL_SUMMARY_FILE = OUTPUT_DIR / "openalex_sciscinet_citation_comparison_summary.csv"
YEAR_SUMMARY_FILE = (
    OUTPUT_DIR / "openalex_sciscinet_citation_comparison_summary_by_year.csv"
)
COVERAGE_SUMMARY_FILE = OUTPUT_DIR / "openalex_sciscinet_match_coverage_summary.csv"
LARGEST_DIFFERENCES_FILE = (
    OUTPUT_DIR / "openalex_sciscinet_largest_citation_differences.csv"
)
DUCKDB_TEMP_DIR = OUTPUT_DIR / "duckdb_temp"

OVERWRITE = False
DUCKDB_MEMORY_LIMIT = "200GB"
DUCKDB_THREADS = 8
TOP_DIFFERENCES_PER_METRIC = 100

METRICS = [
    {
        "metric": "reference_count",
        "project_column": "n_references",
        "sciscinet_column": "reference_count",
        "complete_year_max": None,
    },
    {
        "metric": "C3",
        "project_column": "citation_C3",
        "sciscinet_column": "sciscinet_C3",
        "complete_year_max": None,
    },
    {
        "metric": "C5",
        "project_column": "citation_C5",
        "sciscinet_column": "sciscinet_C5",
        "complete_year_max": None,
    },
    {
        "metric": "C10",
        "project_column": "citation_C10",
        "sciscinet_column": "sciscinet_C10",
        "complete_year_max": None,
    },
]


def sql_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def check_input(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing required input file: {path}")


def check_outputs(paths: list[Path]) -> None:
    existing = [path for path in paths if path.exists()]
    if existing and not OVERWRITE:
        examples = "\n".join(str(path) for path in existing)
        raise FileExistsError(
            "Output file(s) already exist. Set OVERWRITE = True to replace them:\n"
            f"{examples}"
        )
    if OVERWRITE:
        for path in existing:
            path.unlink()


def configure_connection() -> duckdb.DuckDBPyConnection:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    DUCKDB_TEMP_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute(f"SET threads = {DUCKDB_THREADS}")
    con.execute(f"SET memory_limit = {sql_literal(DUCKDB_MEMORY_LIMIT)}")
    con.execute(f"SET temp_directory = {sql_literal(DUCKDB_TEMP_DIR)}")
    con.execute("SET preserve_insertion_order = false")
    return con


def create_input_views(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        f"""
        CREATE OR REPLACE VIEW project_citations_raw AS
        SELECT
            regexp_extract(work_id, '(W[0-9]+)', 1) AS paperid,
            work_id,
            try_cast(publication_year AS BIGINT) AS project_year,
            try_cast(n_references AS BIGINT) AS n_references,
            try_cast(citation_C3 AS BIGINT) AS citation_C3,
            try_cast(citation_C5 AS BIGINT) AS citation_C5,
            try_cast(citation_C10 AS BIGINT) AS citation_C10,
            try_cast(citation_through_2024 AS BIGINT) AS citation_through_2024,
            try_cast(has_complete_C3_window AS BOOLEAN) AS has_complete_C3_window,
            try_cast(has_complete_C5_window AS BOOLEAN) AS has_complete_C5_window,
            try_cast(has_complete_C10_window AS BOOLEAN) AS has_complete_C10_window
        FROM read_csv_auto(
            {sql_literal(PROJECT_CITATION_FILE)},
            header = true,
            all_varchar = true,
            compression = 'gzip'
        )
        WHERE try_cast(publication_year AS BIGINT) = {COMPARISON_YEAR}
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE VIEW sciscinet_papers_raw AS
        SELECT
            trim(cast(paperid AS VARCHAR)) AS paperid,
            try_cast(year AS BIGINT) AS sciscinet_year,
            cast(doctype AS VARCHAR) AS doctype,
            try_cast(reference_count AS BIGINT) AS reference_count,
            try_cast(citation_count AS BIGINT) AS citation_count,
            try_cast(cited_by_count AS BIGINT) AS cited_by_count,
            try_cast(C3 AS BIGINT) AS C3,
            try_cast(C5 AS BIGINT) AS C5,
            try_cast(C10 AS BIGINT) AS C10
        FROM read_parquet({sql_literal(SCISCINET_FILE)})
        WHERE try_cast(year AS BIGINT) = {COMPARISON_YEAR}
        """
    )


def input_diagnostics(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    diagnostics = {}

    def source_diagnostics(view_name: str) -> tuple[int, int, int, int]:
        row = con.execute(
            f"""
            WITH counts_by_id AS (
                SELECT paperid, count(*) AS n_rows
                FROM {view_name}
                GROUP BY paperid
            )
            SELECT
                coalesce(sum(n_rows), 0) AS n_rows,
                coalesce(sum(CASE WHEN paperid <> '' THEN n_rows ELSE 0 END), 0)
                    AS n_valid_rows,
                coalesce(sum(CASE WHEN paperid <> '' THEN 1 ELSE 0 END), 0)
                    AS n_unique_valid_ids,
                coalesce(sum(CASE WHEN paperid <> '' AND n_rows > 1
                    THEN 1 ELSE 0 END), 0) AS n_duplicate_ids
            FROM counts_by_id
            """
        ).fetchone()
        return tuple(int(value) for value in row)

    (
        diagnostics["n_project_rows"],
        diagnostics["n_project_valid_paperids"],
        diagnostics["n_project_unique_paperids"],
        diagnostics["n_project_duplicate_paperids"],
    ) = source_diagnostics("project_citations_raw")
    (
        diagnostics["n_sciscinet_rows"],
        diagnostics["n_sciscinet_valid_paperids"],
        diagnostics["n_sciscinet_unique_paperids"],
        diagnostics["n_sciscinet_duplicate_paperids"],
    ) = source_diagnostics("sciscinet_papers_raw")

    if diagnostics["n_project_duplicate_paperids"] > 0:
        raise ValueError(
            "Project citation file contains duplicate normalized paper IDs: "
            f"{diagnostics['n_project_duplicate_paperids']:,}"
        )
    if diagnostics["n_sciscinet_duplicate_paperids"] > 0:
        raise ValueError(
            "SciSciNet file contains duplicate paper IDs: "
            f"{diagnostics['n_sciscinet_duplicate_paperids']:,}"
        )
    return diagnostics


def create_matched_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE matched AS
        SELECT
            p.paperid,
            p.work_id,
            p.project_year,
            s.sciscinet_year,
            p.project_year = s.sciscinet_year AS publication_year_exact_match,
            s.doctype,
            p.n_references,
            s.reference_count,
            p.n_references - s.reference_count AS reference_count_difference,
            abs(p.n_references - s.reference_count) AS reference_count_absolute_difference,
            p.citation_C3,
            s.C3 AS sciscinet_C3,
            p.citation_C3 - s.C3 AS C3_difference,
            abs(p.citation_C3 - s.C3) AS C3_absolute_difference,
            p.citation_C5,
            s.C5 AS sciscinet_C5,
            p.citation_C5 - s.C5 AS C5_difference,
            abs(p.citation_C5 - s.C5) AS C5_absolute_difference,
            p.citation_C10,
            s.C10 AS sciscinet_C10,
            p.citation_C10 - s.C10 AS C10_difference,
            abs(p.citation_C10 - s.C10) AS C10_absolute_difference,
            p.citation_through_2024,
            s.citation_count AS sciscinet_citation_count,
            s.cited_by_count AS sciscinet_cited_by_count,
            p.has_complete_C3_window,
            p.has_complete_C5_window,
            p.has_complete_C10_window
        FROM project_citations_raw p
        INNER JOIN sciscinet_papers_raw s USING (paperid)
        WHERE p.paperid <> ''
        """
    )


def export_matched_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        f"""
        COPY (
            SELECT * FROM matched ORDER BY paperid
        ) TO {sql_literal(MATCHED_OUTPUT_FILE)}
        (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    print(f"Saved row-level matched comparison to {MATCHED_OUTPUT_FILE}")


def metric_filter(metric: dict[str, object], alias: str = "") -> str:
    prefix = f"{alias}." if alias else ""
    conditions = [
        f"{prefix}{metric['project_column']} IS NOT NULL",
        f"{prefix}{metric['sciscinet_column']} IS NOT NULL",
    ]
    year_max = metric["complete_year_max"]
    if year_max is not None:
        conditions.append(f"{prefix}project_year <= {int(year_max)}")
        conditions.append(f"{prefix}sciscinet_year <= {int(year_max)}")
    return " AND ".join(conditions)


def spearman_correlation(
    con: duckdb.DuckDBPyConnection,
    metric: dict[str, object],
) -> float | None:
    project_col = str(metric["project_column"])
    sciscinet_col = str(metric["sciscinet_column"])
    result = con.execute(
        f"""
        SELECT corr(project_rank, sciscinet_rank)
        FROM (
            SELECT
                rank() OVER (ORDER BY {project_col})
                    + (count(*) OVER (PARTITION BY {project_col}) - 1) / 2.0
                    AS project_rank,
                rank() OVER (ORDER BY {sciscinet_col})
                    + (count(*) OVER (PARTITION BY {sciscinet_col}) - 1) / 2.0
                    AS sciscinet_rank
            FROM matched
            WHERE {metric_filter(metric)}
        )
        """
    ).fetchone()[0]
    return float(result) if result is not None else None


def overall_metric_summary(
    con: duckdb.DuckDBPyConnection,
    metric: dict[str, object],
) -> dict[str, object]:
    project_col = str(metric["project_column"])
    sciscinet_col = str(metric["sciscinet_column"])
    row = con.execute(
        f"""
        SELECT
            count(*) AS n_compared_works,
            sum(CASE WHEN {project_col} = {sciscinet_col} THEN 1 ELSE 0 END)
                AS exact_match_count,
            avg(CASE WHEN {project_col} = {sciscinet_col} THEN 1.0 ELSE 0.0 END)
                AS exact_match_rate,
            avg({project_col} - {sciscinet_col}) AS mean_difference,
            median({project_col} - {sciscinet_col}) AS median_difference,
            avg(abs({project_col} - {sciscinet_col})) AS mean_absolute_error,
            sqrt(avg(pow({project_col} - {sciscinet_col}, 2)))
                AS root_mean_squared_error,
            corr({project_col}, {sciscinet_col}) AS pearson_correlation,
            avg(CASE
                WHEN ({project_col} = 0 AND {sciscinet_col} = 0)
                  OR ({project_col} > 0 AND {sciscinet_col} > 0)
                THEN 1.0 ELSE 0.0 END) AS zero_nonzero_agreement_rate,
            avg({project_col}) AS project_mean,
            avg({sciscinet_col}) AS sciscinet_mean
        FROM matched
        WHERE {metric_filter(metric)}
        """
    ).fetchone()

    columns = [
        "n_compared_works",
        "exact_match_count",
        "exact_match_rate",
        "mean_difference",
        "median_difference",
        "mean_absolute_error",
        "root_mean_squared_error",
        "pearson_correlation",
        "zero_nonzero_agreement_rate",
        "project_mean",
        "sciscinet_mean",
    ]
    summary = dict(zip(columns, row))
    summary.update(
        {
            "metric": metric["metric"],
            "project_column": project_col,
            "sciscinet_column": sciscinet_col,
            "complete_year_max": metric["complete_year_max"],
            "spearman_correlation": spearman_correlation(con, metric),
        }
    )
    return summary


def build_overall_summary(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    rows = [overall_metric_summary(con, metric) for metric in METRICS]
    columns = [
        "metric",
        "project_column",
        "sciscinet_column",
        "complete_year_max",
        "n_compared_works",
        "exact_match_count",
        "exact_match_rate",
        "mean_difference",
        "median_difference",
        "mean_absolute_error",
        "root_mean_squared_error",
        "pearson_correlation",
        "spearman_correlation",
        "zero_nonzero_agreement_rate",
        "project_mean",
        "sciscinet_mean",
    ]
    return pd.DataFrame(rows).reindex(columns=columns)


def build_year_summary(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    frames = []
    for metric in METRICS:
        project_col = str(metric["project_column"])
        sciscinet_col = str(metric["sciscinet_column"])
        result = con.execute(
            f"""
            SELECT
                project_year AS publication_year,
                count(*) AS n_compared_works,
                avg(CASE WHEN {project_col} = {sciscinet_col}
                    THEN 1.0 ELSE 0.0 END) AS exact_match_rate,
                avg({project_col} - {sciscinet_col}) AS mean_difference,
                median({project_col} - {sciscinet_col}) AS median_difference,
                avg(abs({project_col} - {sciscinet_col})) AS mean_absolute_error,
                sqrt(avg(pow({project_col} - {sciscinet_col}, 2)))
                    AS root_mean_squared_error,
                corr({project_col}, {sciscinet_col}) AS pearson_correlation,
                avg({project_col}) AS project_mean,
                avg({sciscinet_col}) AS sciscinet_mean
            FROM matched
            WHERE {metric_filter(metric)}
            GROUP BY project_year
            ORDER BY project_year
            """
        ).fetchdf()
        result.insert(0, "metric", metric["metric"])
        result.insert(1, "project_column", project_col)
        result.insert(2, "sciscinet_column", sciscinet_col)
        frames.append(result)
    return pd.concat(frames, ignore_index=True)


def build_coverage_summary(
    con: duckdb.DuckDBPyConnection,
    diagnostics: dict[str, int],
) -> pd.DataFrame:
    n_matched = int(con.execute("SELECT count(*) FROM matched").fetchone()[0])
    year_matches = int(
        con.execute(
            "SELECT sum(CASE WHEN publication_year_exact_match THEN 1 ELSE 0 END) "
            "FROM matched"
        ).fetchone()[0]
        or 0
    )
    year_mismatches = n_matched - year_matches

    return pd.DataFrame(
        [
            {
                **diagnostics,
                "comparison_year": COMPARISON_YEAR,
                "n_matched_works": n_matched,
                "project_work_match_rate": (
                    n_matched / diagnostics["n_project_unique_paperids"]
                    if diagnostics["n_project_unique_paperids"]
                    else 0
                ),
                "sciscinet_work_match_rate": (
                    n_matched / diagnostics["n_sciscinet_unique_paperids"]
                    if diagnostics["n_sciscinet_unique_paperids"]
                    else 0
                ),
                "n_publication_year_exact_matches": year_matches,
                "n_publication_year_mismatches": year_mismatches,
                "publication_year_exact_match_rate": (
                    year_matches / n_matched if n_matched else 0
                ),
            }
        ]
    )


def build_largest_differences(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    frames = []
    for metric in METRICS:
        project_col = str(metric["project_column"])
        sciscinet_col = str(metric["sciscinet_column"])
        result = con.execute(
            f"""
            SELECT
                paperid,
                work_id,
                project_year,
                sciscinet_year,
                doctype,
                {project_col} AS project_value,
                {sciscinet_col} AS sciscinet_value,
                {project_col} - {sciscinet_col} AS difference,
                abs({project_col} - {sciscinet_col}) AS absolute_difference
            FROM matched
            WHERE {metric_filter(metric)}
            ORDER BY absolute_difference DESC, paperid
            LIMIT {TOP_DIFFERENCES_PER_METRIC}
            """
        ).fetchdf()
        result.insert(0, "metric", metric["metric"])
        frames.append(result)
    return pd.concat(frames, ignore_index=True)


def clean_number(value: object) -> object:
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return pd.NA
    return value


def write_csv(output: pd.DataFrame, path: Path) -> None:
    output = output.map(clean_number)
    output.to_csv(path, index=False)
    print(f"Saved {path}")


def main() -> None:
    check_input(PROJECT_CITATION_FILE)
    check_input(SCISCINET_FILE)
    outputs = [
        MATCHED_OUTPUT_FILE,
        OVERALL_SUMMARY_FILE,
        YEAR_SUMMARY_FILE,
        COVERAGE_SUMMARY_FILE,
        LARGEST_DIFFERENCES_FILE,
    ]
    check_outputs(outputs)

    con = configure_connection()
    try:
        print("Creating out-of-core input views.")
        create_input_views(con)
        diagnostics = input_diagnostics(con)
        print(f"Input diagnostics: {diagnostics}")

        print("Joining project OpenAlex citations with SciSciNet by normalized work ID.")
        create_matched_table(con)
        export_matched_table(con)

        overall = build_overall_summary(con)
        yearly = build_year_summary(con)
        coverage = build_coverage_summary(con, diagnostics)
        largest_differences = build_largest_differences(con)

        write_csv(overall, OVERALL_SUMMARY_FILE)
        write_csv(yearly, YEAR_SUMMARY_FILE)
        write_csv(coverage, COVERAGE_SUMMARY_FILE)
        write_csv(largest_differences, LARGEST_DIFFERENCES_FILE)
    finally:
        con.close()


if __name__ == "__main__":
    main()
