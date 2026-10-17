"""Build PMID-level new-knowledge production and adoption decomposition."""
from __future__ import annotations

import csv
import os
from pathlib import Path
import tempfile

import duckdb


ROOT = Path('/xdisk/sebratt/jinyugao/research/projects/innovation_capacity')
INPUT_SUBDIR = Path('data/processed/knowledge_combination/edge_adoption_metrics/pmid')
OUTPUT_SUBDIR = Path('data/processed/knowledge_combination/edge_adoption_decomposition/pmid')
INPUT_STEM = 'pmid_edge_adoption_metrics'
STEM = 'pmid_edge_adoption_decomposition'

COLUMNS = [
    'PMID', 'PYEAR',
    'n_triples', 'n_new_triples', 'n_new_triples_adopted',
    'share_new_triples', 'share_new_triples_adopted',
    'share_adopted_new_triples_in_all',
]
TYPES = ['VARCHAR', 'INTEGER', 'BIGINT', 'BIGINT', 'BIGINT', 'DOUBLE', 'DOUBLE', 'DOUBLE']


def q(value):
    return "'" + str(value).replace("'", "''") + "'"


def scalar(con, sql):
    return con.execute(sql).fetchone()[0]


def connect(temp):
    con = duckdb.connect()
    con.execute(f'SET temp_directory={q(temp)}')
    con.execute(f"SET threads={int(os.getenv('IC_DUCKDB_THREADS', '4'))}")
    con.execute(f"SET memory_limit={q(os.getenv('IC_DUCKDB_MEMORY_LIMIT', '12GB'))}")
    return con


def overwrite_enabled():
    return os.getenv('IC_PMID_EDGE_ADOPTION_DECOMPOSITION_OVERWRITE', '0').lower() in ('1', 'true', 'yes')


def guard(paths):
    for path in paths:
        if path.exists() and not overwrite_enabled():
            raise FileExistsError(
                f'{path}; set IC_PMID_EDGE_ADOPTION_DECOMPOSITION_OVERWRITE=1 to replace'
            )
        path.parent.mkdir(parents=True, exist_ok=True)


def write_csv(path, rows):
    if not rows:
        raise ValueError('Cannot write CSV without rows')
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + '.tmp')
    with temporary.open('w', newline='', encoding='utf-8') as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    temporary.replace(path)


def read_one(path):
    with path.open(newline='', encoding='utf-8') as handle:
        rows = list(csv.DictReader(handle))
    if len(rows) != 1:
        raise ValueError(f'Expected one CSV row: {path}')
    return rows[0]


def output_paths(root, year):
    base = root / OUTPUT_SUBDIR
    return (
        base / f'{STEM}_{year}.parquet',
        base / 'summary' / f'{STEM}_summary_{year}.csv',
        base / 'validation' / f'{STEM}_validation_{year}.csv',
    )


def prepare_source(con, root, year):
    path = root / INPUT_SUBDIR / f'{INPUT_STEM}_{year}.parquet'
    if not path.is_file():
        raise FileNotFoundError(path)
    con.execute(f'CREATE OR REPLACE TEMP VIEW source AS SELECT * FROM read_parquet({q(path)})')
    required = {
        'PMID', 'PYEAR', 'n_triples', 'n_new_triples', 'n_new_triples_adopted',
        'share_new_triples_adopted', 'share_adopted_new_triples_in_all',
    }
    available = {r[0] for r in con.execute('DESCRIBE source').fetchall()}
    missing = sorted(required - available)
    if missing:
        raise ValueError(f'Missing source columns: {missing}')

    invalid = scalar(con, f"""
        SELECT COUNT(*) FROM source
        WHERE PMID IS NULL OR trim(CAST(PMID AS VARCHAR))=''
           OR PYEAR IS NULL OR PYEAR<>{year}
           OR n_triples IS NULL OR n_new_triples IS NULL OR n_new_triples_adopted IS NULL
           OR n_triples<=0 OR n_new_triples<0 OR n_new_triples_adopted<0
           OR n_new_triples>n_triples OR n_new_triples_adopted>n_new_triples
           OR (n_new_triples=0 AND share_new_triples_adopted IS NOT NULL)
           OR (n_new_triples>0 AND (
                share_new_triples_adopted IS NULL
                OR abs(share_new_triples_adopted-n_new_triples_adopted::DOUBLE/n_new_triples)>1e-9
           ))
           OR share_adopted_new_triples_in_all IS NULL
           OR abs(share_adopted_new_triples_in_all-n_new_triples_adopted::DOUBLE/n_triples)>1e-9
    """)
    duplicate = scalar(con, 'SELECT COUNT(*)-COUNT(DISTINCT CAST(PMID AS VARCHAR)) FROM source')
    if invalid or duplicate:
        raise ValueError(f'Invalid source rows={invalid}; duplicate PMIDs={duplicate}')
    return scalar(con, 'SELECT COUNT(*) FROM source')


def calculate(con):
    con.execute("""
        CREATE OR REPLACE TEMP VIEW expected AS
        SELECT
            CAST(PMID AS VARCHAR) AS PMID,
            CAST(PYEAR AS INTEGER) AS PYEAR,
            CAST(n_triples AS BIGINT) AS n_triples,
            CAST(n_new_triples AS BIGINT) AS n_new_triples,
            CAST(n_new_triples_adopted AS BIGINT) AS n_new_triples_adopted,
            n_new_triples::DOUBLE/n_triples AS share_new_triples,
            n_new_triples_adopted::DOUBLE/NULLIF(n_new_triples,0) AS share_new_triples_adopted,
            n_new_triples_adopted::DOUBLE/n_triples AS share_adopted_new_triples_in_all
        FROM source
    """)


def validate(con, year):
    schema = [(r[0], r[1]) for r in con.execute('DESCRIBE metrics').fetchall()]
    if schema != list(zip(COLUMNS, TYPES)):
        raise ValueError(f'Incorrect 8-column schema: {schema}')
    invalid = scalar(con, f"""
        SELECT COUNT(*) FROM metrics
        WHERE PMID IS NULL OR trim(PMID)='' OR PYEAR IS NULL OR PYEAR<>{year}
           OR n_triples IS NULL OR n_new_triples IS NULL OR n_new_triples_adopted IS NULL
           OR n_triples<=0 OR n_new_triples<0 OR n_new_triples_adopted<0
           OR n_new_triples>n_triples OR n_new_triples_adopted>n_new_triples
           OR share_new_triples IS NULL OR NOT isfinite(share_new_triples)
           OR share_new_triples<0 OR share_new_triples>1
           OR abs(share_new_triples-n_new_triples::DOUBLE/n_triples)>1e-9
           OR (n_new_triples=0 AND share_new_triples_adopted IS NOT NULL)
           OR (n_new_triples>0 AND (
                share_new_triples_adopted IS NULL
                OR NOT isfinite(share_new_triples_adopted)
                OR share_new_triples_adopted<0 OR share_new_triples_adopted>1
                OR abs(share_new_triples_adopted-n_new_triples_adopted::DOUBLE/n_new_triples)>1e-9
           ))
           OR share_adopted_new_triples_in_all IS NULL
           OR NOT isfinite(share_adopted_new_triples_in_all)
           OR share_adopted_new_triples_in_all<0 OR share_adopted_new_triples_in_all>1
           OR abs(share_adopted_new_triples_in_all-n_new_triples_adopted::DOUBLE/n_triples)>1e-9
           OR (n_new_triples>0 AND abs(
                share_new_triples*share_new_triples_adopted
                - share_adopted_new_triples_in_all
           )>1e-9)
    """)
    duplicate = scalar(con, 'SELECT COUNT(*)-COUNT(DISTINCT PMID) FROM metrics')
    if invalid or duplicate:
        raise ValueError(f'Metric validation failed: invalid rows={invalid}; duplicate PMIDs={duplicate}')


def compare_source(con):
    mismatch = scalar(con, """
        SELECT COUNT(*)
        FROM metrics m
        FULL OUTER JOIN source s ON m.PMID=CAST(s.PMID AS VARCHAR)
        WHERE m.PMID IS NULL OR s.PMID IS NULL
           OR m.PYEAR IS DISTINCT FROM CAST(s.PYEAR AS INTEGER)
           OR m.n_triples IS DISTINCT FROM CAST(s.n_triples AS BIGINT)
           OR m.n_new_triples IS DISTINCT FROM CAST(s.n_new_triples AS BIGINT)
           OR m.n_new_triples_adopted IS DISTINCT FROM CAST(s.n_new_triples_adopted AS BIGINT)
           OR (m.share_new_triples_adopted IS NULL)<>(s.share_new_triples_adopted IS NULL)
           OR (m.share_new_triples_adopted IS NOT NULL
               AND abs(m.share_new_triples_adopted-s.share_new_triples_adopted)>1e-9)
           OR abs(m.share_adopted_new_triples_in_all-s.share_adopted_new_triples_in_all)>1e-9
    """)
    if mismatch:
        raise ValueError(f'Decomposition differs from source metrics for {mismatch} PMID rows')


def summarize(con, year):
    cursor = con.execute("""
        SELECT
            COUNT(*)::BIGINT AS n_pmids,
            COUNT(*) FILTER (WHERE n_new_triples>0)::BIGINT AS n_pmids_with_new_triples,
            COUNT(*) FILTER (WHERE n_new_triples=0)::BIGINT AS n_pmids_without_new_triples,
            COUNT(*) FILTER (WHERE n_new_triples_adopted>0)::BIGINT AS n_pmids_with_adopted_new_triples,
            COALESCE(SUM(n_triples),0)::BIGINT AS sum_n_triples,
            COALESCE(SUM(n_new_triples),0)::BIGINT AS sum_n_new_triples,
            COALESCE(SUM(n_new_triples_adopted),0)::BIGINT AS sum_n_new_triples_adopted,
            AVG(share_new_triples)::DOUBLE AS mean_share_new_triples,
            MEDIAN(share_new_triples)::DOUBLE AS median_share_new_triples,
            AVG(share_new_triples_adopted)::DOUBLE AS mean_share_new_triples_adopted,
            MEDIAN(share_new_triples_adopted)::DOUBLE AS median_share_new_triples_adopted,
            AVG(share_adopted_new_triples_in_all)::DOUBLE AS mean_share_adopted_new_triples_in_all,
            MEDIAN(share_adopted_new_triples_in_all)::DOUBLE AS median_share_adopted_new_triples_in_all,
            SUM(n_new_triples)::DOUBLE/NULLIF(SUM(n_triples),0) AS pooled_share_new_triples,
            SUM(n_new_triples_adopted)::DOUBLE/NULLIF(SUM(n_new_triples),0) AS pooled_share_new_triples_adopted,
            SUM(n_new_triples_adopted)::DOUBLE/NULLIF(SUM(n_triples),0) AS pooled_share_adopted_new_triples_in_all
        FROM metrics
    """)
    return {'PYEAR': year, **dict(zip([d[0] for d in cursor.description], cursor.fetchone()))}


def build(root, year):
    target, summary_path, validation_path = output_paths(root, year)
    guard([target, summary_path, validation_path])
    report = {
        'PYEAR': year, 'n_source_pmids': '', 'n_output_pmids': '',
        'source_valid': False, 'metrics_valid': False, 'source_output_match': False,
        'identity_valid': False, 'all_checks_passed': False, 'error': '',
    }
    try:
        (root / OUTPUT_SUBDIR).mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix='decomposition_', dir=root / OUTPUT_SUBDIR) as temp:
            con = connect(temp)
            try:
                report['n_source_pmids'] = prepare_source(con, root, year)
                report['source_valid'] = True
                calculate(con)
                con.execute('CREATE OR REPLACE TEMP VIEW metrics AS SELECT * FROM expected')
                validate(con, year)
                report['metrics_valid'] = True
                compare_source(con)
                report['source_output_match'] = True
                report['identity_valid'] = True
                row = summarize(con, year)
                report['n_output_pmids'] = row['n_pmids']
                if report['n_source_pmids'] != report['n_output_pmids']:
                    raise ValueError('Source and output PMID counts differ')
                staged = Path(temp) / 'output.parquet'
                con.execute(
                    f'COPY (SELECT * FROM metrics ORDER BY PMID) TO {q(staged)} '
                    '(FORMAT PARQUET, COMPRESSION ZSTD)'
                )
                write_csv(summary_path, [row])
                staged.replace(target)
                report['all_checks_passed'] = True
            finally:
                con.close()
    except Exception as exc:
        report['error'] = str(exc)
        raise
    finally:
        write_csv(validation_path, [report])
    print(f'Saved {target}; {report}')


if __name__ == '__main__':
    task = os.getenv('SLURM_ARRAY_TASK_ID')
    year = 1980 + int(task) if task is not None else int(os.environ['IC_FOCAL_YEAR'])
    if not 1980 <= year <= 2018:
        raise ValueError('Year outside 1980-2018 (complete five-year follow-up)')
    build(Path(os.getenv('IC_PROJECT_ROOT', str(ROOT))), year)
