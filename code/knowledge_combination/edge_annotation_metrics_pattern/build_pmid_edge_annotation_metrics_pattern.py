"""Build annual PMID knowledge-type patterns from existing count metrics."""
from __future__ import annotations

import csv
import math
import os
from pathlib import Path
import tempfile

import duckdb

ROOT = Path('/xdisk/sebratt/jinyugao/research/projects/innovation_capacity')
SUBDIR = Path('data/processed/knowledge_combination/edge_annotation_metrics_pattern')
STEM = 'pmid_edge_annotation_metrics_pattern'
PATTERNS = ['Repeated_Only', 'N', 'C', 'R', 'N+C', 'N+R', 'C+R', 'N+C+R']
COUNTS = ['n_new_node', 'n_new_combination', 'n_new_relation', 'n_repeated_triple']
FLAGS = ['has_new_node', 'has_new_combination', 'has_new_relation']
COLUMNS = ['PMID', 'PYEAR', *FLAGS, 'has_new_triples', 'n_new_knowledge_types', 'knowledge_type_pattern']
TYPES = ['VARCHAR', 'INTEGER', *(['INTEGER'] * 5), 'VARCHAR']


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def scalar(con, sql):
    return con.execute(sql).fetchone()[0]


def guard(paths):
    overwrite = os.getenv('IC_EDGE_ANNOTATION_METRICS_PATTERN_OVERWRITE', '0').lower() in ('1', 'true', 'yes')
    for path in paths:
        if path.exists() and not overwrite:
            raise FileExistsError(f'{path}; set IC_EDGE_ANNOTATION_METRICS_PATTERN_OVERWRITE=1 to replace')
        path.parent.mkdir(parents=True, exist_ok=True)


def connect(temp):
    con = duckdb.connect()
    con.execute(f'SET temp_directory={quote(temp)}')
    con.execute(f"SET threads={int(os.getenv('IC_DUCKDB_THREADS', '4'))}")
    con.execute(f"SET memory_limit={quote(os.getenv('IC_DUCKDB_MEMORY_LIMIT', '12GB'))}")
    return con


def write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix('.csv.tmp')
    with temporary.open('w', newline='', encoding='utf-8') as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    temporary.replace(path)


def pattern_sql():
    return "CASE WHEN has_new_triples = 0 THEN 'Repeated_Only' ELSE concat_ws('+', CASE WHEN has_new_node=1 THEN 'N' END, CASE WHEN has_new_combination=1 THEN 'C' END, CASE WHEN has_new_relation=1 THEN 'R' END) END"


def prepare_source(con, root, year):
    path = root / 'data/processed/knowledge_combination/edge_annotation_metrics' / f'pmid_edge_annotation_metrics_{year}.parquet'
    if not path.is_file():
        raise FileNotFoundError(path)
    con.execute(f'CREATE OR REPLACE TEMP VIEW source AS SELECT * FROM read_parquet({quote(path)})')
    counts = ['n_triples', *COUNTS, 'n_new_triples']
    invalid_counts = ' OR '.join(f'{c} IS NULL OR NOT isfinite({c}) OR {c}<0 OR {c}<>floor({c})' for c in counts)
    condition = (f"PMID IS NULL OR trim(CAST(PMID AS VARCHAR))='' OR PYEAR IS NULL OR PYEAR<>{year} OR " + invalid_counts
                 + ' OR n_triples=0 OR n_triples<>' + '+'.join(COUNTS)
                 + ' OR n_new_triples<>' + '+'.join(COUNTS[:3]))
    invalid = scalar(con, f'SELECT COUNT(*) FROM source WHERE {condition}')
    duplicates = scalar(con, 'SELECT COUNT(*)-COUNT(DISTINCT CAST(PMID AS VARCHAR)) FROM source')
    if invalid or duplicates:
        raise ValueError(f'Invalid source rows={invalid}; duplicate PMID excess rows={duplicates}')
    con.execute('CREATE OR REPLACE TEMP VIEW expected AS SELECT *, ' + pattern_sql() + ' AS knowledge_type_pattern FROM (SELECT CAST(PMID AS VARCHAR) AS PMID, CAST(PYEAR AS INTEGER) AS PYEAR, '
                + ', '.join(f'CAST({count}>0 AS INTEGER) AS {flag}' for count, flag in zip(COUNTS[:3], FLAGS))
                + ', CAST(n_new_triples>0 AS INTEGER) AS has_new_triples, ('
                + '+'.join(f'CAST({c}>0 AS INTEGER)' for c in COUNTS[:3]) + ')::INTEGER AS n_new_knowledge_types FROM source)')


def validate(con, year):
    schema = [(r[0], r[1]) for r in con.execute('DESCRIBE patterns').fetchall()]
    result = {'schema_correct': schema == list(zip(COLUMNS, TYPES)), 'invalid_rows': '', 'duplicate_pmid_excess_rows': '', 'input_output_mismatch_rows': ''}
    if not result['schema_correct']:
        return result
    condition = f"PMID IS NULL OR trim(PMID)='' OR PYEAR IS NULL OR PYEAR<>{year} OR "
    condition += ' OR '.join(f'{c} IS NULL OR {c} NOT IN (0,1)' for c in FLAGS + ['has_new_triples'])
    condition += ' OR n_new_knowledge_types IS NULL OR n_new_knowledge_types<>' + '+'.join(FLAGS)
    condition += ' OR has_new_triples<>CAST(n_new_knowledge_types>0 AS INTEGER) OR knowledge_type_pattern IS NULL OR knowledge_type_pattern<>' + pattern_sql()
    result['invalid_rows'] = scalar(con, f'SELECT COUNT(*) FROM patterns WHERE {condition}')
    result['duplicate_pmid_excess_rows'] = scalar(con, 'SELECT COUNT(*)-COUNT(DISTINCT PMID) FROM patterns')
    result['input_output_mismatch_rows'] = scalar(con, 'SELECT COUNT(*) FROM ((SELECT * FROM patterns EXCEPT ALL SELECT * FROM expected) UNION ALL (SELECT * FROM expected EXCEPT ALL SELECT * FROM patterns))')
    return result


def checks_pass(checks):
    return checks['schema_correct'] and all(v == 0 for k,v in checks.items() if k != 'schema_correct')


def summarize(con, year):
    total = scalar(con, 'SELECT COUNT(*) FROM patterns')
    grouped = con.execute('SELECT p.knowledge_type_pattern, COUNT(*), AVG(s.n_triples), MEDIAN(s.n_triples) FROM patterns p JOIN source s ON p.PMID=CAST(s.PMID AS VARCHAR) GROUP BY p.knowledge_type_pattern').fetchall()
    lookup = {r[0]: r[1:] for r in grouped}
    rows = []
    for label in PATTERNS:
        count, mean, median = lookup.get(label, (0, None, None))
        rows.append({'PYEAR': year, 'knowledge_type_pattern': label, 'n_pmids': count, 'n_pmids_year': total, 'share_pmids': count/total if total else None, 'mean_n_triples': mean, 'median_n_triples': median})
    if sum(r['n_pmids'] for r in rows) != total:
        raise ValueError('Pattern counts do not sum to annual PMID count')
    return rows


def summary_matches(path, expected):
    with path.open(newline='', encoding='utf-8') as handle:
        actual = list(csv.DictReader(handle))
    if len(actual) != 8 or {r['knowledge_type_pattern'] for r in actual} != set(PATTERNS):
        return False
    lookup = {r['knowledge_type_pattern']: r for r in actual}
    for wanted in expected:
        observed = lookup[wanted['knowledge_type_pattern']]
        for key, value in wanted.items():
            text = observed.get(key)
            if value is None:
                if text != '':
                    return False
            elif isinstance(value, str):
                if text != value:
                    return False
            elif isinstance(value, int):
                if text is None or int(text) != value:
                    return False
            elif text is None or not math.isclose(float(text), value, rel_tol=1e-9, abs_tol=1e-9):
                return False
    return True


def build(root, year):
    base = root / SUBDIR
    target = base / f'{STEM}_{year}.parquet'
    report = base / 'summary' / f'{STEM}_summary_{year}.csv'
    guard([target, report])
    with tempfile.TemporaryDirectory(prefix='patterns_', dir=base) as temp:
        con = connect(temp)
        try:
            prepare_source(con, root, year)
            con.execute('CREATE TEMP VIEW patterns AS SELECT * FROM expected')
            checks = validate(con, year)
            if not checks_pass(checks):
                raise ValueError(checks)
            rows = summarize(con, year)
            staged = Path(temp) / 'annual.parquet'
            con.execute(f'COPY (SELECT * FROM patterns ORDER BY PMID) TO {quote(staged)} (FORMAT PARQUET, COMPRESSION ZSTD)')
            staged.replace(target)
            write_csv(report, rows)
            print(f'Saved {target}; n_pmids={sum(r["n_pmids"] for r in rows)}; checks={checks}')
        finally:
            con.close()


if __name__ == '__main__':
    task = os.getenv('SLURM_ARRAY_TASK_ID')
    year = 1980 + int(task) if task is not None else int(os.environ['IC_FOCAL_YEAR'])
    if not 1980 <= year <= 2023:
        raise ValueError(f'Year outside 1980-2023: {year}')
    build(Path(os.getenv('IC_PROJECT_ROOT', str(ROOT))), year)
