"""Aggregate five-year exact-triple transitions into 27 PMID-level columns."""
from __future__ import annotations

import csv
import os
from pathlib import Path
import tempfile

import duckdb

ROOT = Path('/xdisk/sebratt/jinyugao/research/projects/innovation_capacity')
SUBDIR = Path('data/processed/knowledge_combination/edge_adoption_metrics/pmid')
STEM = 'pmid_edge_adoption_metrics'
CATEGORIES = ['New_Node', 'New_Combination', 'New_Relation', 'Repeated_Triple']
CATEGORY_COUNTS = ['n_new_node', 'n_new_combination', 'n_new_relation', 'n_repeated_triple']
BASE_COUNTS = ['n_triples', *CATEGORY_COUNTS, 'n_new_triples']
OUTCOME_COUNTS = ['n_new_node_adopted', 'n_new_combination_adopted', 'n_new_relation_adopted', 'n_new_triples_adopted', 'n_repeated_triple_continued', 'n_triples_reappeared']
RATIOS = [
    ('share_new_node_adopted', 'n_new_node_adopted', 'n_new_node'),
    ('share_new_combination_adopted', 'n_new_combination_adopted', 'n_new_combination'),
    ('share_new_relation_adopted', 'n_new_relation_adopted', 'n_new_relation'),
    ('share_new_triples_adopted', 'n_new_triples_adopted', 'n_new_triples'),
    ('share_repeated_triple_continued', 'n_repeated_triple_continued', 'n_repeated_triple'),
    ('share_triples_reappeared', 'n_triples_reappeared', 'n_triples'),
    ('share_adopted_new_triples_in_all', 'n_new_triples_adopted', 'n_triples'),
]
FLAGS = ['has_new_node_adopted', 'has_new_combination_adopted', 'has_new_relation_adopted', 'has_new_triples_adopted', 'has_repeated_triple_continued', 'has_triples_reappeared']
COLUMNS = ['PMID', 'PYEAR', *BASE_COUNTS, *OUTCOME_COUNTS, *[r[0] for r in RATIOS], *FLAGS]
TYPES = ['VARCHAR', 'INTEGER'] + ['BIGINT'] * 12 + ['DOUBLE'] * 7 + ['INTEGER'] * 6


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


def guard(paths):
    overwrite = os.getenv('IC_PMID_EDGE_ADOPTION_METRICS_OVERWRITE', '0').lower() in ('1', 'true', 'yes')
    for path in paths:
        if path.exists() and not overwrite:
            raise FileExistsError(f'{path}; set IC_PMID_EDGE_ADOPTION_METRICS_OVERWRITE=1 to replace')
        path.parent.mkdir(parents=True, exist_ok=True)


def write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix('.csv.tmp')
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
    base = root / SUBDIR
    return (base / f'{STEM}_{year}.parquet', base / 'summary' / f'{STEM}_summary_{year}.csv', base / 'validation' / f'{STEM}_validation_{year}.csv')


def prepare_source(con, root, year):
    path = root / 'data/processed/knowledge_combination/edge_transition' / f'edge_transition_{year}.parquet'
    if not path.is_file():
        raise FileNotFoundError(path)
    con.execute(f'CREATE OR REPLACE TEMP VIEW source AS SELECT * FROM read_parquet({q(path)})')
    schema = dict((r[0], r[1]) for r in con.execute('DESCRIBE source').fetchall())
    if schema.get('future_exact_triple_seen') != 'BOOLEAN':
        raise ValueError('Expected Boolean future_exact_triple_seen')
    required = ['PMID', 'subject_cui_primary', 'PREDICATE', 'object_cui_primary']
    condition = ' OR '.join(f'{c} IS NULL OR trim(CAST({c} AS VARCHAR))=\'\'' for c in required)
    condition += f' OR PYEAR IS NULL OR PYEAR<>{year} OR subject_cui_primary=object_cui_primary OR future_exact_triple_seen IS NULL'
    condition += ' OR edge_annotation IS NULL OR edge_annotation NOT IN (' + ','.join(map(q, CATEGORIES)) + ')'
    expected = "CASE WHEN NOT future_exact_triple_seen THEN 'Disappeared' WHEN edge_annotation='Repeated_Triple' THEN 'Continued' ELSE 'Adopted' END"
    condition += f' OR future_five_year_transition IS NULL OR future_five_year_transition<>{expected}'
    invalid = scalar(con, f'SELECT COUNT(*) FROM source WHERE {condition}')
    duplicate = scalar(con, 'SELECT COALESCE(SUM(n-1),0) FROM (SELECT COUNT(*) n FROM source GROUP BY CAST(PMID AS VARCHAR),subject_cui_primary,PREDICATE,object_cui_primary HAVING COUNT(*)>1)')
    inconsistent = scalar(con, 'SELECT COUNT(*) FROM (SELECT subject_cui_primary,PREDICATE,object_cui_primary FROM source GROUP BY subject_cui_primary,PREDICATE,object_cui_primary HAVING COUNT(DISTINCT edge_annotation)>1 OR COUNT(DISTINCT future_exact_triple_seen)>1)')
    if invalid or duplicate or inconsistent:
        raise ValueError(f'Invalid source rows={invalid}; duplicate PMID triples={duplicate}; inconsistent exact triples={inconsistent}')
    count = scalar(con, 'SELECT COUNT(*) FROM source')
    return count


def aggregate(con):
    expressions = ['COUNT(*)::BIGINT AS n_triples']
    expressions += [f'COUNT(*) FILTER (WHERE edge_annotation={q(cat)})::BIGINT AS {name}' for cat, name in zip(CATEGORIES, CATEGORY_COUNTS)]
    expressions += ["COUNT(*) FILTER (WHERE edge_annotation<>'Repeated_Triple')::BIGINT AS n_new_triples"]
    expressions += [f"COUNT(*) FILTER (WHERE edge_annotation={q(cat)} AND future_five_year_transition='Adopted')::BIGINT AS {name}" for cat, name in zip(CATEGORIES[:3], OUTCOME_COUNTS[:3])]
    expressions += ["COUNT(*) FILTER (WHERE future_five_year_transition='Adopted')::BIGINT AS n_new_triples_adopted", "COUNT(*) FILTER (WHERE future_five_year_transition='Continued')::BIGINT AS n_repeated_triple_continued", 'COUNT(*) FILTER (WHERE future_exact_triple_seen)::BIGINT AS n_triples_reappeared']
    con.execute('CREATE OR REPLACE TEMP TABLE counts AS SELECT CAST(PMID AS VARCHAR) PMID, CAST(PYEAR AS INTEGER) PYEAR, ' + ','.join(expressions) + ' FROM source GROUP BY CAST(PMID AS VARCHAR),PYEAR')
    ratios = [f'{n}::DOUBLE / NULLIF({d},0) AS {s}' for s, n, d in RATIOS]
    flags = [f'CAST({n}>0 AS INTEGER) AS {f}' for f, n in zip(FLAGS, OUTCOME_COUNTS)]
    con.execute('CREATE OR REPLACE TEMP VIEW expected AS SELECT *, ' + ','.join(ratios + flags) + ' FROM counts')


def validate(con, year):
    schema = [(r[0], r[1]) for r in con.execute('DESCRIBE metrics').fetchall()]
    if schema != list(zip(COLUMNS, TYPES)):
        raise ValueError('Incorrect 27-column schema')
    conditions = [f'PYEAR IS NULL OR PYEAR<>{year}', "PMID IS NULL OR trim(PMID)=''", 'n_triples=0']
    conditions += [f'{c} IS NULL OR {c}<0' for c in BASE_COUNTS + OUTCOME_COUNTS]
    conditions += ['n_triples<>' + '+'.join(CATEGORY_COUNTS), 'n_new_triples<>' + '+'.join(CATEGORY_COUNTS[:3]), 'n_new_triples_adopted<>' + '+'.join(OUTCOME_COUNTS[:3]), 'n_triples_reappeared<>n_new_triples_adopted+n_repeated_triple_continued']
    for s, n, d in RATIOS:
        conditions += [f'{n}>{d}', f'({d}=0 AND {s} IS NOT NULL)', f'({d}>0 AND ({s} IS NULL OR NOT isfinite({s}) OR {s}<0 OR {s}>1 OR abs({s}-{n}::DOUBLE/{d})>1e-9))']
    conditions += [f'{f} IS NULL OR {f}<>CAST({n}>0 AS INTEGER)' for f, n in zip(FLAGS, OUTCOME_COUNTS)]
    if scalar(con, 'SELECT COUNT(*) FROM metrics WHERE ' + ' OR '.join(conditions)):
        raise ValueError('Metric count, ratio or flag validation failed')
    if scalar(con, 'SELECT COUNT(*)-COUNT(DISTINCT PMID) FROM metrics'):
        raise ValueError('Duplicate PMIDs')


def compare_source(con):
    condition = ['a.PMID IS NULL', 'b.PMID IS NULL', 'a.PYEAR IS DISTINCT FROM b.PYEAR']
    condition += [f'a.{c} IS DISTINCT FROM b.{c}' for c in BASE_COUNTS + OUTCOME_COUNTS + FLAGS]
    for s, _, _ in RATIOS:
        condition.append(f'(a.{s} IS NULL)<>(b.{s} IS NULL) OR abs(a.{s}-b.{s})>1e-9')
    if scalar(con, 'SELECT COUNT(*) FROM metrics a FULL OUTER JOIN expected b ON a.PMID=b.PMID WHERE ' + ' OR '.join(condition)):
        raise ValueError('Metrics differ from source transition aggregates')


def summarize(con, year, n_input):
    expressions = ['COUNT(*) AS n_pmids']
    expressions += [f'COUNT(*) FILTER (WHERE {f}=1) AS n_pmids_{f}' for f in FLAGS]
    expressions += [f'COALESCE(SUM({c}),0) AS {c}' for c in BASE_COUNTS + OUTCOME_COUNTS]
    cursor = con.execute('SELECT ' + ','.join(expressions) + ' FROM metrics')
    result = {'PYEAR': year, 'n_input_triples': n_input, **dict(zip([d[0] for d in cursor.description], cursor.fetchone()))}
    if result['n_triples'] != n_input:
        raise ValueError('Input/output triple totals differ')
    return result


def build(root, year):
    target, summary, validation = output_paths(root, year)
    guard([target, summary, validation])
    report = {'PYEAR': year, 'n_input_triples': '', 'n_pmids': '', 'source_valid': False, 'metrics_valid': False, 'source_output_match': False, 'all_checks_passed': False, 'error': ''}
    try:
        with tempfile.TemporaryDirectory(prefix='adoption_', dir=root / SUBDIR) as temp:
            con = connect(temp)
            try:
                report['n_input_triples'] = prepare_source(con, root, year)
                report['source_valid'] = True
                aggregate(con)
                con.execute('CREATE TEMP VIEW metrics AS SELECT * FROM expected')
                validate(con, year); report['metrics_valid'] = True
                compare_source(con); report['source_output_match'] = True
                row = summarize(con, year, report['n_input_triples'])
                report['n_pmids'] = row['n_pmids']
                staged = Path(temp) / 'output.parquet'
                con.execute(f'COPY (SELECT * FROM metrics ORDER BY PMID) TO {q(staged)} (FORMAT PARQUET, COMPRESSION ZSTD)')
                write_csv(summary, [row])
                staged.replace(target)
                report['all_checks_passed'] = True
            finally:
                con.close()
    except Exception as exc:
        report['error'] = str(exc)
        raise
    finally:
        write_csv(validation, [report])
    print(f'Saved {target}; {report}')


if __name__ == '__main__':
    task = os.getenv('SLURM_ARRAY_TASK_ID')
    year = 1980 + int(task) if task is not None else int(os.environ['IC_FOCAL_YEAR'])
    if not 1980 <= year <= 2018:
        raise ValueError('Year outside 1980-2018 (complete five-year follow-up)')
    build(Path(os.getenv('IC_PROJECT_ROOT', str(ROOT))), year)
