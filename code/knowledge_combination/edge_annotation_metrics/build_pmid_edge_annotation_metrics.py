"""Aggregate one year's existing edge annotations to 16 PMID-level metrics."""
from __future__ import annotations
import csv
import os
from pathlib import Path
import tempfile
import duckdb

ROOT = Path('/xdisk/sebratt/jinyugao/research/projects/innovation_capacity')
SUBDIR = Path('data/processed/knowledge_combination/edge_annotation_metrics')
CATEGORIES = ['New_Node', 'New_Combination', 'New_Relation', 'Repeated_Triple']
COUNTS = ['n_new_node', 'n_new_combination', 'n_new_relation', 'n_repeated_triple']
SHARES = ['share_new_node', 'share_new_combination', 'share_new_relation', 'share_repeated_triple']
INNER = ['share_new_node_among_new', 'share_new_combination_among_new', 'share_new_relation_among_new']
COLUMNS = ['PMID', 'PYEAR', 'n_triples', *COUNTS, 'n_new_triples', *SHARES, 'share_new_triples', *INNER]
TYPES = ['VARCHAR', 'INTEGER'] + ['BIGINT'] * 6 + ['DOUBLE'] * 8

def quote(value):
    return "'" + str(value).replace("'", "''") + "'"

def scalar(con, sql):
    return con.execute(sql).fetchone()[0]

def write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + '.tmp')
    with temp.open('w', newline='', encoding='utf-8') as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    temp.replace(path)

def guard(paths):
    overwrite = os.getenv('IC_EDGE_ANNOTATION_METRICS_OVERWRITE', '0').lower() in ('1', 'true', 'yes')
    for path in paths:
        if path.exists() and not overwrite:
            raise FileExistsError(f'{path}; set IC_EDGE_ANNOTATION_METRICS_OVERWRITE=1 to replace')
        path.parent.mkdir(parents=True, exist_ok=True)

def connect(temp):
    con = duckdb.connect()
    con.execute(f'SET temp_directory={quote(temp)}')
    con.execute(f"SET threads={int(os.getenv('IC_DUCKDB_THREADS', '4'))}")
    con.execute(f"SET memory_limit={quote(os.getenv('IC_DUCKDB_MEMORY_LIMIT', '12GB'))}")
    return con

def summary(con, year, input_count):
    values = con.execute('SELECT COUNT(*), COUNT(*) FILTER (WHERE n_new_triples > 0), COUNT(*) FILTER (WHERE n_new_triples = 0), ' + ', '.join(f'COALESCE(SUM({c}),0)' for c in ['n_triples', *COUNTS, 'n_new_triples']) + ' FROM metrics').fetchone()
    keys = ['n_pmids', 'n_pmids_with_new_triples', 'n_pmids_without_new_triples', 'n_triples', *COUNTS, 'n_new_triples']
    return {'PYEAR': year, 'n_input_triples': input_count, **dict(zip(keys, values))}

def validate(con, year):
    schema = con.execute('DESCRIBE metrics').fetchall()
    result = {'schema_correct': [(r[0], r[1]) for r in schema] == list(zip(COLUMNS, TYPES))}
    if not result['schema_correct']:
        return result
    checks = {
        'invalid_year_rows': f'PYEAR IS NULL OR PYEAR <> {year}',
        'invalid_pmid_rows': "PMID IS NULL OR trim(PMID) = ''",
        'invalid_count_rows': ' OR '.join(f'{c} IS NULL OR {c} < 0' for c in ['n_triples', *COUNTS, 'n_new_triples']) + ' OR n_triples = 0 OR n_triples <> ' + ' + '.join(COUNTS) + ' OR n_new_triples <> ' + ' + '.join(COUNTS[:3]),
    }
    main = list(zip(SHARES + ['share_new_triples'], COUNTS + ['n_new_triples']))
    checks['invalid_main_share_rows'] = ' OR '.join(f'{s} IS NULL OR NOT isfinite({s}) OR {s} < 0 OR {s} > 1 OR abs({s} - {n}::DOUBLE / NULLIF(n_triples,0)) > 1e-9' for s,n in main) + ' OR abs(' + ' + '.join(SHARES) + ' - 1) > 1e-9'
    checks['invalid_among_new_share_rows'] = '(n_new_triples = 0 AND (' + ' OR '.join(f'{s} IS NOT NULL' for s in INNER) + ')) OR (n_new_triples > 0 AND (' + ' OR '.join(f'{s} IS NULL OR NOT isfinite({s}) OR {s} < 0 OR {s} > 1 OR abs({s} - {n}::DOUBLE / NULLIF(n_new_triples,0)) > 1e-9' for s,n in zip(INNER, COUNTS[:3])) + ' OR abs(' + ' + '.join(INNER) + ' - 1) > 1e-9))'
    for key, condition in checks.items():
        result[key] = scalar(con, f'SELECT COUNT(*) FROM metrics WHERE {condition}')
    result['duplicate_pmid_excess_rows'] = scalar(con, 'SELECT COUNT(*) - COUNT(DISTINCT PMID) FROM metrics')
    return result

def checks_pass(result):
    return result.get('schema_correct') is True and all(v == 0 for k,v in result.items() if k != 'schema_correct')

def build(root, year):
    source = root / 'data/processed/knowledge_combination/edge_annotation' / f'edge_annotation_{year}.parquet'
    target = root / SUBDIR / f'pmid_edge_annotation_metrics_{year}.parquet'
    report = root / SUBDIR / 'summary' / f'pmid_edge_annotation_metrics_summary_{year}.csv'
    if not source.is_file():
        raise FileNotFoundError(source)
    guard([target, report])
    with tempfile.TemporaryDirectory(prefix='pmid_metrics_', dir=target.parent) as temp:
        con = connect(temp)
        try:
            con.execute(f'CREATE VIEW source AS SELECT * FROM read_parquet({quote(source)})')
            invalid = scalar(con, "SELECT COUNT(*) FROM source WHERE PMID IS NULL OR trim(CAST(PMID AS VARCHAR)) = '' OR PYEAR IS NULL OR PYEAR <> " + str(year) + " OR subject_cui_primary IS NULL OR object_cui_primary IS NULL OR PREDICATE IS NULL OR trim(subject_cui_primary) = '' OR trim(object_cui_primary) = '' OR trim(PREDICATE) = '' OR subject_cui_primary = object_cui_primary OR edge_annotation IS NULL OR edge_annotation NOT IN (" + ','.join(map(quote, CATEGORIES)) + ')')
            duplicates = scalar(con, 'SELECT COALESCE(SUM(n-1),0) FROM (SELECT COUNT(*) n FROM source GROUP BY PMID, subject_cui_primary, PREDICATE, object_cui_primary HAVING COUNT(*) > 1)')
            if invalid or duplicates:
                raise ValueError(f'Invalid input rows={invalid}; duplicate PMID-triple excess rows={duplicates}')
            count_sql = ', '.join(f'COUNT(*) FILTER (WHERE edge_annotation={quote(cat)})::BIGINT AS {col}' for cat,col in zip(CATEGORIES, COUNTS))
            con.execute(f'CREATE TEMP TABLE counts AS SELECT CAST(PMID AS VARCHAR) PMID, CAST(PYEAR AS INTEGER) PYEAR, COUNT(*)::BIGINT n_triples, {count_sql} FROM source GROUP BY PMID, PYEAR')
            con.execute('CREATE TEMP VIEW totals AS SELECT *, (' + ' + '.join(COUNTS[:3]) + ')::BIGINT n_new_triples FROM counts')
            ratios = [f'{n}::DOUBLE / n_triples AS {s}' for s,n in zip(SHARES + ['share_new_triples'], COUNTS + ['n_new_triples'])] + [f'{n}::DOUBLE / NULLIF(n_new_triples,0) AS {s}' for s,n in zip(INNER, COUNTS[:3])]
            con.execute('CREATE TEMP VIEW metrics AS SELECT *, ' + ', '.join(ratios) + ' FROM totals')
            result = validate(con, year)
            if not checks_pass(result):
                raise ValueError(result)
            record = summary(con, year, scalar(con, 'SELECT COUNT(*) FROM source'))
            input_totals = con.execute('SELECT ' + ', '.join(f'COUNT(*) FILTER (WHERE edge_annotation={quote(cat)})' for cat in CATEGORIES) + ' FROM source').fetchone()
            if record['n_input_triples'] != record['n_triples'] or any(record[c] != v for c,v in zip(COUNTS,input_totals)):
                raise ValueError('Input/output category totals differ')
            staged = Path(temp) / 'output.parquet'
            con.execute(f'COPY (SELECT * FROM metrics ORDER BY PMID) TO {quote(staged)} (FORMAT PARQUET, COMPRESSION ZSTD)')
            staged.replace(target)
            write_csv(report, [record])
            print(f'Saved {target}; {record}; validation={result}')
        finally:
            con.close()

def main():
    task = os.getenv('SLURM_ARRAY_TASK_ID')
    year = 1980 + int(task) if task is not None else int(os.environ['IC_FOCAL_YEAR'])
    if not 1980 <= year <= 2023:
        raise ValueError(f'Year outside 1980-2023: {year}')
    build(Path(os.getenv('IC_PROJECT_ROOT', str(ROOT))), year)

if __name__ == '__main__':
    main()
