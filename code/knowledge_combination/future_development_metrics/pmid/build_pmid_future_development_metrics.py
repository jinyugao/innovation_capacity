"""Aggregate existing development and transition results to 44 PMID-level columns."""
from __future__ import annotations

import csv
import os
from pathlib import Path
import tempfile

import duckdb

ROOT = Path('/xdisk/sebratt/jinyugao/research/projects/innovation_capacity')
SUBDIR = Path('data/processed/knowledge_combination/future_development_metrics/pmid')
STEM = 'pmid_future_development_metrics'
CATEGORIES = ['New_Node', 'New_Combination', 'New_Relation', 'Repeated_Triple']
KEY = ['PMID', 'PYEAR', 'subject_cui_primary', 'PREDICATE', 'object_cui_primary']
DENOMS = ['n_new_node', 'n_new_combination', 'n_unique_new_combination_pairs']
PATHS = ['nn_future_nc', 'nn_future_nr', 'nn_future_any', 'nc_future_nr']
DEV = ['nn_dev_neither', 'nn_dev_nc_only', 'nn_dev_nr_only', 'nn_dev_both']
JOINT = [f'{category}_{state}' for category in ['nn', 'nc'] for state in ['neither_adopted_nor_developed', 'adopted_only', 'developed_only', 'adopted_and_developed']]
PAIR = 'nc_pairs_future_nr'
COUNTS = DENOMS + ['n_' + x for x in PATHS + DEV + JOINT + [PAIR]]
RATIOS = [('share_' + x, 'n_' + x, 'n_unique_new_combination_pairs' if x == PAIR else 'n_new_node' if x.startswith('nn_') else 'n_new_combination') for x in PATHS + DEV + JOINT + [PAIR]]
FLAGS = [('has_' + x, 'n_' + x) for x in PATHS + [PAIR]]
COLUMNS = ['PMID', 'PYEAR', *DENOMS]
for name in PATHS:
    COLUMNS += ['n_' + name, 'share_' + name, 'has_' + name]
for name in DEV + JOINT:
    COLUMNS += ['n_' + name, 'share_' + name]
COLUMNS += ['n_' + PAIR, 'share_' + PAIR, 'has_' + PAIR]
TYPES = ['VARCHAR' if c == 'PMID' else 'INTEGER' if c == 'PYEAR' or c.startswith('has_') else 'DOUBLE' if c.startswith('share_') else 'BIGINT' for c in COLUMNS]


def q(value):
    return "'" + str(value).replace("'", "''") + "'"


def scalar(con, sql):
    return con.execute(sql).fetchone()[0]


def connect(temp):
    con = duckdb.connect()
    con.execute(f'SET temp_directory={q(temp)}')
    con.execute(f"SET threads={int(os.getenv('IC_DUCKDB_THREADS', '4'))}")
    con.execute(f"SET memory_limit={q(os.getenv('IC_DUCKDB_MEMORY_LIMIT', '24GB'))}")
    return con


def guard(paths):
    overwrite = os.getenv('IC_PMID_FUTURE_DEVELOPMENT_METRICS_OVERWRITE', '0').lower() in ('1', 'true', 'yes')
    for path in paths:
        if path.exists() and not overwrite:
            raise FileExistsError(f'{path}; set IC_PMID_FUTURE_DEVELOPMENT_METRICS_OVERWRITE=1 to replace')
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


def reject(con, sql, message):
    if scalar(con, sql):
        raise ValueError(message)


def load(con, name, path, year, flags):
    if not path.is_file():
        raise FileNotFoundError(path)
    con.execute(f'CREATE TEMP VIEW {name} AS SELECT * REPLACE (CAST(PMID AS VARCHAR) AS PMID) FROM read_parquet({q(path)})')
    schema = dict((r[0], r[1]) for r in con.execute(f'DESCRIBE {name}').fetchall())
    if any(schema.get(f) != 'BOOLEAN' for f in flags):
        raise ValueError(f'{name}: missing or non-Boolean flags')
    invalid = [f'{c} IS NULL OR trim(CAST({c} AS VARCHAR))=\'\'' for c in KEY if c != 'PYEAR']
    invalid += [f'PYEAR IS NULL OR PYEAR<>{year}', 'subject_cui_primary=object_cui_primary', 'edge_annotation IS NULL', 'edge_annotation NOT IN (' + ','.join(map(q,CATEGORIES)) + ')']
    invalid += [f'{f} IS NULL' for f in flags]
    reject(con, f'SELECT COUNT(*) FROM {name} WHERE ' + ' OR '.join(invalid), f'{name}: invalid keys, year, category or flags')
    reject(con, f'SELECT COUNT(*) FROM (SELECT {",".join(KEY)} FROM {name} GROUP BY {",".join(KEY)} HAVING COUNT(*)>1)', f'{name}: duplicate PMID-triple keys')


def same_rows(con, left, right, cols, label):
    select = ','.join(cols)
    reject(con, f'SELECT COUNT(*) FROM ((SELECT {select} FROM {left} EXCEPT ALL SELECT {select} FROM {right}) UNION ALL (SELECT {select} FROM {right} EXCEPT ALL SELECT {select} FROM {left}))', f'{label}: source rows missing, extra or inconsistent')


def check_development_counts(con, name, prefixes):
    for prefix, flag in prefixes:
        rows, triples = prefix + '_rows', prefix + '_triples'
        condition = f'{rows} IS NULL OR {triples} IS NULL OR NOT isfinite({rows}) OR NOT isfinite({triples}) OR {rows}<0 OR {triples}<0 OR {rows}<>floor({rows}) OR {triples}<>floor({triples}) OR {triples}>{rows} OR ({rows}>0)<>({triples}>0) OR {flag}<>({rows}>0)'
        reject(con, f'SELECT COUNT(*) FROM {name} WHERE {condition}', f'{name}: development count/flag inconsistency')


def prepare_source(con, root, year):
    base = root / 'data/processed/knowledge_combination'
    history = ['subject_seen_in_prior_window', 'object_seen_in_prior_window']
    nn_flags = ['future_focal_new_node_new_combination_seen', 'future_focal_new_node_new_relation_seen']
    nc_flag = 'future_focal_pair_new_relation_seen'
    load(con, 'annotation', base/'edge_annotation'/f'edge_annotation_{year}.parquet', year, history)
    load(con, 'transition', base/'edge_transition'/f'edge_transition_{year}.parquet', year, ['future_exact_triple_seen'])
    load(con, 'nn', base/'future_development/new_node'/f'future_development_new_node_{year}.parquet', year, history+nn_flags+['focal_subject_is_new_node', 'focal_object_is_new_node'])
    load(con, 'nc', base/'future_development/new_combination'/f'future_development_new_combination_{year}.parquet', year, [nc_flag])
    same_rows(con, 'annotation', 'transition', KEY+['edge_annotation'], 'Transition coverage')
    same_rows(con, "(SELECT * FROM annotation WHERE edge_annotation='New_Node')", 'nn', KEY+['edge_annotation']+history, 'New Node coverage')
    same_rows(con, "(SELECT * FROM annotation WHERE edge_annotation='New_Combination')", 'nc', KEY+['edge_annotation'], 'New Combination coverage')
    expected = "CASE WHEN NOT future_exact_triple_seen THEN 'Disappeared' WHEN edge_annotation='Repeated_Triple' THEN 'Continued' ELSE 'Adopted' END"
    reject(con, f'SELECT COUNT(*) FROM transition WHERE future_five_year_transition IS NULL OR future_five_year_transition<>{expected}', 'Invalid transition state')
    reject(con, "SELECT COUNT(*) FROM annotation WHERE (edge_annotation='New_Node') IS DISTINCT FROM (NOT subject_seen_in_prior_window OR NOT object_seen_in_prior_window)", 'New Node classification conflicts with historical flags')
    reject(con, 'SELECT COUNT(*) FROM nn WHERE focal_subject_is_new_node IS DISTINCT FROM NOT subject_seen_in_prior_window OR focal_object_is_new_node IS DISTINCT FROM NOT object_seen_in_prior_window OR n_focal_new_nodes IS DISTINCT FROM (CAST(focal_subject_is_new_node AS INTEGER)+CAST(focal_object_is_new_node AS INTEGER))', 'Invalid focal new-node flags')
    reject(con, 'SELECT COUNT(*) FROM nc WHERE node_a IS DISTINCT FROM least(subject_cui_primary,object_cui_primary) OR node_b IS DISTINCT FROM greatest(subject_cui_primary,object_cui_primary)', 'Invalid canonical pair')
    check_development_counts(con, 'nn', [('n_future_focal_new_node_new_combination', nn_flags[0]), ('n_future_focal_new_node_new_relation', nn_flags[1])])
    check_development_counts(con, 'nc', [('n_future_focal_pair_new_relation', nc_flag)])
    reject(con, 'SELECT COUNT(*) FROM (SELECT node_a,node_b FROM nc GROUP BY node_a,node_b HAVING COUNT(DISTINCT future_focal_pair_new_relation_seen)>1 OR COUNT(DISTINCT n_future_focal_pair_new_relation_rows)>1 OR COUNT(DISTINCT n_future_focal_pair_new_relation_triples)>1)', 'Inconsistent pair development results')
    reject(con, 'SELECT COUNT(*) FROM (SELECT subject_cui_primary,PREDICATE,object_cui_primary FROM transition GROUP BY subject_cui_primary,PREDICATE,object_cui_primary HAVING COUNT(DISTINCT edge_annotation)>1 OR COUNT(DISTINCT future_exact_triple_seen)>1)', 'Inconsistent exact-triple transition')
    reject(con, 'SELECT COUNT(*) FROM (SELECT subject_cui_primary,PREDICATE,object_cui_primary FROM nn GROUP BY subject_cui_primary,PREDICATE,object_cui_primary HAVING COUNT(DISTINCT future_focal_new_node_new_combination_seen)>1 OR COUNT(DISTINCT future_focal_new_node_new_relation_seen)>1)', 'Inconsistent exact-triple development')
    # Full source coverage was verified before LEFT JOIN: absence of a category
    # is zero; a missing expected development row is an error, not a zero.
    keys = ','.join(KEY)
    con.execute(f'''CREATE TEMP TABLE source AS SELECT A.PMID, CAST(A.PYEAR AS INTEGER) PYEAR,
        A.edge_annotation category, T.future_exact_triple_seen adopted,
        COALESCE(N.future_focal_new_node_new_combination_seen,FALSE) nn_nc,
        COALESCE(N.future_focal_new_node_new_relation_seen,FALSE) nn_nr,
        COALESCE(C.future_focal_pair_new_relation_seen,FALSE) nc_nr,
        least(A.subject_cui_primary,A.object_cui_primary) node_a,
        greatest(A.subject_cui_primary,A.object_cui_primary) node_b
        FROM annotation A JOIN transition T USING ({keys})
        LEFT JOIN nn N USING ({keys}) LEFT JOIN nc C USING ({keys})''')
    return scalar(con, 'SELECT COUNT(*) FROM source')


def aggregate(con):
    nn, nc = "category='New_Node'", "category='New_Combination'"
    expressions = {'n_new_node': nn, 'n_new_combination': nc,
        'n_nn_future_nc': f'{nn} AND nn_nc', 'n_nn_future_nr': f'{nn} AND nn_nr',
        'n_nn_future_any': f'{nn} AND (nn_nc OR nn_nr)', 'n_nc_future_nr': f'{nc} AND nc_nr',
        'n_nn_dev_neither': f'{nn} AND NOT nn_nc AND NOT nn_nr',
        'n_nn_dev_nc_only': f'{nn} AND nn_nc AND NOT nn_nr',
        'n_nn_dev_nr_only': f'{nn} AND NOT nn_nc AND nn_nr',
        'n_nn_dev_both': f'{nn} AND nn_nc AND nn_nr'}
    for category, selection, developed in [('nn',nn,'(nn_nc OR nn_nr)'),('nc',nc,'nc_nr')]:
        for state, condition in [('neither_adopted_nor_developed',f'NOT adopted AND NOT {developed}'),('adopted_only',f'adopted AND NOT {developed}'),('developed_only',f'NOT adopted AND {developed}'),('adopted_and_developed',f'adopted AND {developed}')]:
            expressions[f'n_{category}_{state}'] = f'{selection} AND {condition}'
    sql = ','.join(f'COUNT(*) FILTER (WHERE {condition})::BIGINT AS {name}' for name,condition in expressions.items())
    con.execute(f'CREATE TEMP TABLE triple_counts AS SELECT PMID,PYEAR,{sql} FROM source GROUP BY PMID,PYEAR')
    con.execute("CREATE TEMP TABLE pair_counts AS SELECT PMID,COUNT(*)::BIGINT n_unique_new_combination_pairs,COUNT(*) FILTER (WHERE nc_nr)::BIGINT n_nc_pairs_future_nr FROM (SELECT DISTINCT PMID,node_a,node_b,nc_nr FROM source WHERE category='New_Combination') GROUP BY PMID")
    con.execute('CREATE TEMP VIEW counts AS SELECT T.*, COALESCE(P.n_unique_new_combination_pairs,0)::BIGINT n_unique_new_combination_pairs,COALESCE(P.n_nc_pairs_future_nr,0)::BIGINT n_nc_pairs_future_nr FROM triple_counts T LEFT JOIN pair_counts P USING (PMID)')
    derived = {s:f'{n}::DOUBLE/NULLIF({d},0) AS {s}' for s,n,d in RATIOS}
    derived.update({f:f'CAST({n}>0 AS INTEGER) AS {f}' for f,n in FLAGS})
    con.execute('CREATE TEMP VIEW expected AS SELECT '+','.join(derived.get(c,c) for c in COLUMNS)+' FROM counts')


def validate(con, year):
    if [(r[0],r[1]) for r in con.execute('DESCRIBE metrics').fetchall()] != list(zip(COLUMNS,TYPES)):
        raise ValueError('Incorrect 44-column schema')
    conditions = ["PMID IS NULL OR trim(PMID)=''",f'PYEAR IS NULL OR PYEAR<>{year}']
    conditions += [f'{c} IS NULL OR {c}<0' for c in COUNTS]
    conditions += ['n_new_node<>'+ '+'.join('n_'+x for x in DEV)]
    for category,denominator,development in [('nn','n_new_node','n_nn_future_any'),('nc','n_new_combination','n_nc_future_nr')]:
        joint = [x for x in JOINT if x.startswith(category+'_')]
        conditions += [denominator+'<>'+ '+'.join('n_'+x for x in joint),f'{development}<>n_{category}_developed_only+n_{category}_adopted_and_developed']
    conditions += ['n_nn_future_nc<>n_nn_dev_nc_only+n_nn_dev_both', 'n_nn_future_nr<>n_nn_dev_nr_only+n_nn_dev_both', 'n_nn_future_any<>n_nn_dev_nc_only+n_nn_dev_nr_only+n_nn_dev_both', 'n_unique_new_combination_pairs>n_new_combination', 'n_nc_pairs_future_nr>n_nc_future_nr', 'has_nc_pairs_future_nr<>has_nc_future_nr']
    for s,n,d in RATIOS:
        conditions += [f'{n}>{d}',f'({d}=0 AND {s} IS NOT NULL)',f'({d}>0 AND ({s} IS NULL OR NOT isfinite({s}) OR {s}<0 OR {s}>1 OR abs({s}-{n}::DOUBLE/{d})>1e-9))']
    conditions += [f'{f} IS NULL OR {f}<>CAST({n}>0 AS INTEGER)' for f,n in FLAGS]
    reject(con,'SELECT COUNT(*) FROM metrics WHERE '+' OR '.join(conditions),'Invalid counts, proportions or flags')
    reject(con,'SELECT COUNT(*)-COUNT(DISTINCT PMID) FROM metrics','Duplicate PMIDs')


def compare_source(con):
    conditions = ['a.PMID IS NULL','b.PMID IS NULL','a.PYEAR IS DISTINCT FROM b.PYEAR']
    conditions += [f'a.{c} IS DISTINCT FROM b.{c}' for c in COUNTS+[f for f,n in FLAGS]]
    conditions += [f'(a.{s} IS NULL)<>(b.{s} IS NULL) OR abs(a.{s}-b.{s})>1e-9' for s,n,d in RATIOS]
    reject(con,'SELECT COUNT(*) FROM metrics a FULL OUTER JOIN expected b ON a.PMID=b.PMID WHERE '+' OR '.join(conditions),'Output differs from source aggregates')


def summarize(con,year,n_input):
    columns = ['COUNT(*) n_pmids', 'COUNT(*) FILTER (WHERE n_new_node>0) n_pmids_with_new_node','COUNT(*) FILTER (WHERE n_new_combination>0) n_pmids_with_new_combination']
    columns += [f'COUNT(*) FILTER (WHERE {f}=1) n_pmids_{f}' for f,n in FLAGS]
    columns += [f'COALESCE(SUM({c}),0) {c}' for c in COUNTS]
    cur = con.execute('SELECT '+','.join(columns)+' FROM metrics')
    row = {'PYEAR':year,'n_input_triples':n_input,**dict(zip([d[0] for d in cur.description],cur.fetchone()))}
    if row['n_pmids'] != scalar(con,'SELECT COUNT(DISTINCT PMID) FROM source'):
        raise ValueError('PMID coverage mismatch')
    return row


def build(root,year):
    target,summary,validation = output_paths(root,year)
    guard([target,summary,validation])
    report = {'PYEAR':year,'n_input_triples':'','n_pmids':'','source_valid':False,'metrics_valid':False,'source_output_match':False,'all_checks_passed':False,'error':''}
    try:
        with tempfile.TemporaryDirectory(prefix='development_metrics_',dir=root/SUBDIR) as temp:
            con=connect(temp)
            try:
                report['n_input_triples']=prepare_source(con,root,year); report['source_valid']=True
                aggregate(con)
                con.execute('CREATE TEMP VIEW metrics AS SELECT * FROM expected')
                validate(con,year); report['metrics_valid']=True
                compare_source(con); report['source_output_match']=True
                row=summarize(con,year,report['n_input_triples']); report['n_pmids']=row['n_pmids']
                staged=Path(temp)/'output.parquet'
                con.execute(f'COPY (SELECT * FROM metrics ORDER BY PMID) TO {q(staged)} (FORMAT PARQUET,COMPRESSION ZSTD)')
                write_csv(summary,[row]); staged.replace(target)
                report['all_checks_passed']=True
            finally:
                con.close()
    except Exception as exc:
        report['error']=str(exc)
        raise
    finally:
        write_csv(validation,[report])
    print(f'Saved {target}; {report}')


if __name__=='__main__':
    task=os.getenv('SLURM_ARRAY_TASK_ID')
    year=1980+int(task) if task is not None else int(os.environ['IC_FOCAL_YEAR'])
    if not 1980<=year<=2018:
        raise ValueError('Year outside 1980-2018')
    build(Path(os.getenv('IC_PROJECT_ROOT',str(ROOT))),year)
