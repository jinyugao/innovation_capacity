"""Compute concept, undirected pair and directed typed triple metrics at two levels."""
from __future__ import annotations
import csv
import os
from pathlib import Path
import tempfile
import duckdb

ROOT = Path('/xdisk/sebratt/jinyugao/research/projects/innovation_capacity')
SUBDIR = Path('data/processed/knowledge_combination/edge_annotation_new_knowledge_metrics')
STEM = 'edge_annotation_new_knowledge_metrics'
CATEGORIES = ['New_Node', 'New_Combination', 'New_Relation', 'Repeated_Triple']
COUNTS = ['n_new_node', 'n_new_combination', 'n_new_relation', 'n_repeated_triple']
TRIPLE_COUNTS = ['n_triples', *COUNTS, 'n_new_triples']
TRIPLE_SHARES = ['share_new_node', 'share_new_combination', 'share_new_relation', 'share_repeated_triple', 'share_new_triples']
EXTRA = ['n_unique_concepts', 'n_unique_new_concepts', 'share_unique_new_concepts', 'n_unique_concept_pairs', 'n_unique_new_combination_pairs', 'share_unique_new_combination_pairs']
METRICS = EXTRA + TRIPLE_COUNTS + TRIPLE_SHARES
ALL_COUNTS = [c for c in METRICS if c.startswith('n_')]
RATIOS = [('share_unique_new_concepts','n_unique_new_concepts','n_unique_concepts'), ('share_unique_new_combination_pairs','n_unique_new_combination_pairs','n_unique_concept_pairs')] + [(s,n,'n_triples') for s,n in zip(TRIPLE_SHARES,COUNTS+['n_new_triples'])]


def q(value):
    return "'" + str(value).replace("'", "''") + "'"


def scalar(con, query):
    return con.execute(query).fetchone()[0]


def connect(temp):
    con=duckdb.connect()
    con.execute(f'SET temp_directory={q(temp)}')
    con.execute(f"SET threads={int(os.getenv('IC_DUCKDB_THREADS','4'))}")
    con.execute(f"SET memory_limit={q(os.getenv('IC_DUCKDB_MEMORY_LIMIT','24GB'))}")
    return con


def guard(paths):
    overwrite=os.getenv('IC_NEW_KNOWLEDGE_METRICS_OVERWRITE','0').lower() in ('1','true','yes')
    for path in paths:
        if path.exists() and not overwrite:
            raise FileExistsError(f'{path}; set IC_NEW_KNOWLEDGE_METRICS_OVERWRITE=1 to replace')
        path.parent.mkdir(parents=True,exist_ok=True)


def write_csv(path, rows):
    path.parent.mkdir(parents=True,exist_ok=True)
    temp=path.with_suffix('.csv.tmp')
    with temp.open('w',newline='',encoding='utf-8') as handle:
        writer=csv.DictWriter(handle,fieldnames=list(rows[0]))
        writer.writeheader(); writer.writerows(rows)
    temp.replace(path)


def paths(root,year):
    base=root/SUBDIR
    return (base/'pmid'/f'pmid_{STEM}_{year}.parquet', base/'year'/f'year_{STEM}_{year}.csv', base/'validation'/f'{STEM}_validation_{year}.csv')


def require_zero(con, query, name, report):
    count=scalar(con,query)
    report[name]=count
    if count:
        raise ValueError(f'{name}={count}')


def prepare(con,root,year,report):
    source=root/'data/processed/knowledge_combination/edge_annotation'/f'edge_annotation_{year}.parquet'
    if not source.is_file():
        raise FileNotFoundError(source)
    con.execute(f'CREATE TEMP VIEW raw AS SELECT * FROM read_parquet({q(source)})')
    schema=dict((r[0],r[1]) for r in con.execute('DESCRIBE raw').fetchall())
    flags=['subject_seen_in_prior_window','object_seen_in_prior_window','pair_connected_in_prior_window','exact_edge_seen_in_prior_window']
    if any(schema.get(c)!='BOOLEAN' for c in flags):
        raise ValueError('Expected Boolean prior-window flags in annotation; regenerate upstream annotation if missing')
    required=['PMID','subject_cui_primary','object_cui_primary','PREDICATE']
    condition=' OR '.join(f'{c} IS NULL OR trim(CAST({c} AS VARCHAR))=\'\'' for c in required)
    condition+=f' OR PYEAR IS NULL OR PYEAR<>{year} OR subject_cui_primary=object_cui_primary OR '+ ' OR '.join(f'{c} IS NULL' for c in flags)
    condition+=' OR edge_annotation IS NULL OR edge_annotation NOT IN ('+','.join(map(q,CATEGORIES))+')'
    require_zero(con,f'SELECT COUNT(*) FROM raw WHERE {condition}','invalid_input_rows',report)
    con.execute('CREATE TEMP TABLE triples AS SELECT CAST(PMID AS VARCHAR) PMID, subject_cui_primary s, PREDICATE p, object_cui_primary o, edge_annotation category, NOT subject_seen_in_prior_window sn, NOT object_seen_in_prior_window onew, pair_connected_in_prior_window pc, exact_edge_seen_in_prior_window ec FROM raw')
    require_zero(con,'SELECT COALESCE(SUM(n-1),0) FROM (SELECT COUNT(*) n FROM triples GROUP BY PMID,s,p,o HAVING COUNT(*)>1)','duplicate_input_triples',report)
    expected="CASE WHEN sn OR onew THEN 'New_Node' WHEN NOT pc THEN 'New_Combination' WHEN NOT ec THEN 'New_Relation' ELSE 'Repeated_Triple' END"
    require_zero(con,f'SELECT COUNT(*) FROM triples WHERE category<>{expected} OR (pc AND (sn OR onew)) OR (ec AND NOT pc)','annotation_flag_mismatches',report)
    con.execute('CREATE TEMP TABLE concept_occurrences AS SELECT PMID,s concept,sn is_new FROM triples UNION SELECT PMID,o concept,onew is_new FROM triples')
    require_zero(con,'SELECT COUNT(*) FROM (SELECT concept FROM concept_occurrences GROUP BY concept HAVING COUNT(DISTINCT is_new)>1)','inconsistent_concepts',report)
    con.execute("CREATE TEMP TABLE pairs AS SELECT DISTINCT PMID,least(s,o) a,greatest(s,o) b,category='New_Combination' is_new FROM triples")
    require_zero(con,'SELECT COUNT(*) FROM (SELECT a,b FROM pairs GROUP BY a,b HAVING COUNT(DISTINCT is_new)>1)','inconsistent_pairs',report)
    require_zero(con,'SELECT COUNT(*) FROM (SELECT s,p,o FROM triples GROUP BY s,p,o HAVING COUNT(DISTINCT category)>1)','inconsistent_triples',report)
    con.execute('CREATE TEMP TABLE annual_concepts AS SELECT DISTINCT concept,is_new FROM concept_occurrences')
    con.execute('CREATE TEMP TABLE annual_pairs AS SELECT DISTINCT a,b,is_new FROM pairs')
    con.execute('CREATE TEMP TABLE annual_triples AS SELECT DISTINCT s,p,o,category FROM triples')
    report['n_input_triples']=scalar(con,'SELECT COUNT(*) FROM triples')


def aggregate(con,year):
    for scope in ['pmid','annual']:
        grouped=scope=='pmid'
        prefix='PMID, ' if grouped else ''
        group=' GROUP BY PMID' if grouped else ''
        ctable='concept_occurrences' if grouped else 'annual_concepts'
        ptable='pairs' if grouped else 'annual_pairs'
        ttable='triples' if grouped else 'annual_triples'
        con.execute(f'CREATE TEMP TABLE {scope}_concept_counts AS SELECT {prefix}COUNT(*)::BIGINT n_unique_concepts,COUNT(*) FILTER (WHERE is_new)::BIGINT n_unique_new_concepts FROM {ctable}{group}')
        con.execute(f'CREATE TEMP TABLE {scope}_pair_counts AS SELECT {prefix}COUNT(*)::BIGINT n_unique_concept_pairs,COUNT(*) FILTER (WHERE is_new)::BIGINT n_unique_new_combination_pairs FROM {ptable}{group}')
        category_counts=', '.join(f'COUNT(*) FILTER (WHERE category={q(cat)})::BIGINT {name}' for cat,name in zip(CATEGORIES,COUNTS))
        con.execute(f'CREATE TEMP TABLE {scope}_triple_counts AS SELECT {prefix}COUNT(*)::BIGINT n_triples,{category_counts},COUNT(*) FILTER (WHERE category<>\'Repeated_Triple\')::BIGINT n_new_triples FROM {ttable}{group}')
        joins=' JOIN ' if grouped else ' CROSS JOIN '
        using=' USING (PMID)' if grouped else ''
        query=f'SELECT * FROM {scope}_concept_counts{joins}{scope}_pair_counts{using}{joins}{scope}_triple_counts{using}'
        ratios={s:f'{n}::DOUBLE / NULLIF({d},0) AS {s}' for s,n,d in RATIOS}
        select=(['PMID'] if grouped else []) + [f'{year}::INTEGER PYEAR'] + [ratios.get(m,m) for m in METRICS]
        con.execute(f'CREATE TEMP VIEW {scope}_metrics AS SELECT '+','.join(select)+f' FROM ({query})')


def validate_metrics(con,table,year,pmid):
    expected=(['PMID'] if pmid else [])+['PYEAR']+METRICS
    schema=con.execute(f'DESCRIBE {table}').fetchall()
    expected_types=(['VARCHAR'] if pmid else [])+['INTEGER']+['BIGINT' if c.startswith('n_') else 'DOUBLE' for c in METRICS]
    if [(r[0],r[1]) for r in schema]!=list(zip(expected,expected_types)):
        raise ValueError(f'{table}: incorrect schema')
    condition=f'PYEAR IS NULL OR PYEAR<>{year} OR '+' OR '.join(f'{c} IS NULL OR {c}<0' for c in ALL_COUNTS)
    condition+=' OR n_triples<>'+ '+'.join(COUNTS)+' OR n_new_triples<>'+ '+'.join(COUNTS[:3])
    condition+=' OR n_unique_new_concepts>n_unique_concepts OR n_unique_new_combination_pairs>n_unique_concept_pairs OR n_unique_new_combination_pairs>n_new_combination OR n_unique_concept_pairs>n_triples'
    if pmid:
        condition+=" OR PMID IS NULL OR trim(PMID)='' OR n_triples=0 OR n_unique_concepts=0 OR n_unique_concept_pairs=0"
    for s,n,d in RATIOS:
        condition+=f' OR ({d}=0 AND {s} IS NOT NULL) OR ({d}>0 AND ({s} IS NULL OR NOT isfinite({s}) OR {s}<0 OR {s}>1 OR abs({s}-{n}::DOUBLE/{d})>1e-9))'
    condition+=' OR (n_triples>0 AND abs('+ '+'.join(TRIPLE_SHARES[:4])+'-1)>1e-9)'
    if scalar(con,f'SELECT COUNT(*) FROM {table} WHERE {condition}'):
        raise ValueError(f'{table}: count or ratio validation failed')
    if pmid and scalar(con,f'SELECT COUNT(*)-COUNT(DISTINCT PMID) FROM {table}'):
        raise ValueError('Duplicate PMIDs')


def compare_old(con,root,year):
    old=root/'data/processed/knowledge_combination/edge_annotation_metrics'/f'pmid_edge_annotation_metrics_{year}.parquet'
    if not old.is_file():
        raise FileNotFoundError(old)
    con.execute(f'CREATE OR REPLACE TEMP VIEW old_metrics AS SELECT * FROM read_parquet({q(old)})')
    if scalar(con,'SELECT COUNT(*)-COUNT(DISTINCT PMID) FROM old_metrics'):
        raise ValueError('Old metrics contain duplicate/null PMIDs')
    conditions=['a.PMID IS NULL','b.PMID IS NULL','a.PYEAR IS DISTINCT FROM b.PYEAR']
    conditions += [f'a.{c} IS DISTINCT FROM b.{c}' for c in TRIPLE_COUNTS]
    conditions += [f'b.{s} IS NULL OR NOT isfinite(b.{s}) OR abs(a.{s}-b.{s})>1e-9' for s in TRIPLE_SHARES]
    if scalar(con,'SELECT COUNT(*) FROM pmid_metrics a FULL OUTER JOIN old_metrics b ON a.PMID=CAST(b.PMID AS VARCHAR) WHERE '+' OR '.join(conditions)):
        raise ValueError('PMID metrics differ from previous metrics')


def csv_row(con, table):
    cursor=con.execute(f'SELECT * FROM {table}')
    return dict(zip([d[0] for d in cursor.description],cursor.fetchone()))


def build(root,year):
    target,annual,validation=paths(root,year)
    guard([target,annual,validation])
    report={'PYEAR':year,'invalid_input_rows':'','duplicate_input_triples':'','annotation_flag_mismatches':'','inconsistent_concepts':'','inconsistent_pairs':'','inconsistent_triples':'','n_input_triples':'','n_pmids':'','pmid_sets_match':False,'old_metrics_match':False,'all_checks_passed':False,'error':''}
    try:
        with tempfile.TemporaryDirectory(prefix='new_knowledge_',dir=root/SUBDIR) as temp:
            con=connect(temp)
            try:
                prepare(con,root,year,report); aggregate(con,year)
                validate_metrics(con,'pmid_metrics',year,True)
                validate_metrics(con,'annual_metrics',year,False)
                if scalar(con,'SELECT COUNT(*) FROM ((SELECT DISTINCT PMID FROM triples EXCEPT SELECT PMID FROM pmid_metrics) UNION ALL (SELECT PMID FROM pmid_metrics EXCEPT SELECT DISTINCT PMID FROM triples))'):
                    raise ValueError('PMID sets differ')
                report['pmid_sets_match']=True
                compare_old(con,root,year); report['old_metrics_match']=True
                report['n_pmids']=scalar(con,'SELECT COUNT(*) FROM pmid_metrics')
                staged=Path(temp)/'pmid.parquet'
                con.execute(f'COPY (SELECT * FROM pmid_metrics ORDER BY PMID) TO {q(staged)} (FORMAT PARQUET, COMPRESSION ZSTD)')
                write_csv(annual,[csv_row(con,'annual_metrics')])
                staged.replace(target)
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
    if not 1980<=year<=2023:
        raise ValueError('Year outside 1980-2023')
    build(Path(os.getenv('IC_PROJECT_ROOT',str(ROOT))),year)
