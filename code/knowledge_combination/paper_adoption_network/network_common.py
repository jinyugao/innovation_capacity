"""Shared IO and validation for the PMID knowledge-adoption network."""
import json
import os
from pathlib import Path
import duckdb

ROOT=Path(os.getenv('IC_PROJECT_ROOT','/xdisk/sebratt/jinyugao/research/projects/innovation_capacity'))
BASE=Path('data/processed/knowledge_combination')
OUT=BASE/'paper_adoption_network'
KEY=['subject_cui_primary','PREDICATE','object_cui_primary']
NEW=['New_Node','New_Combination','New_Relation']
KINDS=['new_node','new_combination','new_relation']
ALL=NEW+['Repeated_Triple']

def q(x):return "'"+str(x).replace("'","''")+"'"
def scalar(c,s):return c.execute(s).fetchone()[0]
def require(c,s,message):
    if scalar(c,s):raise ValueError(message)
def connection(folder):
    c=duckdb.connect(str(Path(folder)/'work.duckdb'))
    c.execute(f"SET threads={int(os.getenv('IC_DUCKDB_THREADS','4'))}")
    c.execute(f"SET memory_limit={q(os.getenv('IC_DUCKDB_MEMORY_LIMIT','48GB'))}")
    c.execute(f"SET temp_directory={q(Path(folder)/'spill')}")
    c.execute('SET preserve_insertion_order=false')
    return c

def annotation(root,y):return root/BASE/'edge_annotation'/f'edge_annotation_{y}.parquet'
def transition(root,y):return root/BASE/'edge_transition'/f'edge_transition_{y}.parquet'
def metric(root,y):return root/BASE/'edge_adoption_metrics/pmid'/f'pmid_edge_adoption_metrics_{y}.parquet'
def annual(root,y):
    b=root/OUT
    return {'edges':b/'edges'/f'pmid_adoption_network_edges_{y}.parquet',
            'trace':b/'triple_pmid'/f'pmid_adoption_network_triple_pmid_{y}.parquet',
            'nodes':b/'nodes'/f'pmid_adoption_network_focal_nodes_{y}.parquet',
            'summary':b/'summary'/f'pmid_adoption_network_summary_{y}.json',
            'validation':b/'validation'/f'pmid_adoption_network_validation_{y}.json'}

def signature(files):
    return {str(f):{'bytes':f.stat().st_size,'mtime_ns':f.stat().st_mtime_ns} for f in files}
def guard(files,overwrite):
    for f in files:
        if f.exists() and not overwrite:raise FileExistsError(f'{f}; use --overwrite to replace')
def publish(stage,paths):
    for k,f in paths.items():
        f.parent.mkdir(parents=True,exist_ok=True)
        (stage/f.name).replace(f)
def write_json(p,data):p.write_text(json.dumps(data,indent=2))
def parquet(c,query,path):c.execute(f'COPY ({query}) TO {q(path)} (FORMAT PARQUET,COMPRESSION ZSTD)')
def csv(c,query,path):c.execute(f'COPY ({query}) TO {q(path)} (FORMAT CSV,HEADER TRUE)')

def load(c,table,path,y,status=False):
    if not path.is_file():raise FileNotFoundError(path)
    schema=dict((r[0],r[1]) for r in c.execute(f'DESCRIBE SELECT * FROM read_parquet({q(path)})').fetchall())
    needed=['PMID','PYEAR',*KEY,'edge_annotation']+(['future_five_year_transition','future_exact_triple_seen'] if status else [])
    missing=set(needed)-schema.keys()
    if missing:raise ValueError(f'{path}: missing {missing}')
    if status and schema['future_exact_triple_seen']!='BOOLEAN':raise ValueError('future_exact_triple_seen must be Boolean')
    # Do not materialize names, semantic types or historical flags unused here.
    c.execute(f'CREATE TABLE {table} AS SELECT {",".join(needed)} FROM read_parquet({q(path)})')
    bad=" OR ".join(["PMID IS NULL","NOT regexp_full_match(CAST(PMID AS VARCHAR),'[1-9][0-9]*')",f"PYEAR IS NULL OR NOT regexp_full_match(CAST(PYEAR AS VARCHAR),'{y}')",*[f'{k} IS NULL OR trim({k})=\'\'' for k in KEY],"subject_cui_primary=object_cui_primary",'edge_annotation IS NULL',f"edge_annotation NOT IN ({','.join(map(q,ALL))})"])
    require(c,f'SELECT count(*) FROM {table} WHERE {bad}',f'{path}: invalid rows/year/categories')
    keys=','.join(['PMID',*KEY])
    require(c,f'SELECT count(*) FROM (SELECT {keys} FROM {table} GROUP BY ALL HAVING count(*)>1)',f'{path}: duplicate PMID triples')
    require(c,f"SELECT count(*) FROM (SELECT {','.join(KEY)} FROM {table} GROUP BY ALL HAVING count(DISTINCT edge_annotation)>1)",f'{path}: inconsistent triple categories within year')
    if status:
        require(c,f'''SELECT count(*) FROM {table} WHERE future_exact_triple_seen IS NULL OR future_five_year_transition IS NULL
            OR future_five_year_transition IS DISTINCT FROM CASE WHEN future_exact_triple_seen THEN
            CASE WHEN edge_annotation='Repeated_Triple' THEN 'Continued' ELSE 'Adopted' END ELSE 'Disappeared' END''',f'{path}: inconsistent transition flags')
    c.execute(f'ALTER TABLE {table} ALTER PMID TYPE VARCHAR')
    c.execute(f'ALTER TABLE {table} ALTER PYEAR TYPE INTEGER')
