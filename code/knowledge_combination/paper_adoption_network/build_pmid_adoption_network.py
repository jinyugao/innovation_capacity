"""Build one focal-year network: adopter PMID -> focal PMID, five-year window."""
import argparse
import os
from pathlib import Path
import tempfile
from network_common import *


def build(root,y,buckets=64,overwrite=False,profile_only=False):
    root=Path(root)
    if not 1980<=y<=2018 or buckets<1:raise ValueError('year must be 1980-2018; buckets must be positive')
    files=[transition(root,y),annotation(root,y),*[annotation(root,t) for t in range(y+1,y+6)],metric(root,y)]
    for f in files:
        if not f.is_file():raise FileNotFoundError(f)
    paths=annual(root,y)
    profile=root/OUT/'summary'/f'pmid_adoption_network_profile_{y}.json'
    guard([profile] if profile_only else list(paths.values()),overwrite)
    parent=root/OUT;parent.mkdir(parents=True,exist_ok=True)
    before=signature(files)
    with tempfile.TemporaryDirectory(prefix=f'build_{y}_',dir=parent) as temp:
        stage=Path(temp)/'outputs';stage.mkdir()
        with connection(temp) as c:
            print(f'{y}: validating focal annotation and transition',flush=True)
            load(c,'focal',transition(root,y),y,True)
            load(c,'focal_annotation',annotation(root,y),y)
            cols=','.join(['PMID','PYEAR',*KEY,'edge_annotation'])
            require(c,f'SELECT count(*) FROM ((SELECT {cols} FROM focal EXCEPT SELECT {cols} FROM focal_annotation) UNION ALL (SELECT {cols} FROM focal_annotation EXCEPT SELECT {cols} FROM focal))','Focal annotation/transition mismatch')
            c.execute(f'''CREATE TABLE focal_new AS SELECT PMID,PYEAR,{','.join(KEY)},edge_annotation,
                future_exact_triple_seen,hash(PMID)%{buckets} audit_bucket FROM focal
                WHERE edge_annotation IN ({','.join(map(q,NEW))}) ORDER BY audit_bucket''')
            c.execute(f'''CREATE TABLE triple_counts AS SELECT {','.join(KEY)},edge_annotation,
                count(*) n_focal_pmids FROM focal_new GROUP BY ALL''')
            c.execute('DROP TABLE focal_annotation')
            for t in range(y+1,y+6):
                print(f'{y}: validating future year {t}',flush=True)
                load(c,'future_year',annotation(root,t),t)
                prefix='CREATE TABLE future_all AS' if t==y+1 else 'INSERT INTO future_all'
                c.execute(f'{prefix} SELECT PMID,PYEAR,{",".join(KEY)} FROM future_year')
                c.execute('DROP TABLE future_year')
            require(c,'SELECT count(*) FROM (SELECT PMID FROM future_all GROUP BY PMID HAVING count(DISTINCT PYEAR)>1)','Future PMID has multiple years')
            require(c,'SELECT count(*) FROM (SELECT PMID FROM focal INTERSECT SELECT PMID FROM future_all)','PMID occurs in focal and future years')
            keys=','.join(KEY)
            c.execute(f'''CREATE TABLE matched_future AS SELECT u.*,t.edge_annotation,t.n_focal_pmids
                FROM future_all u JOIN triple_counts t USING({keys})''')
            c.execute(f'''CREATE TABLE future_counts AS SELECT {keys},count(*) n_future_pmids FROM matched_future GROUP BY ALL''')
            c.execute('DROP TABLE future_all')
            c.execute(f'''CREATE TABLE matched_focal AS SELECT f.*,coalesce(t.n_future_pmids,0) n_future_pmids
                FROM focal_new f LEFT JOIN future_counts t USING({keys})''')
            require(c,'SELECT count(*) FROM matched_focal WHERE future_exact_triple_seen IS DISTINCT FROM (n_future_pmids>0)','Reconstructed five-year adoption disagrees with transition')
            c.execute(f'''CREATE TABLE reconstructed AS SELECT f.PMID,{y}::INTEGER PYEAR,
                count(*)::BIGINT n_triples,
                {','.join(f"count(*) FILTER(WHERE f.edge_annotation={q(cat)})::BIGINT n_{kind}" for cat,kind in zip(NEW,KINDS))},
                count(*) FILTER(WHERE f.edge_annotation<>'Repeated_Triple')::BIGINT n_new_triples,
                {','.join(f"count(*) FILTER(WHERE f.edge_annotation={q(cat)} AND m.n_future_pmids>0)::BIGINT n_{kind}_adopted" for cat,kind in zip(NEW,KINDS))},
                count(*) FILTER(WHERE m.n_future_pmids>0)::BIGINT n_new_triples_adopted
                FROM focal f LEFT JOIN matched_focal m USING(PMID,{keys}) GROUP BY f.PMID''')
            c.execute(f'CREATE TABLE previous AS SELECT * FROM read_parquet({q(metric(root,y))})')
            require(c,'SELECT count(*) FROM (SELECT PMID FROM previous GROUP BY PMID HAVING count(*)>1)','Duplicate PMID in previous metrics')
            rcols=[r[0] for r in c.execute('DESCRIBE reconstructed').fetchall()]
            comparison=' OR '.join(['r.PMID IS NULL','p.PMID IS NULL',*[f'r.{k} IS DISTINCT FROM p.{k}' for k in rcols if k!='PMID'],"p.has_new_triples_adopted IS DISTINCT FROM CAST(r.n_new_triples_adopted>0 AS INTEGER)"])
            require(c,f'SELECT count(*) FROM reconstructed r FULL JOIN previous p USING(PMID) WHERE {comparison}','Existing PMID adoption metrics disagree with reconstructed counts')
            estimated=scalar(c,'SELECT coalesce(sum(n_focal_pmids::HUGEINT*n_future_pmids::HUGEINT),0) FROM triple_counts JOIN future_counts USING('+keys+')')
            summary={'focal_year':y,'future_years':list(range(y+1,y+6)),
                'n_focal_pmids':scalar(c,'SELECT count(*) FROM reconstructed'),
                'n_new_triple_pmid_rows':scalar(c,'SELECT count(*) FROM focal_new'),
                'n_unique_new_triples':scalar(c,'SELECT count(*) FROM triple_counts'),
                'n_matched_future_triple_pmid_rows':scalar(c,'SELECT count(*) FROM matched_future'),
                'n_matched_future_pmids':scalar(c,'SELECT count(DISTINCT PMID) FROM matched_future'),
                'n_triple_paper_pair_occurrences':estimated,
                'max_focal_pmids_per_triple':scalar(c,'SELECT coalesce(max(n_focal_pmids),0) FROM triple_counts'),
                'max_future_pmids_per_triple':scalar(c,'SELECT coalesce(max(n_future_pmids),0) FROM future_counts'),
                'aggregation_buckets':buckets,'input_signature':before}
            if profile_only:
                summary['all_checks_passed']=True
                if signature(files)!=before:raise ValueError('Inputs changed during profiling')
                write_json(stage/profile.name,summary)
            else:
                print(f'{y}: {estimated:,} triple-paper-pair occurrences; aggregating {buckets} focal-PMID buckets',flush=True)
                sums=','.join(f"count(*) FILTER(WHERE f.edge_annotation={q(cat)})::BIGINT n_shared_{kind}_triples" for cat,kind in zip(NEW,KINDS))
                for b in range(buckets):
                    prefix='CREATE TABLE edges AS' if b==0 else 'INSERT INTO edges'
                    c.execute(f'''{prefix} SELECT u.PMID adopter_pmid,f.PMID focal_pmid,u.PYEAR::INTEGER adopter_year,
                        {y}::INTEGER focal_year,(u.PYEAR-{y})::INTEGER year_gap,{sums},count(*)::BIGINT n_shared_new_triples
                        FROM focal_new f JOIN matched_future u USING({keys}) WHERE f.audit_bucket={b}
                        GROUP BY u.PMID,f.PMID,u.PYEAR''')
                require(c,'SELECT count(*) FROM (SELECT adopter_pmid,focal_pmid FROM edges GROUP BY ALL HAVING count(*)>1)','Duplicate annual directed PMID pair')
                require(c,'SELECT count(*) FROM edges WHERE year_gap NOT BETWEEN 1 AND 5 OR adopter_pmid=focal_pmid OR n_shared_new_triples<=0 OR n_shared_new_triples<>n_shared_new_node_triples+n_shared_new_combination_triples+n_shared_new_relation_triples','Invalid edge')
                if scalar(c,'SELECT coalesce(sum(n_shared_new_triples),0) FROM edges')!=estimated:raise ValueError('Edge weight total mismatch')
                c.execute('''CREATE TABLE incoming AS SELECT focal_pmid PMID,count(*)::BIGINT n_future_papers,
                    sum(n_shared_new_triples)::BIGINT n_shared_new_triple_occurrences FROM edges GROUP BY focal_pmid''')
                c.execute('''CREATE TABLE focal_nodes AS SELECT r.*,coalesce(i.n_future_papers,0)::BIGINT n_future_papers,
                    coalesce(i.n_shared_new_triple_occurrences,0)::BIGINT n_shared_new_triple_occurrences,
                    CAST(r.n_new_triples_adopted>0 AS INTEGER) has_new_triples_adopted FROM reconstructed r LEFT JOIN incoming i USING(PMID)''')
                require(c,'SELECT count(*) FROM focal_nodes WHERE (n_future_papers>0) IS DISTINCT FROM (n_new_triples_adopted>0)','In-degree does not match adoption indicator')
                c.execute(f'''CREATE TABLE trace AS SELECT {y}::INTEGER focal_year,'focal_new' AS role,f.PMID,f.PYEAR::INTEGER PYEAR,{','.join('f.'+k for k in KEY)},f.edge_annotation focal_edge_annotation,t.n_focal_pmids
                    FROM focal_new f JOIN triple_counts t USING({keys}) UNION ALL
                    SELECT {y}::INTEGER,'future_matched',PMID,PYEAR::INTEGER,{keys},edge_annotation,n_focal_pmids FROM matched_future''')
                for k,table in [('edges','edges'),('trace','trace'),('nodes','focal_nodes')]:parquet(c,f'SELECT * FROM {table}',stage/paths[k].name)
                summary['n_edges']=scalar(c,'SELECT count(*) FROM edges')
                summary['n_focal_pmids_with_incoming_edges']=scalar(c,'SELECT count(*) FROM focal_nodes WHERE n_future_papers>0')
                if signature(files)!=before:raise ValueError('Inputs changed during build')
                write_json(stage/paths['summary'].name,summary)
                write_json(stage/paths['validation'].name,{'focal_year':y,'all_checks_passed':True,'input_signature':before,
                    'outputs':{k:{'bytes':(stage/paths[k].name).stat().st_size,'mtime_ns':(stage/paths[k].name).stat().st_mtime_ns} for k in ['edges','trace','nodes','summary']},
                    'n_edges':summary['n_edges'],'n_focal_pmids':summary['n_focal_pmids'],'duckdb_version':duckdb.__version__})
        publish(stage,{'profile':profile} if profile_only else paths)
        print(json.dumps(summary,indent=2),flush=True)
        return summary

def parser():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--project-root',type=Path,default=ROOT)
    p.add_argument('--year',type=int)
    p.add_argument('--aggregation-buckets',type=int,default=int(os.getenv('IC_PMID_NETWORK_BUCKETS','64')))
    p.add_argument('--profile-only',action='store_true')
    p.add_argument('--overwrite',action='store_true')
    return p
if __name__=='__main__':
    a=parser().parse_args()
    y=a.year
    if y is None:
        y=1980+int(os.environ['SLURM_ARRAY_TASK_ID']) if 'SLURM_ARRAY_TASK_ID' in os.environ else int(os.environ['IC_FOCAL_YEAR'])
    build(a.project_root,y,a.aggregation_buckets,a.overwrite,a.profile_only)
