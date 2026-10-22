"""Validate and combine focal-year edges; retain all eligible PMID nodes."""
import argparse
import json
from pathlib import Path
import tempfile
from network_common import *


def combine(root,start=1980,end=2018,overwrite=False):
    root=Path(root)
    if not 1980<=start<=end<=2018:raise ValueError('Expected 1980 <= start <= end <= 2018')
    b=root/OUT;tag=f'{start}_{end}';node_tag=f'{start}_{end+5}'
    outputs={'edges':b/'edges'/f'pmid_adoption_network_edges_{tag}.parquet',
             'nodes':b/'nodes'/f'pmid_adoption_network_nodes_{node_tag}.parquet',
             'summary':b/'summary'/f'pmid_adoption_network_summary_{tag}.csv',
             'validation':b/'validation'/f'pmid_adoption_network_validation_{tag}.json'}
    guard(outputs.values(),overwrite)
    reports=[];annual_files=[];expected_sources={}
    for y in range(start,end+1):
        paths=annual(root,y)
        for f in paths.values():
            if not f.is_file():raise FileNotFoundError(f)
        report=json.loads(paths['validation'].read_text());summary=json.loads(paths['summary'].read_text())
        if report.get('all_checks_passed') is not True or report.get('focal_year')!=y:raise ValueError(f'Failed annual validation: {y}')
        if signature([Path(f) for f in report['input_signature']])!=report['input_signature']:raise ValueError(f'Stale source inputs: {y}')
        for source,stamp in report['input_signature'].items():
            if source in expected_sources and expected_sources[source]!=stamp:raise ValueError(f'Annual builds used different source versions: {source}')
            expected_sources[source]=stamp
        for k in ['edges','trace','nodes','summary']:
            if signature([paths[k]])[str(paths[k])]!=report['outputs'][k]:raise ValueError(f'Annual output changed after validation: {paths[k]}')
        annual_files.extend(paths.values());reports.append(summary)
    before=signature(annual_files)
    b.mkdir(parents=True,exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='combine_',dir=b) as temp:
        stage=Path(temp)/'outputs';stage.mkdir()
        with connection(temp) as c:
            for name,kind in [('edges','edges'),('focal_nodes','nodes')]:
                paths=[annual(root,y)[kind] for y in range(start,end+1)]
                c.execute(f"CREATE TABLE {name} AS SELECT * FROM read_parquet([{','.join(map(q,paths))}])")
            require(c,'SELECT count(*) FROM (SELECT adopter_pmid,focal_pmid FROM edges GROUP BY ALL HAVING count(*)>1)','Duplicate global directed PMID pair')
            require(c,'SELECT count(*) FROM edges WHERE year_gap NOT BETWEEN 1 AND 5 OR adopter_year-focal_year<>year_gap OR adopter_pmid=focal_pmid OR n_shared_new_triples<=0 OR n_shared_new_triples<>n_shared_new_node_triples+n_shared_new_combination_triples+n_shared_new_relation_triples','Invalid combined edge')
            require(c,'SELECT count(*) FROM (SELECT PMID FROM focal_nodes GROUP BY PMID HAVING count(*)>1)','Cross-year duplicate focal PMID')
            if scalar(c,'SELECT count(*) FROM edges')!=sum(r['n_edges'] for r in reports):raise ValueError('Combined edge count mismatch')
            if scalar(c,'SELECT coalesce(sum(n_shared_new_triples),0) FROM edges')!=sum(r['n_triple_paper_pair_occurrences'] for r in reports):raise ValueError('Combined edge weight mismatch')
            # Node table includes all annotated papers, not only endpoint PMIDs.
            annotation_files=[annotation(root,y) for y in range(start,end+6)]
            annotation_before=signature(annotation_files)
            for y in range(start,end+6):
                load(c,'annotation_year',annotation(root,y),y)
                statement=f'''SELECT PMID,{y}::INTEGER PYEAR,count(*)::BIGINT n_triples,
                    {','.join(f"count(*) FILTER(WHERE edge_annotation={q(cat)})::BIGINT n_{kind}" for cat,kind in zip(NEW,KINDS))},
                    count(*) FILTER(WHERE edge_annotation<>'Repeated_Triple')::BIGINT n_new_triples
                    FROM annotation_year GROUP BY PMID'''
                c.execute(('CREATE TABLE all_nodes AS ' if y==start else 'INSERT INTO all_nodes ')+statement)
                c.execute('DROP TABLE annotation_year')
            require(c,'SELECT count(*) FROM (SELECT PMID FROM all_nodes GROUP BY PMID HAVING count(*)>1)','Annotation PMID occurs in multiple years')
            rcols=['PYEAR','n_triples',*[f'n_{k}' for k in KINDS],'n_new_triples']
            mismatch=' OR '.join(['a.PMID IS NULL','f.PMID IS NULL',*[f'a.{k} IS DISTINCT FROM f.{k}' for k in rcols]])
            require(c,f'SELECT count(*) FROM (SELECT * FROM all_nodes WHERE PYEAR BETWEEN {start} AND {end}) a FULL JOIN focal_nodes f USING(PMID) WHERE {mismatch}','Node/focal annotations mismatch')
            require(c,'SELECT count(*) FROM edges e LEFT JOIN all_nodes a ON e.adopter_pmid=a.PMID LEFT JOIN all_nodes f ON e.focal_pmid=f.PMID WHERE a.PMID IS NULL OR f.PMID IS NULL OR a.PYEAR<>e.adopter_year OR f.PYEAR<>e.focal_year','Edge endpoints/years absent from nodes')
            c.execute('CREATE TABLE incoming AS SELECT focal_pmid PMID,count(*)::BIGINT in_degree,sum(n_shared_new_triples)::BIGINT in_strength FROM edges GROUP BY focal_pmid')
            c.execute('CREATE TABLE outgoing AS SELECT adopter_pmid PMID,count(*)::BIGINT out_degree,sum(n_shared_new_triples)::BIGINT out_strength FROM edges GROUP BY adopter_pmid')
            cols=','.join(f'CASE WHEN a.PYEAR<={end} THEN f.n_{k}_adopted END n_{k}_adopted' for k in KINDS)
            c.execute(f'''CREATE TABLE nodes AS SELECT a.*,a.PYEAR<={end} focal_eligible,
                a.PYEAR>{start} adopter_eligible,
                a.PYEAR<={end} has_complete_five_year_followup,
                a.PYEAR>={start+5} AND a.PYEAR<={end+1} has_complete_five_year_focal_coverage,
                {cols},CASE WHEN a.PYEAR<={end} THEN f.n_new_triples_adopted END n_new_triples_adopted,
                CASE WHEN a.PYEAR<={end} THEN f.has_new_triples_adopted END has_new_triples_adopted,
                CASE WHEN a.PYEAR>{end} THEN 'target_only'
                     WHEN a.n_new_triples=0 THEN 'no_new_triples'
                     WHEN f.n_new_triples_adopted=0 THEN 'new_triples_not_adopted' ELSE 'new_triples_adopted' END focal_status,
                coalesce(i.in_degree,0)::BIGINT in_degree,coalesce(i.in_strength,0)::BIGINT in_strength,
                coalesce(o.out_degree,0)::BIGINT out_degree,coalesce(o.out_strength,0)::BIGINT out_strength
                FROM all_nodes a LEFT JOIN focal_nodes f USING(PMID) LEFT JOIN incoming i USING(PMID) LEFT JOIN outgoing o USING(PMID)''')
            require(c,'SELECT count(*) FROM nodes WHERE focal_eligible AND (in_degree>0) IS DISTINCT FROM (has_new_triples_adopted=1)','Node in-degree/adoption mismatch')
            # Reconcile each focal-year node file with global graph degrees.
            require(c,'SELECT count(*) FROM nodes n JOIN focal_nodes f USING(PMID) WHERE n.in_degree<>f.n_future_papers OR n.in_strength<>f.n_shared_new_triple_occurrences','Annual/global in-degree mismatch')
            for k in ['edges','nodes']:parquet(c,f'SELECT * FROM {k}',stage/outputs[k].name)
            csv(c,f'''SELECT y.focal_year,coalesce(e.n_edges,0)::BIGINT n_edges,coalesce(e.total_shared_new_triples,0) total_shared_new_triples,
                 coalesce(e.n_focal_pmids_with_incoming_edges,0)::BIGINT n_focal_pmids_with_incoming_edges,
                 coalesce(e.n_adopter_pmids,0)::BIGINT n_adopter_pmids FROM range({start},{end+1}) y(focal_year)
                 LEFT JOIN (SELECT focal_year,count(*) n_edges,sum(n_shared_new_triples) total_shared_new_triples,
                 count(DISTINCT focal_pmid) n_focal_pmids_with_incoming_edges,count(DISTINCT adopter_pmid) n_adopter_pmids
                 FROM edges GROUP BY focal_year) e USING(focal_year) ORDER BY focal_year''',stage/outputs['summary'].name)
            report={'start_year':start,'end_year':end,'node_end_year':end+5,'n_nodes':scalar(c,'SELECT count(*) FROM nodes'),
                    'n_edges':scalar(c,'SELECT count(*) FROM edges'),'n_zero_degree_nodes':scalar(c,'SELECT count(*) FROM nodes WHERE in_degree=0 AND out_degree=0'),
                    'n_focal_eligible_nodes':scalar(c,'SELECT count(*) FROM nodes WHERE focal_eligible'),
                    'direction':'adopter_pmid -> focal_pmid','all_checks_passed':True}
            if (signature(annual_files)!=before or signature(annotation_files)!=annotation_before
                or signature([Path(f) for f in expected_sources])!=expected_sources):raise ValueError('Inputs changed during combine')
            write_json(stage/outputs['validation'].name,report)
        publish(stage,outputs)
        print(json.dumps(report,indent=2),flush=True)
        return report

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--project-root',type=Path,default=ROOT)
    p.add_argument('--start-year',type=int,default=1980);p.add_argument('--end-year',type=int,default=2018)
    p.add_argument('--overwrite',action='store_true');a=p.parse_args()
    combine(a.project_root,a.start_year,a.end_year,a.overwrite)
