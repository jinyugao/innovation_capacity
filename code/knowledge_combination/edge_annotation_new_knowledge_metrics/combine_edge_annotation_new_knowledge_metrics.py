"""Validate and concatenate yearly PMID and globally deduplicated annual metrics."""
from __future__ import annotations
import csv
import os
from pathlib import Path
import tempfile
from build_edge_annotation_new_knowledge_metrics import (
    ROOT,SUBDIR,STEM,METRICS,ALL_COUNTS,q,scalar,connect,guard,write_csv,paths,
    validate_metrics,compare_old,
)


def read_one(path):
    with path.open(newline='',encoding='utf-8') as handle:
        rows=list(csv.DictReader(handle))
    if len(rows)!=1:
        raise ValueError(f'Expected one row: {path}')
    return rows[0]


def combine(root,years=range(1980,2024)):
    years=list(years)
    if not years or years!=sorted(set(years)):
        raise ValueError('Years must be sorted and unique')
    base=root/SUBDIR
    master=base/'pmid'/f'pmid_{STEM}_{years[0]}_{years[-1]}.parquet'
    yearly=base/'year'/f'year_{STEM}_{years[0]}_{years[-1]}.csv'
    checks_path=base/'validation'/f'{STEM}_year_validation.csv'
    report_path=base/'validation'/f'{STEM}_validation_summary.csv'
    guard([master,yearly,checks_path,report_path])
    checks,annual_rows,files=[],[],[]
    with tempfile.TemporaryDirectory(prefix='combine_new_knowledge_',dir=base) as temp:
        con=connect(temp)
        try:
            for year in years:
                pmid,annual,validation=paths(root,year)
                check={'PYEAR':year,'pmid_file_exists':pmid.is_file(),'year_file_exists':annual.is_file(),'validation_file_exists':validation.is_file(),'n_pmids':'','all_checks_passed':False,'error':''}
                try:
                    if not all(p.is_file() for p in [pmid,annual,validation]):
                        raise FileNotFoundError('Missing annual output or validation report')
                    prior=read_one(validation)
                    if int(prior['PYEAR'])!=year or prior['all_checks_passed']!='True' or prior['old_metrics_match']!='True' or prior['pmid_sets_match']!='True':
                        raise ValueError('Annual build validation did not pass')
                    con.execute(f'CREATE OR REPLACE TEMP VIEW pmid_metrics AS SELECT * FROM read_parquet({q(pmid)})')
                    validate_metrics(con,'pmid_metrics',year,True)
                    compare_old(con,root,year)
                    raw=read_one(annual)
                    if list(raw)!=['PYEAR']+METRICS:
                        raise ValueError('Incorrect annual CSV columns')
                    types={'PYEAR':'INTEGER',**{m:('BIGINT' if m in ALL_COUNTS else 'DOUBLE') for m in METRICS}}
                    spec='{'+','.join(q(k)+':'+q(v) for k,v in types.items())+'}'
                    con.execute(f'CREATE OR REPLACE TEMP VIEW annual_metrics AS SELECT * FROM read_csv({q(annual)},header=true,columns={spec},nullstr=\'\')')
                    validate_metrics(con,'annual_metrics',year,False)
                    count=scalar(con,'SELECT COUNT(*) FROM pmid_metrics')
                    total=scalar(con,'SELECT COALESCE(SUM(n_triples),0) FROM pmid_metrics')
                    if count!=int(prior['n_pmids']) or total!=int(prior['n_input_triples']):
                        raise ValueError('Annual output differs from build report')
                    # Unique annual counts are bounded by each paper and by their sum.
                    for metric in ALL_COUNTS:
                        maximum,summed=con.execute(f'SELECT COALESCE(MAX({metric}),0),COALESCE(SUM({metric}),0) FROM pmid_metrics').fetchone()
                        value=int(raw[metric])
                        if not maximum<=value<=summed:
                            raise ValueError(f'Annual unique count outside PMID bounds: {metric}')
                    check.update(n_pmids=count,all_checks_passed=True)
                    files.append(pmid); annual_rows.append(raw)
                except Exception as exc:
                    check['error']=str(exc)
                checks.append(check)
            write_csv(checks_path,checks)
            passed=sum(r['all_checks_passed'] for r in checks)
            report={'start_year':years[0],'end_year':years[-1],'expected_years':len(years),'passed_years':passed,'failed_years':len(years)-passed,'annual_row_sum':sum(r['n_pmids'] for r in checks if isinstance(r['n_pmids'],int)),'combined_rows':'','distinct_pmids':'','cross_year_duplicate_pmid_excess_rows':'','year_table_rows':len(annual_rows),'all_checks_passed':False,'error':''}
            try:
                if passed!=len(years):
                    raise ValueError('Annual validation failed; see year validation report')
                listing='['+','.join(map(q,files))+']'
                con.execute(f'CREATE TEMP VIEW combined AS SELECT * FROM read_parquet({listing})')
                count,distinct=con.execute('SELECT COUNT(*),COUNT(DISTINCT PMID) FROM combined').fetchone()
                report.update(combined_rows=count,distinct_pmids=distinct,cross_year_duplicate_pmid_excess_rows=count-distinct)
                if count!=distinct or count!=report['annual_row_sum']:
                    raise ValueError('Global PMID uniqueness or row accounting failed')
                staged=Path(temp)/'master.parquet'
                con.execute(f'COPY (SELECT * FROM combined ORDER BY PYEAR,PMID) TO {q(staged)} (FORMAT PARQUET,COMPRESSION ZSTD)')
                write_csv(yearly,annual_rows)
                staged.replace(master)
                report['all_checks_passed']=True
            except Exception as exc:
                report['error']=str(exc)
                raise
            finally:
                write_csv(report_path,[report])
            print(f'Saved {master} and {yearly}; {report}')
        finally:
            con.close()


if __name__=='__main__':
    combine(Path(os.getenv('IC_PROJECT_ROOT',str(ROOT))))
