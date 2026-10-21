"""Audit SemMedDB/OpenAlex mapping multiplicity and dates; no SciSciNet input."""
from __future__ import annotations
import argparse
import csv
import gzip
import json
import os
from pathlib import Path
import tempfile
import duckdb

PROJECT=Path(os.getenv('IC_PROJECT_ROOT','/xdisk/sebratt/jinyugao/research/projects/innovation_capacity'))
OA=Path('/xdisk/sebratt/jinyugao/research/data/processed/openalex/flattened_snapshot_2025')

def q(v):
    return "'"+str(v).replace("'","''")+"'"

def work(e):
    return f"nullif(regexp_extract(trim({e}),'^(?:https?://openalex[.]org/)?(W[0-9]+)/?$',1),'')"

def pmid(e):
    v=f"nullif(regexp_extract(trim({e}),'^(?:https?://(?:www[.])?ncbi[.]nlm[.]nih[.]gov/pubmed/|https?://pubmed[.]ncbi[.]nlm[.]nih[.]gov/)?([0-9]+)/?$',1),'')"
    return f"CASE WHEN regexp_matches({v},'[1-9]') THEN ltrim({v},'0') END"

def year(e):
    t=f'trim(CAST({e} AS VARCHAR))'
    return f"CASE WHEN regexp_full_match({t},'[12][0-9]{{3}}') THEN try_cast({t} AS INTEGER) END"

def scan(path):
    return f"read_csv({q(path)},header=true,all_varchar=true,delim=',',quote='\"',escape='\"',nullstr='')"

def citations_scan(args):
    opener=gzip.open if args.citations.suffix=='.gz' else open
    with opener(args.citations,'rt',encoding='utf-8-sig',newline='') as h:
        first=next(csv.reader(h),None)
    if not first or len(first)!=5:
        raise ValueError('CITATIONS must have five columns: PMID,ISSN,DP,EDAT,PYEAR')
    header=first[0].strip().upper()=='PMID'
    if header!=args.citations_header:
        raise ValueError('CITATIONS header does not match --citations-header')
    if header:
        if [v.strip().upper() for v in first]!=['PMID','ISSN','DP','EDAT','PYEAR']:
            raise ValueError('Unexpected CITATIONS column order')
        return scan(args.citations)
    return f"read_csv({q(args.citations)},header=false,columns={{'PMID':'VARCHAR','ISSN':'VARCHAR','DP':'VARCHAR','EDAT':'VARCHAR','PYEAR':'VARCHAR'}},delim=',',quote='\"',escape='\"',nullstr='')"

def run(args):
    if args.sample_size<1:
        raise ValueError('sample-size must be positive')
    for key in ['analysis','citations','ids','years']:
        if not getattr(args,key).is_file():raise FileNotFoundError(getattr(args,key))
    if args.output.exists() and any(args.output.iterdir()):
        raise FileExistsError(f'Output directory is not empty: {args.output}')
    args.output.parent.mkdir(parents=True,exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='semmed_openalex_',dir=args.output.parent) as temp:
        stage=Path(temp)/'results';stage.mkdir()
        with duckdb.connect(str(Path(temp)/'audit.duckdb')) as con:
            con.execute(f"SET threads={int(os.getenv('IC_DUCKDB_THREADS','8'))}")
            con.execute(f"SET memory_limit={q(os.getenv('IC_DUCKDB_MEMORY_LIMIT','48GB'))}")
            con.execute(f"SET temp_directory={q(Path(temp)/'spill')}")
            con.execute('SET preserve_insertion_order=false')
            def sql(s):con.execute(s)
            def n(s):return con.execute(s).fetchone()[0]
            def export(s,name):
                fmt='PARQUET, COMPRESSION ZSTD' if name.endswith('.parquet') else 'CSV, HEADER TRUE'
                sql(f'COPY ({s}) TO {q(stage/name)} (FORMAT {fmt})')
            print('1/4: research cohort and original SemMedDB dates',flush=True)
            sql(f'''CREATE TABLE analysis_raw AS SELECT {pmid('CAST(PMID AS VARCHAR)')} pmid,
                  {year('PYEAR')} analysis_year FROM read_parquet({q(args.analysis)})''')
            if not n('SELECT count(*) FROM analysis_raw') or n('SELECT count(*) FROM analysis_raw WHERE pmid IS NULL OR analysis_year IS NULL'):
                raise ValueError('Empty cohort or invalid analysis PMID/year')
            sql('''CREATE TABLE cohort AS SELECT pmid,count(*) analysis_rows,
                count(DISTINCT analysis_year) n_analysis_years,
                CASE WHEN count(DISTINCT analysis_year)=1 THEN min(analysis_year) END analysis_year
                FROM analysis_raw GROUP BY pmid''')
            export('SELECT * FROM cohort WHERE analysis_rows>1','analysis_key_diagnostics.csv')
            dp_expr=r"regexp_extract(DP,'^\s*([0-9]{4})',1)"
            edat_expr=r"regexp_extract(EDAT,'^\s*([0-9]{4})',1)"
            sql(f'''CREATE TABLE dates AS SELECT {pmid('PMID')} pmid,ISSN,DP,EDAT,PYEAR raw_pyear,
                {year('PYEAR')} semmed_year,
                {year(dp_expr)} dp_year,
                {year(edat_expr)} edat_year
                FROM {citations_scan(args)} WHERE {pmid('PMID')} IN (SELECT pmid FROM cohort)''')
            export('SELECT DISTINCT * FROM dates','semmed_citation_dates.parquet')
            sql('''CREATE TABLE semmed AS SELECT pmid,count(*) semmed_rows,
                string_agg(DISTINCT ISSN,'; ' ORDER BY ISSN) journal_issns,
                string_agg(DISTINCT raw_pyear,'; ' ORDER BY raw_pyear) raw_semmed_years,
                string_agg(DISTINCT DP,'; ' ORDER BY DP) raw_semmed_dp,
                string_agg(DISTINCT EDAT,'; ' ORDER BY EDAT) raw_semmed_edat,
                count(DISTINCT semmed_year) n_semmed_years,
                count(*) FILTER(WHERE semmed_year IS NULL) invalid_semmed_year_rows,
                CASE WHEN count(DISTINCT semmed_year)=1 AND count(*) FILTER(WHERE semmed_year IS NULL)=0 THEN min(semmed_year) END semmed_year,
                CASE WHEN count(DISTINCT dp_year)=1 AND count(*) FILTER(WHERE dp_year IS NULL)=0 THEN min(dp_year) END dp_year,
                CASE WHEN count(DISTINCT edat_year)=1 AND count(*) FILTER(WHERE edat_year IS NULL)=0 THEN min(edat_year) END edat_year
                FROM dates GROUP BY pmid''')
            print('2/4: global OpenAlex PMID-work cardinalities',flush=True)
            sql(f'''CREATE TABLE ids_raw AS SELECT trim(work_id) raw_work_id,trim(pmid) raw_pmid,trim(doi) raw_doi,
                {work('work_id')} work_id,{pmid('pmid')} pmid,
                nullif(lower(regexp_replace(trim(doi),'^https?://(?:dx[.])?doi[.]org/','','i')),'') doi
                FROM {scan(args.ids)} WHERE pmid IS NOT NULL AND trim(pmid)<>'' ''')
            export('SELECT * FROM ids_raw WHERE work_id IS NULL OR pmid IS NULL','rejected_openalex_mappings.csv')
            sql('CREATE TABLE mappings AS SELECT DISTINCT pmid,work_id FROM ids_raw WHERE work_id IS NOT NULL AND pmid IS NOT NULL')
            sql('CREATE TABLE global_pmid AS SELECT pmid,count(*) n_works FROM mappings GROUP BY pmid')
            sql('CREATE TABLE global_work AS SELECT work_id,count(*) n_pmids_for_work FROM mappings GROUP BY work_id')
            export("SELECT 'global_openalex' AS scope,n_works,count(*) n_pmids,sum(n_works) n_pmid_work_links FROM global_pmid GROUP BY n_works ORDER BY n_works",'global_work_multiplicity.csv')
            sql('CREATE TABLE target_mapping AS SELECT m.*,w.n_pmids_for_work FROM mappings m JOIN global_work w USING(work_id) SEMI JOIN cohort USING(pmid)')
            sql('CREATE TABLE target_works AS SELECT DISTINCT work_id FROM target_mapping')
            sql('''CREATE TABLE dois AS SELECT work_id,count(DISTINCT doi) n_dois_for_work,
                string_agg(DISTINCT raw_doi,'; ' ORDER BY raw_doi) raw_dois,
                CASE WHEN count(DISTINCT doi)=1 THEN min(doi) END doi
                FROM ids_raw SEMI JOIN target_works USING(work_id) GROUP BY work_id''')
            print('3/4: OpenAlex publication years',flush=True)
            sql(f'''CREATE TABLE oa_raw AS SELECT {work('work_id')} work_id,
                publication_year raw_year,{year('publication_year')} oa_year
                FROM {scan(args.years)} WHERE {work('work_id')} IN (SELECT work_id FROM target_works)''')
            sql('''CREATE TABLE oa AS SELECT work_id,count(*) oa_rows,count(DISTINCT oa_year) n_oa_years,
                count(*) FILTER(WHERE oa_year IS NULL) invalid_oa_year_rows,
                string_agg(DISTINCT raw_year,'; ' ORDER BY raw_year) raw_openalex_years,
                CASE WHEN count(DISTINCT oa_year)=1 AND count(*) FILTER(WHERE oa_year IS NULL)=0 THEN min(oa_year) END openalex_year
                FROM oa_raw GROUP BY work_id''')
            export('SELECT * FROM oa WHERE oa_rows>1 OR n_oa_years<>1 OR invalid_oa_year_rows>0','openalex_year_key_diagnostics.csv')
            print('4/4: one-row-per-PMID classifications and summaries',flush=True)
            sql('''CREATE TABLE links AS SELECT a.*,s.* EXCLUDE(pmid),m.work_id,m.n_pmids_for_work,
                o.* EXCLUDE(work_id),d.* EXCLUDE(work_id),
                o.openalex_year-s.semmed_year year_difference,
                CASE WHEN m.work_id IS NULL THEN 'unmatched'
                     WHEN s.semmed_year IS NULL THEN 'semmed_year_unavailable'
                     WHEN o.openalex_year IS NULL THEN 'openalex_year_unavailable'
                     WHEN o.openalex_year=s.semmed_year THEN 'equal' ELSE 'different' END work_year_status
                FROM cohort a LEFT JOIN semmed s USING(pmid) LEFT JOIN target_mapping m USING(pmid)
                LEFT JOIN oa o USING(work_id) LEFT JOIN dois d USING(work_id)''')
            export('SELECT * FROM links','pmid_work_year_details.parquet')
            sql('''CREATE TABLE paper_counts AS SELECT pmid,analysis_year,n_analysis_years,
                semmed_year,dp_year,edat_year,journal_issns,semmed_rows,n_semmed_years,invalid_semmed_year_rows,
                count(work_id) n_works,
                count(*) FILTER(WHERE work_id IS NOT NULL AND openalex_year IS NOT NULL) n_works_with_usable_year,
                count(*) FILTER(WHERE work_id IS NOT NULL AND openalex_year IS NULL) n_works_with_unavailable_year,
                count(*) FILTER(WHERE work_year_status='equal') n_matching_works,
                count(*) FILTER(WHERE work_year_status='different') n_different_year_works,
                count(DISTINCT openalex_year) n_distinct_usable_openalex_years,
                min(openalex_year) min_openalex_year,max(openalex_year) max_openalex_year,
                min(year_difference) min_year_difference,max(year_difference) max_year_difference,
                coalesce(bool_or(n_pmids_for_work>1),false) has_work_shared_with_other_pmids,
                count(*) FILTER(WHERE doi IS NOT NULL) n_works_with_usable_doi,
                count(DISTINCT doi) n_distinct_usable_dois,
                count(*) FILTER(WHERE n_dois_for_work>1) n_works_with_conflicting_dois
                FROM links GROUP BY ALL''')
            sql('''CREATE TABLE papers AS SELECT *,
                CASE WHEN n_works=0 THEN 'unmatched' WHEN n_works=1 THEN 'single_work' ELSE 'multiple_works' END mapping_group,
                CASE WHEN n_works=0 THEN 'unmatched'
                     WHEN semmed_year IS NULL THEN 'semmed_year_unavailable'
                     WHEN n_works_with_unavailable_year>0 THEN 'incomplete_openalex_years'
                     WHEN n_matching_works=n_works THEN 'all_match'
                     WHEN n_matching_works>0 THEN 'some_match' ELSE 'none_match' END year_status,
                CASE WHEN semmed_year IS NULL OR analysis_year IS NULL THEN NULL ELSE semmed_year=analysis_year END analysis_semmed_year_equal,
                CASE WHEN semmed_year IS NULL OR dp_year IS NULL THEN NULL ELSE semmed_year=dp_year END semmed_pyear_dp_equal
                FROM paper_counts''')
            export('SELECT * FROM papers','pmid_summary.parquet')
            group='mapping_group,year_status,has_work_shared_with_other_pmids'
            export(f'SELECT {group},count(*) n_pmids,sum(n_works) n_pmid_work_links,sum(CASE WHEN n_different_year_works>0 THEN 1 ELSE 0 END) n_pmids_with_any_observed_year_difference FROM papers GROUP BY ALL ORDER BY 1,2,3','mapping_year_summary.csv')
            export(f'SELECT analysis_year,{group},count(*) n_pmids,sum(n_works) n_pmid_work_links,sum(CASE WHEN n_different_year_works>0 THEN 1 ELSE 0 END) n_pmids_with_any_observed_year_difference FROM papers GROUP BY ALL ORDER BY 1,2,3,4','mapping_year_summary_by_year.csv')
            export('SELECT n_works,count(*) n_pmids,sum(n_works) n_pmid_work_links FROM papers GROUP BY n_works ORDER BY n_works','cohort_work_multiplicity.csv')
            export('SELECT analysis_year,n_works,count(*) n_pmids FROM papers GROUP BY ALL ORDER BY 1,2','cohort_work_multiplicity_by_year.csv')
            export('SELECT n_works,n_distinct_usable_openalex_years,n_works_with_unavailable_year,year_status,count(*) n_pmids FROM papers WHERE n_works>1 GROUP BY ALL ORDER BY 1,2,3,4','multiple_work_year_patterns.csv')
            export('''SELECT p.mapping_group,l.analysis_year,l.year_difference,count(*) n_pmid_work_links,count(DISTINCT l.pmid) n_pmids
                FROM links l JOIN papers p USING(pmid) WHERE l.year_difference IS NOT NULL GROUP BY ALL ORDER BY 1,2,3''','year_difference_by_mapping_group.csv')
            export('SELECT analysis_year,semmed_year,dp_year,edat_year,count(*) n_pmids FROM papers GROUP BY ALL ORDER BY 1,2,3,4','semmed_date_comparison.csv')
            export('''SELECT journal_issns,mapping_group,year_status,count(*) n_pmids FROM papers GROUP BY ALL ORDER BY 1,2,3''','mapping_year_summary_by_issn.csv')
            export('''SELECT n_works,n_works_with_usable_doi,n_distinct_usable_dois,n_works_with_conflicting_dois,count(*) n_pmids
                FROM papers WHERE n_works>1 GROUP BY ALL ORDER BY 1,2,3,4''','multiple_work_doi_summary.csv')
            sql(f'''CREATE TABLE inspection AS SELECT * FROM (SELECT *,row_number() OVER(PARTITION BY mapping_group,year_status,
                CASE WHEN min_year_difference IS NULL THEN 'unavailable' WHEN min_year_difference=max_year_difference AND abs(min_year_difference)<=1
                THEN CAST(min_year_difference AS VARCHAR) ELSE 'other_or_mixed' END
                ORDER BY hash(pmid),pmid) sample_rank FROM papers WHERE mapping_group<>'single_work' OR year_status<>'all_match'
                OR analysis_semmed_year_equal=false OR semmed_pyear_dp_equal=false) WHERE sample_rank<={args.sample_size}''')
            export('SELECT * FROM inspection','inspection_pmids.csv')
            export('SELECT l.* FROM links l SEMI JOIN inspection USING(pmid)','inspection_work_details.csv')
            cohort_n=n('SELECT count(*) FROM cohort')
            if n('SELECT count(*) FROM papers')!=cohort_n or n('SELECT count(DISTINCT pmid) FROM papers')!=cohort_n:
                raise ValueError('PMID summary row accounting failed')
            expected=n('SELECT sum(CASE WHEN g.n_works IS NULL THEN 1 ELSE g.n_works END) FROM cohort LEFT JOIN global_pmid g USING(pmid)')
            if n('SELECT count(*) FROM links')!=expected:raise ValueError('PMID-work row accounting failed')
            if n('SELECT count(*) FROM papers WHERE n_works<>n_works_with_usable_year+n_works_with_unavailable_year OR (semmed_year IS NOT NULL AND n_matching_works+n_different_year_works<>n_works_with_usable_year)'):
                raise ValueError('Per-PMID work/year partition failed')
            report={'inputs':{k:str(getattr(args,k)) for k in ['analysis','citations','ids','years']},
                'analysis_pmids':cohort_n,'analysis_raw_rows':n('SELECT count(*) FROM analysis_raw'),
                'analysis_pmids_with_conflicting_years':n('SELECT count(*) FROM cohort WHERE n_analysis_years>1'),
                'global_pmids_with_multiple_works':n('SELECT count(*) FROM global_pmid WHERE n_works>1'),
                'global_works_with_multiple_pmids':n('SELECT count(*) FROM global_work WHERE n_pmids_for_work>1'),
                'cohort_pmids_with_multiple_works':n('SELECT count(*) FROM papers WHERE n_works>1'),
                'cohort_max_works_per_pmid':n('SELECT max(n_works) FROM papers'),
                'cohort_pmids_with_no_mapping':n('SELECT count(*) FROM papers WHERE n_works=0'),
                'cohort_pmids_with_any_observed_year_difference':n('SELECT count(*) FROM papers WHERE n_different_year_works>0'),
                'output_pmid_rows':cohort_n,'output_link_rows':expected,'all_checks_passed':True,
                'note':'No SciSciNet input. Matching work years do not prove bibliographic identity. Missing/ambiguous years are not counted as equal or different. Status groups are mutually exclusive at PMID level; year-difference rows may overlap at PMID level.'}
            (stage/'validation_summary.json').write_text(json.dumps(report,indent=2))
        args.output.mkdir(exist_ok=True)
        for f in stage.iterdir():
            if f.name!='validation_summary.json':f.replace(args.output/f.name)
        (stage/'validation_summary.json').replace(args.output/'validation_summary.json')
        print(json.dumps(report,indent=2),flush=True)

def parser():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--analysis',type=Path,default=PROJECT/'data/processed/knowledge_combination/edge_annotation_metrics/pmid_edge_annotation_metrics_1980_2023.parquet')
    p.add_argument('--citations',type=Path,default=Path('/home/u23/jinyugao/research/data/source/semmedVER43_R/semmedVER43_2024_R_CITATIONS.csv.gz'))
    p.add_argument('--ids',type=Path,default=OA/'openalex_works_ids.csv.gz')
    p.add_argument('--years',type=Path,default=OA/'openalex_works_publication_year.csv.gz')
    p.add_argument('--output',type=Path,default=PROJECT/'data/processed/data_validation/semmeddb_openalex')
    p.add_argument('--citations-header',action='store_true')
    p.add_argument('--sample-size',type=int,default=100)
    return p

if __name__=='__main__':run(parser().parse_args())
