"""Audit identifier cardinality and year provenance without resolving conflicts."""
from __future__ import annotations
import argparse
import csv
import gzip
import json
import os
from pathlib import Path
import tempfile
import duckdb

PROJECT = Path(os.getenv('IC_PROJECT_ROOT','/xdisk/sebratt/jinyugao/research/projects/innovation_capacity'))
OA = Path('/xdisk/sebratt/jinyugao/research/data/processed/openalex/flattened_snapshot_2025')

def q(x):
    return "'" + str(x).replace("'", "''") + "'"

def work(expr):
    return f"nullif(regexp_extract(trim({expr}), '^(?:https?://openalex[.]org/)?(W[0-9]+)/?$', 1),'')"

def pmid(expr):
    v = f"nullif(regexp_extract(trim({expr}), '^(?:https?://(?:www[.])?ncbi[.]nlm[.]nih[.]gov/pubmed/|https?://pubmed[.]ncbi[.]nlm[.]nih[.]gov/)?([0-9]+)/?$',1),'')"
    return f"CASE WHEN regexp_matches({v}, '[1-9]') THEN ltrim({v}, '0') END"

def year_value(expr):
    # DuckDB INTEGER casts round decimal strings and accept scientific notation.
    # A publication year must instead be an explicit four-digit year.
    text = f"trim(CAST({expr} AS VARCHAR))"
    return f"CASE WHEN regexp_full_match({text}, '[12][0-9]{{3}}') THEN try_cast({text} AS INTEGER) END"

def csvscan(path, header=True):
    return f"read_csv({q(path)},header={str(header).lower()},all_varchar=true,delim=',',quote='\"',escape='\"',nullstr='')"

def run(args):
    for name in ['analysis','citations','ids','years','sciscinet']:
        if not getattr(args,name).is_file():
            raise FileNotFoundError(getattr(args,name))
    if args.output.exists() and any(args.output.iterdir()):
        raise FileExistsError(f'Output is not empty: {args.output}; choose a new output directory')
    args.output.parent.mkdir(parents=True,exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='three_source_audit_',dir=args.output.parent) as temp:
        staging=Path(temp)/'outputs';staging.mkdir()
        with duckdb.connect(str(Path(temp)/'audit.duckdb')) as con:
            con.execute(f"SET threads={int(os.getenv('IC_DUCKDB_THREADS','8'))}")
            con.execute(f"SET memory_limit={q(os.getenv('IC_DUCKDB_MEMORY_LIMIT','48GB'))}")
            con.execute(f"SET temp_directory={q(Path(temp)/'spill')}")
            con.execute('SET preserve_insertion_order=false')
            def sql(s):
                con.execute(s)
            def export(query,file):
                fmt='PARQUET, COMPRESSION ZSTD' if file.endswith('.parquet') else 'CSV, HEADER TRUE'
                sql(f'COPY ({query}) TO {q(staging/file)} (FORMAT {fmt})')
            def n(query):
                return con.execute(query).fetchone()[0]
            print('1/5: checking analysis PMID-year keys',flush=True)
            sql(f'''CREATE TABLE analysis_raw AS SELECT CAST(PMID AS VARCHAR) raw_pmid,
                {pmid('CAST(PMID AS VARCHAR)')} pmid, CAST(PYEAR AS VARCHAR) raw_year,
                {year_value('PYEAR')} analysis_year FROM read_parquet({q(args.analysis)})''')
            if n('SELECT count(*) FROM analysis_raw WHERE pmid IS NULL OR analysis_year IS NULL'):
                raise ValueError('Invalid analysis PMID/year; repair the input before auditing')
            sql('CREATE TABLE analysis AS SELECT DISTINCT pmid, analysis_year FROM analysis_raw')
            sql('CREATE TABLE cohort AS SELECT DISTINCT pmid FROM analysis')
            if not n('SELECT count(*) FROM analysis'):
                raise ValueError('Analysis cohort is empty')
            export('SELECT pmid,count(*) n_years FROM analysis GROUP BY pmid HAVING count(*)>1','analysis_year_conflicts.csv')
            print('2/5: scanning OpenAlex ID mappings (global cardinalities)',flush=True)
            sql(f'''CREATE TABLE ids_raw AS SELECT trim(work_id) raw_work_id,trim(pmid) raw_pmid,
                 trim(doi) doi,{work('work_id')} work_id,{pmid('pmid')} pmid
                 FROM {csvscan(args.ids)} WHERE pmid IS NOT NULL AND trim(pmid)<>'' ''')
            export("SELECT * FROM ids_raw WHERE work_id IS NULL OR pmid IS NULL",'rejected_openalex_mappings.csv')
            sql('CREATE TABLE mappings AS SELECT DISTINCT work_id,pmid FROM ids_raw WHERE work_id IS NOT NULL AND pmid IS NOT NULL')
            sql('CREATE TABLE work_cardinality AS SELECT work_id,count(*) n_pmids_for_work FROM mappings GROUP BY work_id')
            sql('CREATE TABLE pmid_cardinality AS SELECT pmid,count(*) n_works_for_pmid FROM mappings GROUP BY pmid')
            sql('CREATE TABLE target_works AS SELECT DISTINCT work_id FROM mappings SEMI JOIN cohort USING(pmid)')
            sql('CREATE TABLE target_mapping AS SELECT m.*,w.n_pmids_for_work,p.n_works_for_pmid FROM mappings m JOIN work_cardinality w USING(work_id) JOIN pmid_cardinality p USING(pmid) SEMI JOIN target_works USING(work_id)')
            export('SELECT * FROM target_mapping WHERE n_works_for_pmid>1 OR n_pmids_for_work>1 ORDER BY pmid,work_id','mapping_conflicts.csv')
            sql("CREATE TABLE dois AS SELECT work_id,string_agg(DISTINCT doi,'; ' ORDER BY doi) openalex_dois FROM ids_raw SEMI JOIN target_works USING(work_id) GROUP BY work_id")
            print('3/5: scanning original citation dates and OpenAlex years',flush=True)
            opener=gzip.open if args.citations.suffix=='.gz' else open
            with opener(args.citations,'rt',encoding='utf-8-sig',newline='') as handle:
                first=next(csv.reader(handle),None)
            if not first or len(first)!=5:
                raise ValueError('CITATIONS must contain five CSV columns: PMID,ISSN,DP,EDAT,PYEAR')
            has_header=first[0].strip().upper()=='PMID'
            if has_header!=args.citations_header:
                raise ValueError('CITATIONS header does not match --citations-header setting')
            if has_header and [v.strip().upper() for v in first]!=['PMID','ISSN','DP','EDAT','PYEAR']:
                raise ValueError('Unexpected CITATIONS column order')
            if args.citations_header:
                source=csvscan(args.citations)
            else:
                source=f"read_csv({q(args.citations)},header=false,columns={{'PMID':'VARCHAR','ISSN':'VARCHAR','DP':'VARCHAR','EDAT':'VARCHAR','PYEAR':'VARCHAR'}},delim=',',quote='\"',escape='\"',nullstr='')"
            sql(f'''CREATE TABLE citations AS SELECT {pmid('PMID')} pmid,DP semmed_dp,EDAT semmed_edat,
              PYEAR raw_semmed_pyear,{year_value('PYEAR')} semmed_pyear,
              try_cast(regexp_extract(DP,'^\\s*([0-9]{{4}})',1) AS INTEGER) dp_year,
              try_cast(regexp_extract(EDAT,'^\\s*([0-9]{{4}})',1) AS INTEGER) edat_year
              FROM {source} WHERE {pmid('PMID')} IN (SELECT pmid FROM cohort)''')
            export('SELECT DISTINCT * FROM citations','semmed_citation_dates.parquet')
            sql(f'''CREATE TABLE oa_years AS SELECT {work('work_id')} work_id,
              publication_year raw_openalex_year,{year_value('publication_year')} openalex_year
              FROM {csvscan(args.years)} WHERE {work('work_id')} IN (SELECT work_id FROM target_works)''')
            print('4/5: scanning original SciSciNet work IDs and years',flush=True)
            sql(f'''CREATE TABLE sci AS SELECT {work('CAST(paperid AS VARCHAR)')} work_id,
              CAST(year AS VARCHAR) raw_sciscinet_year,{year_value('year')} sciscinet_year
              FROM read_parquet({q(args.sciscinet)})
              WHERE {work('CAST(paperid AS VARCHAR)')} IN (SELECT work_id FROM target_works)''')
            # A single valid year is usable only if no nonempty invalid year accompanies it.
            for table,key,col,prefix,raw in [
              ('citations','pmid','semmed_pyear','semmed','raw_semmed_pyear'),
              ('oa_years','work_id','openalex_year','openalex','raw_openalex_year'),
              ('sci','work_id','sciscinet_year','sciscinet','raw_sciscinet_year')]:
                sql(f'''CREATE TABLE {prefix}_summary AS SELECT {key},count(*) {prefix}_rows,
                  count(DISTINCT {col}) {prefix}_n_years,
                  count(*) FILTER(WHERE {col} IS NULL) {prefix}_missing_or_invalid_year_rows,
                  string_agg(DISTINCT {raw},'; ' ORDER BY {raw}) {prefix}_raw_years,
                  CASE WHEN count(DISTINCT {col})=1 AND count(*) FILTER(WHERE {col} IS NULL)=0
                       THEN min({col}) END {col}
                  FROM {table} GROUP BY {key}''')
                export(f'SELECT * FROM {prefix}_summary WHERE {prefix}_n_years<>1 OR {prefix}_missing_or_invalid_year_rows>0 OR {prefix}_rows>1',f'{prefix}_key_diagnostics.csv')
            print('5/5: writing full match table and year diagnostics',flush=True)
            sql('''CREATE TABLE joined AS SELECT a.*,m.work_id,
              coalesce(p.n_works_for_pmid,0) n_works_for_pmid,coalesce(w.n_pmids_for_work,0) n_pmids_for_work,
              d.openalex_dois,s.* EXCLUDE(pmid),o.* EXCLUDE(work_id),c.* EXCLUDE(work_id)
              FROM analysis a LEFT JOIN mappings m USING(pmid)
              LEFT JOIN pmid_cardinality p USING(pmid) LEFT JOIN work_cardinality w USING(work_id)
              LEFT JOIN dois d USING(work_id) LEFT JOIN semmed_summary s USING(pmid)
              LEFT JOIN openalex_summary o USING(work_id) LEFT JOIN sciscinet_summary c USING(work_id)''')
            sql('''CREATE TABLE audited AS SELECT *,
              semmed_pyear-analysis_year delta_semmed_analysis,
              openalex_year-semmed_pyear delta_openalex_semmed,
              sciscinet_year-semmed_pyear delta_sciscinet_semmed,
              sciscinet_year-openalex_year delta_sciscinet_openalex,
              CASE WHEN semmed_pyear IS NOT NULL AND semmed_pyear<>analysis_year THEN 'analysis_semmed_year_mismatch'
                   WHEN work_id IS NULL THEN 'no_openalex_mapping'
                   WHEN n_works_for_pmid>1 OR n_pmids_for_work>1 THEN 'ambiguous_mapping'
                   WHEN semmed_pyear IS NULL OR openalex_year IS NULL OR sciscinet_year IS NULL THEN 'missing_or_conflicting_year'
                   WHEN semmed_pyear=openalex_year AND openalex_year=sciscinet_year THEN 'all_equal'
                   WHEN openalex_year=sciscinet_year THEN 'openalex_sciscinet_equal'
                   WHEN semmed_pyear=openalex_year THEN 'semmed_openalex_equal'
                   WHEN semmed_pyear=sciscinet_year THEN 'semmed_sciscinet_equal'
                   ELSE 'all_different' END match_status
              FROM joined''')
            export('SELECT * FROM audited','pmid_work_year_audit.parquet')
            export('SELECT analysis_year,match_status,count(*) n_rows,count(DISTINCT pmid) n_pmids,count(DISTINCT work_id) n_works FROM audited GROUP BY ALL ORDER BY 1,2','match_summary_by_year.csv')
            # These summaries exclude ambiguous mappings and conflicting source years.
            clean="n_works_for_pmid=1 AND n_pmids_for_work=1"
            for name,x,y in [('semmed_analysis','semmed_pyear','analysis_year'),('openalex_semmed','openalex_year','semmed_pyear'),('sciscinet_semmed','sciscinet_year','semmed_pyear'),('sciscinet_openalex','sciscinet_year','openalex_year')]:
                scope='true' if name=='semmed_analysis' else clean
                export(f'SELECT analysis_year,{x}-{y} year_difference,count(DISTINCT pmid) n_pmids FROM audited WHERE {scope} AND {x} IS NOT NULL AND {y} IS NOT NULL GROUP BY ALL ORDER BY 1,2',f'year_difference_{name}.csv')
            export('''SELECT semmed_pyear,dp_year,edat_year,count(*) n_citation_rows,count(DISTINCT pmid) n_pmids FROM citations GROUP BY ALL ORDER BY 1,2,3''','semmed_pyear_dp_edat_comparison.csv')
            export("""SELECT * FROM (SELECT *,row_number() OVER(PARTITION BY match_status,CASE WHEN delta_sciscinet_semmed IS NULL THEN 'missing' WHEN delta_sciscinet_semmed=0 THEN 'zero' WHEN abs(delta_sciscinet_semmed)=1 THEN CAST(delta_sciscinet_semmed AS VARCHAR) ELSE 'larger_difference' END ORDER BY hash(pmid,work_id,analysis_year),pmid,work_id,analysis_year) sample_rank FROM audited WHERE match_status<>'all_equal') WHERE sample_rank<="""+str(args.sample_size),'inspection_sample.csv')
            expected=n('SELECT sum(CASE WHEN n_works_for_pmid IS NULL THEN 1 ELSE n_works_for_pmid END) FROM analysis LEFT JOIN pmid_cardinality USING(pmid)')
            actual=n('SELECT count(*) FROM audited')
            if actual!=expected:
                raise ValueError(f'Join row accounting failed: {actual} != {expected}')
            if n('SELECT count(DISTINCT pmid) FROM audited')!=n('SELECT count(*) FROM cohort'):
                raise ValueError('Join lost cohort PMIDs')
            report={'inputs':{k:str(getattr(args,k)) for k in ['analysis','citations','ids','years','sciscinet']},
              'analysis_input_rows':n('SELECT count(*) FROM analysis_raw'),
              'analysis_duplicate_pmid_year_rows':n('SELECT count(*) FROM analysis_raw')-n('SELECT count(*) FROM analysis'),'analysis_distinct_pmid_years':n('SELECT count(*) FROM analysis'),
              'analysis_pmids':n('SELECT count(*) FROM cohort'),
              'global_pmids_with_multiple_works':n('SELECT count(*) FROM pmid_cardinality WHERE n_works_for_pmid>1'),
              'global_works_with_multiple_pmids':n('SELECT count(*) FROM work_cardinality WHERE n_pmids_for_work>1'),
              'matched_analysis_pmids':n('SELECT count(DISTINCT pmid) FROM audited WHERE work_id IS NOT NULL'),
              'semmed_citation_missing_pmids':n('SELECT count(DISTINCT pmid) FROM audited WHERE semmed_rows IS NULL'),'global_valid_openalex_mappings':n('SELECT count(*) FROM mappings'),
              'global_invalid_mapping_rows':n('SELECT count(*) FROM ids_raw WHERE work_id IS NULL OR pmid IS NULL'),
              'output_rows':actual,'expected_output_rows':expected,'all_checks_passed':True,
              'note':'Conflicts are retained; no date or mapping has been repaired. Aggregate summary rows are not mutually exclusive PMID groups when a PMID has multiple analysis years. See README for scope and assumptions.'}
            (staging/'validation_summary.json').write_text(json.dumps(report,indent=2))
            args.output.mkdir(exist_ok=True)
            for file in staging.iterdir():
                if file.name!='validation_summary.json':file.replace(args.output/file.name)
            (staging/'validation_summary.json').replace(args.output/'validation_summary.json')
            print(json.dumps(report,indent=2),flush=True)


def parser():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--analysis',type=Path,default=Path(os.getenv('IC_AUDIT_ANALYSIS_FILE',str(PROJECT/'data/processed/knowledge_combination/edge_annotation_metrics/pmid_edge_annotation_metrics_1980_2023.parquet'))))
    p.add_argument('--citations',type=Path,default=Path(os.getenv('IC_AUDIT_CITATIONS_FILE','/home/u23/jinyugao/research/data/source/semmedVER43_R/semmedVER43_2024_R_CITATIONS.csv.gz')))
    p.add_argument('--ids',type=Path,default=OA/'openalex_works_ids.csv.gz')
    p.add_argument('--years',type=Path,default=OA/'openalex_works_publication_year.csv.gz')
    p.add_argument('--sciscinet',type=Path,default=Path('/home/u23/jinyugao/research/data/source/sciscinet_v2/sciscinet_papers.parquet'))
    p.add_argument('--output',type=Path,default=Path(os.getenv('IC_PROJECT_ROOT',str(PROJECT)))/'data/processed/data_validation/semmeddb_openalex_sciscinet')
    p.add_argument('--citations-header',action='store_true',help='Original SemMedDB CSV normally has no header')
    p.add_argument('--sample-size',type=int,default=100,help='Inspection rows per status and year-difference stratum')
    return p

if __name__=='__main__':
    args=parser().parse_args()
    if args.sample_size<1:raise ValueError('sample-size must be positive')
    run(args)
