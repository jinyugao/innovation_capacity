"""Validate annual PMID metrics, then concatenate all 44 years."""
from __future__ import annotations
import csv
import os
from pathlib import Path
import tempfile
from build_pmid_edge_annotation_metrics import ROOT, SUBDIR, quote, scalar, connect, guard, write_csv, summary, validate, checks_pass

def combine(root, years=range(1980, 2024)):
    years = list(years)
    base = root / SUBDIR
    target = base / f'pmid_edge_annotation_metrics_{years[0]}_{years[-1]}.parquet'
    annual_report = base / 'validation/pmid_edge_annotation_metrics_year_validation.csv'
    final_report = base / 'validation/pmid_edge_annotation_metrics_validation_summary.csv'
    combined_summary = base / 'summary/pmid_edge_annotation_metrics_summary_by_year.csv'
    guard([target, annual_report, final_report, combined_summary])
    rows, summaries, files = [], [], []
    with tempfile.TemporaryDirectory(prefix='combine_pmid_metrics_', dir=base) as temp:
        con = connect(temp)
        try:
            for year in years:
                source = base / f'pmid_edge_annotation_metrics_{year}.parquet'
                report = base / 'summary' / f'pmid_edge_annotation_metrics_summary_{year}.csv'
                row = {'PYEAR': year, 'metrics_file_exists': source.is_file(), 'summary_file_exists': report.is_file(), 'schema_correct': False, 'invalid_year_rows': '', 'invalid_pmid_rows': '', 'invalid_count_rows': '', 'invalid_main_share_rows': '', 'invalid_among_new_share_rows': '', 'duplicate_pmid_excess_rows': '', 'summary_matches': False, 'n_pmids': '', 'all_checks_passed': False, 'error': ''}
                try:
                    if not source.is_file() or not report.is_file():
                        raise ValueError('Missing annual metrics or summary file')
                    con.execute(f'CREATE OR REPLACE TEMP VIEW metrics AS SELECT * FROM read_parquet({quote(source)})')
                    checks = validate(con, year)
                    row.update(checks)
                    if not checks_pass(checks):
                        raise ValueError('Annual metric checks failed')
                    with report.open(newline='', encoding='utf-8') as handle:
                        records = list(csv.DictReader(handle))
                    if len(records) != 1:
                        raise ValueError('Expected one summary row')
                    expected = summary(con, year, scalar(con, 'SELECT COALESCE(SUM(n_triples),0) FROM metrics'))
                    row['summary_matches'] = all(int(records[0][key]) == value for key,value in expected.items())
                    row['n_pmids'] = expected['n_pmids']
                    if not row['summary_matches']:
                        raise ValueError('Annual summary differs from metrics')
                    summaries.append(expected)
                    files.append(source)
                    row['all_checks_passed'] = True
                except Exception as exc:
                    row['error'] = str(exc)
                rows.append(row)
            write_csv(annual_report, rows)
            passed = sum(r['all_checks_passed'] for r in rows)
            result = {'start_year': years[0], 'end_year': years[-1], 'expected_years': len(years), 'found_metrics_files': sum(r['metrics_file_exists'] for r in rows), 'found_summary_files': sum(r['summary_file_exists'] for r in rows), 'passed_years': passed, 'failed_years': len(years)-passed, 'annual_row_sum': sum(r['n_pmids'] for r in rows if isinstance(r['n_pmids'],int)), 'combined_rows': '', 'distinct_pmids': '', 'cross_year_duplicate_pmid_excess_rows': '', 'all_checks_passed': False, 'error': ''}
            try:
                if passed != len(years):
                    raise ValueError('Annual validation failed; see year validation CSV')
                paths = '[' + ','.join(map(quote, files)) + ']'
                con.execute(f'CREATE TEMP VIEW combined AS SELECT * FROM read_parquet({paths})')
                count, distinct = con.execute('SELECT COUNT(*), COUNT(DISTINCT PMID) FROM combined').fetchone()
                result.update(combined_rows=count, distinct_pmids=distinct, cross_year_duplicate_pmid_excess_rows=count-distinct)
                if count != result['annual_row_sum'] or count != distinct:
                    raise ValueError('Combined row count mismatch or cross-year duplicate PMIDs')
                staged = Path(temp) / 'combined.parquet'
                con.execute(f'COPY (SELECT * FROM combined ORDER BY PYEAR, PMID) TO {quote(staged)} (FORMAT PARQUET, COMPRESSION ZSTD)')
                staged.replace(target)
                write_csv(combined_summary, summaries)
                result['all_checks_passed'] = True
            except Exception as exc:
                result['error'] = str(exc)
                raise
            finally:
                write_csv(final_report, [result])
            print(f'Saved {target}; {result}')
        finally:
            con.close()

if __name__ == '__main__':
    combine(Path(os.getenv('IC_PROJECT_ROOT', str(ROOT))))
