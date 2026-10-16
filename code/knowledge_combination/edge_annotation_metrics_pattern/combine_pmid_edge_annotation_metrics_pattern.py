"""Validate and combine annual PMID edge-annotation pattern tables."""
from __future__ import annotations

import os
from pathlib import Path
import tempfile

from build_pmid_edge_annotation_metrics_pattern import (
    ROOT, SUBDIR, STEM, quote, connect, guard, write_csv,
    prepare_source, validate, checks_pass, summarize, summary_matches,
)


def combine(root, years=range(1980, 2024)):
    years = list(years)
    if not years or years != sorted(set(years)):
        raise ValueError('Years must be a nonempty, sorted, unique sequence')
    base = root / SUBDIR
    target = base / f'{STEM}_{years[0]}_{years[-1]}.parquet'
    annual_report = base / 'validation' / f'{STEM}_year_validation.csv'
    final_report = base / 'validation' / f'{STEM}_validation_summary.csv'
    combined_summary = base / 'summary' / f'{STEM}_summary_by_year.csv'
    guard([target, annual_report, final_report, combined_summary])
    rows, summaries, files = [], [], []
    with tempfile.TemporaryDirectory(prefix='combine_patterns_', dir=base) as temp:
        con = connect(temp)
        try:
            for year in years:
                path = base / f'{STEM}_{year}.parquet'
                report = base / 'summary' / f'{STEM}_summary_{year}.csv'
                row = {'PYEAR': year, 'pattern_file_exists': path.is_file(), 'summary_file_exists': report.is_file(), 'schema_correct': False, 'invalid_rows': '', 'duplicate_pmid_excess_rows': '', 'input_output_mismatch_rows': '', 'summary_matches': False, 'n_pmids': '', 'all_checks_passed': False, 'error': ''}
                try:
                    if not path.is_file() or not report.is_file():
                        raise FileNotFoundError('Missing annual pattern table or summary')
                    prepare_source(con, root, year)
                    con.execute(f'CREATE OR REPLACE TEMP VIEW patterns AS SELECT * FROM read_parquet({quote(path)})')
                    checks = validate(con, year)
                    row.update(checks)
                    if not checks_pass(checks):
                        raise ValueError('Pattern validation failed')
                    expected = summarize(con, year)
                    row['summary_matches'] = summary_matches(report, expected)
                    row['n_pmids'] = sum(r['n_pmids'] for r in expected)
                    if not row['summary_matches']:
                        raise ValueError('Summary differs from pattern table and source counts')
                    summaries.extend(expected)
                    files.append(path)
                    row['all_checks_passed'] = True
                except Exception as exc:
                    row['error'] = str(exc)
                rows.append(row)
            write_csv(annual_report, rows)
            passed = sum(r['all_checks_passed'] for r in rows)
            result = {'start_year': years[0], 'end_year': years[-1], 'expected_years': len(years), 'found_pattern_files': sum(r['pattern_file_exists'] for r in rows), 'found_summary_files': sum(r['summary_file_exists'] for r in rows), 'passed_years': passed, 'failed_years': len(years)-passed, 'annual_row_sum': sum(r['n_pmids'] for r in rows if isinstance(r['n_pmids'], int)), 'combined_rows': '', 'distinct_pmids': '', 'cross_year_duplicate_pmid_excess_rows': '', 'expected_summary_rows': len(years)*8, 'combined_summary_rows': len(summaries), 'all_checks_passed': False, 'error': ''}
            try:
                if passed != len(years):
                    raise ValueError('Annual validation failed; see year validation CSV')
                paths = '[' + ','.join(map(quote, files)) + ']'
                con.execute(f'CREATE TEMP VIEW combined AS SELECT * FROM read_parquet({paths})')
                count, distinct = con.execute('SELECT COUNT(*), COUNT(DISTINCT PMID) FROM combined').fetchone()
                result.update(combined_rows=count, distinct_pmids=distinct, cross_year_duplicate_pmid_excess_rows=count-distinct)
                if count != result['annual_row_sum'] or count != distinct or len(summaries) != len(years)*8:
                    raise ValueError('Combined row accounting or global PMID uniqueness failed')
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
