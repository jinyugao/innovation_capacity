"""Validate and combine 39 annual PMID future_development tables; no year-unique metrics."""
from __future__ import annotations

import os
from pathlib import Path
import tempfile
from build_pmid_future_development_metrics import (
    ROOT, SUBDIR, STEM, q, connect, guard, write_csv, read_one, output_paths,
    prepare_source, aggregate, validate, compare_source, summarize,
)


def combine(root, years=range(1980, 2019)):
    years = list(years)
    if not years or years != sorted(set(years)) or any(y < 1980 or y > 2018 for y in years):
        raise ValueError('Expected sorted unique years within 1980-2018')
    base = root / SUBDIR
    target = base / f'{STEM}_{years[0]}_{years[-1]}.parquet'
    summary_path = base / 'summary' / f'{STEM}_summary_by_year.csv'
    annual_report = base / 'validation' / f'{STEM}_year_validation.csv'
    final_report = base / 'validation' / f'{STEM}_validation_summary.csv'
    guard([target, summary_path, annual_report, final_report])
    rows, summaries, files = [], [], []
    with tempfile.TemporaryDirectory(prefix='combine_future_development_', dir=base) as temp:
        for year in years:
            path, summary, validation = output_paths(root, year)
            row = {'PYEAR': year, 'metrics_file_exists': path.is_file(), 'summary_file_exists': summary.is_file(), 'validation_file_exists': validation.is_file(), 'source_valid': False, 'metrics_valid': False, 'source_output_match': False, 'summary_matches': False, 'n_pmids': '', 'all_checks_passed': False, 'error': ''}
            con = connect(temp)
            try:
                if not all(p.is_file() for p in [path, summary, validation]):
                    raise FileNotFoundError('Missing annual output or validation file')
                previous = read_one(validation)
                if previous['all_checks_passed'] != 'True' or int(previous['PYEAR']) != year:
                    raise ValueError('Annual build validation did not pass')
                n_input = prepare_source(con, root, year)
                row['source_valid'] = True
                aggregate(con)
                con.execute(f'CREATE TEMP VIEW metrics AS SELECT * FROM read_parquet({q(path)})')
                validate(con, year); row['metrics_valid'] = True
                compare_source(con); row['source_output_match'] = True
                expected = summarize(con, year, n_input)
                observed = read_one(summary)
                row['summary_matches'] = list(observed) == list(expected) and all(int(observed[k]) == v for k, v in expected.items())
                if not row['summary_matches']:
                    raise ValueError('Summary differs from source and PMID output')
                if int(previous['n_pmids']) != expected['n_pmids'] or int(previous['n_input_triples']) != n_input:
                    raise ValueError('Annual validation counts differ from current source')
                row.update(n_pmids=expected['n_pmids'], all_checks_passed=True)
                files.append(path); summaries.append(expected)
            except Exception as exc:
                row['error'] = str(exc)
            finally:
                con.close()
            rows.append(row)
        write_csv(annual_report, rows)
        passed = sum(r['all_checks_passed'] for r in rows)
        result = {'start_year': years[0], 'end_year': years[-1], 'expected_years': len(years), 'passed_years': passed, 'failed_years': len(years)-passed, 'annual_row_sum': sum(r['n_pmids'] for r in rows if isinstance(r['n_pmids'], int)), 'combined_rows': '', 'distinct_pmids': '', 'cross_year_duplicate_pmid_excess_rows': '', 'all_checks_passed': False, 'error': ''}
        con = connect(temp)
        try:
            if passed != len(years):
                raise ValueError('Annual validation failed; see year validation CSV')
            inputs = '[' + ','.join(map(q, files)) + ']'
            con.execute(f'CREATE TEMP VIEW combined AS SELECT * FROM read_parquet({inputs})')
            count, distinct = con.execute('SELECT COUNT(*), COUNT(DISTINCT PMID) FROM combined').fetchone()
            result.update(combined_rows=count, distinct_pmids=distinct, cross_year_duplicate_pmid_excess_rows=count-distinct)
            if count != distinct or count != result['annual_row_sum']:
                raise ValueError('Global PMID uniqueness or row accounting failed')
            staged = Path(temp) / 'master.parquet'
            con.execute(f'COPY (SELECT * FROM combined ORDER BY PYEAR,PMID) TO {q(staged)} (FORMAT PARQUET, COMPRESSION ZSTD)')
            write_csv(summary_path, summaries)
            staged.replace(target)
            result['all_checks_passed'] = True
        except Exception as exc:
            result['error'] = str(exc)
            raise
        finally:
            write_csv(final_report, [result])
            con.close()
    print(f'Saved {target}; {result}')


if __name__ == '__main__':
    combine(Path(os.getenv('IC_PROJECT_ROOT', str(ROOT))))
