"""Retain all SciSciNet records whose OpenAlex work has a valid PMID."""
from __future__ import annotations

import csv
import os
from pathlib import Path
import tempfile

import duckdb

ROOT = Path('/xdisk/sebratt/jinyugao/research/projects/innovation_capacity')
SOURCE = Path('/home/u23/jinyugao/research/data/source/sciscinet_v2/sciscinet_papers.parquet')
IDS = Path('/xdisk/sebratt/jinyugao/research/data/processed/openalex/flattened_snapshot_2025/openalex_works_ids.csv.gz')


def q(value):
    return "'" + str(value).replace("'", "''") + "'"


def count(con, sql):
    return con.execute(sql).fetchone()[0]


def build(root=ROOT, source=SOURCE, ids=IDS, overwrite=False):
    root, source, ids = map(Path, (root, source, ids))
    for path in (source, ids):
        if not path.is_file():
            raise FileNotFoundError(path)
    outdir = root / 'data/processed/sciscinet_v2'
    output = outdir / 'sciscinet_papers_pmid.parquet'
    summary = outdir / 'sciscinet_papers_pmid_summary.csv'
    rejected = outdir / 'sciscinet_papers_pmid_rejected_mappings.csv'
    outdir.mkdir(parents=True, exist_ok=True)
    if not overwrite and any(p.exists() for p in (output, summary)):
        raise FileExistsError('Output exists; set IC_SCISCINET_PMID_OVERWRITE=1 to replace')
    with tempfile.TemporaryDirectory(prefix='sciscinet_pmid_', dir=outdir) as temp:
        con = duckdb.connect()
        try:
            con.execute(f'SET temp_directory={q(temp)}')
            con.execute(f"SET threads={int(os.getenv('IC_DUCKDB_THREADS', '8'))}")
            con.execute(f"SET memory_limit={q(os.getenv('IC_DUCKDB_MEMORY_LIMIT', '48GB'))}")
            # Explicit CSV quoting protects correctly quoted DOIs containing commas.
            con.execute(f'''CREATE TEMP TABLE candidates AS SELECT DISTINCT
                trim(work_id) raw_work_id, trim(pmid) raw_pmid,
                regexp_extract(trim(work_id), '^(?:https?://openalex[.]org/)?(W[0-9]+)/?$', 1) paperid,
                regexp_extract(trim(pmid), '^(?:https?://(?:www[.])?ncbi[.]nlm[.]nih[.]gov/pubmed/|https?://pubmed[.]ncbi[.]nlm[.]nih[.]gov/)?([0-9]+)/?$', 1) PMID
                FROM read_csv({q(ids)}, header=true, all_varchar=true,
                    delim=',', quote='"', escape='"', nullstr='', compression='gzip')
                WHERE pmid IS NOT NULL AND trim(pmid)<>'' ''')
            con.execute("""CREATE TEMP TABLE checked AS SELECT *, concat_ws(';',
                CASE WHEN paperid IS NULL OR paperid='' THEN 'invalid_work_id' END,
                CASE WHEN PMID IS NULL OR PMID='' OR NOT regexp_matches(PMID, '[1-9]')
                    THEN 'invalid_pmid' END) rejection_reason FROM candidates""")
            bad = count(con, "SELECT COUNT(*) FROM checked WHERE rejection_reason<>''")
            # Publish diagnostics even if later conflict checks stop the job.
            staged_rejected = Path(temp) / rejected.name
            con.execute(f"COPY (SELECT * FROM checked WHERE rejection_reason<>'') TO {q(staged_rejected)} (FORMAT CSV, HEADER TRUE)")
            staged_rejected.replace(rejected)
            print(f'Skipping {bad:,} invalid distinct mappings; diagnostics: {rejected}', flush=True)
            con.execute("CREATE TEMP TABLE mapping AS SELECT DISTINCT paperid, PMID FROM checked WHERE rejection_reason=''")
            conflicts = count(con, 'SELECT COUNT(*) FROM (SELECT paperid FROM mapping GROUP BY paperid HAVING COUNT(*)>1)')
            if conflicts:
                raise ValueError(f'{conflicts} work IDs map to multiple PMIDs; resolve before joining')
            mapped = count(con, 'SELECT COUNT(*) FROM mapping')
            if not mapped:
                raise ValueError('No valid PMID-linked works')
            multi = count(con, 'SELECT COUNT(*) FROM (SELECT PMID FROM mapping GROUP BY PMID HAVING COUNT(*)>1)')
            con.execute(f'CREATE TEMP VIEW sci AS SELECT * FROM read_parquet({q(source)})')
            names = [r[0] for r in con.execute('DESCRIBE sci').fetchall()]
            if 'paperid' not in names:
                raise ValueError('SciSciNet input lacks paperid')
            if any(n.lower() == 'pmid' for n in names):
                raise ValueError('SciSciNet already contains a PMID column; inspect before appending')
            staged = Path(temp) / output.name
            # Mapping is unique by paperid. Keep every source record, including
            # multiple field assignments; never silently collapse to one paper row.
            con.execute(f'''COPY (SELECT S.*, M.PMID FROM sci S JOIN mapping M
                ON trim(CAST(S.paperid AS VARCHAR))=M.paperid)
                TO {q(staged)} (FORMAT PARQUET, COMPRESSION ZSTD)''')
            con.execute(f'CREATE TEMP VIEW result AS SELECT * FROM read_parquet({q(staged)})')
            nrows, nworks, npmids, invalid = con.execute('''SELECT COUNT(*), COUNT(DISTINCT paperid),
                COUNT(DISTINCT PMID), COUNT(*) FILTER (WHERE PMID IS NULL OR trim(PMID)='') FROM result''').fetchone()
            if not nrows or invalid:
                raise ValueError(f'Invalid/empty result: rows={nrows}, invalid PMID rows={invalid}')
            report = dict(openalex_ids_file=str(ids), sciscinet_source_file=str(source),
                output_file=str(output), rejected_mappings_file=str(rejected),
                n_invalid_distinct_mappings_skipped=bad, n_mapped_works=mapped,
                n_mapped_pmids=count(con, 'SELECT COUNT(DISTINCT PMID) FROM mapping'),
                n_pmids_with_multiple_work_ids=multi, n_output_records=nrows,
                n_matched_works=nworks, n_unmatched_mapped_works=mapped-nworks,
                work_match_rate=nworks/mapped, n_output_pmids=npmids,
                n_extra_records_beyond_unique_works=nrows-nworks,
                all_checks_passed=True)
            staged_summary = Path(temp) / summary.name
            with staged_summary.open('w', newline='') as handle:
                writer = csv.DictWriter(handle, fieldnames=list(report))
                writer.writeheader()
                writer.writerow(report)
            staged.replace(output)
            staged_summary.replace(summary)
            print(report, flush=True)
            return report
        finally:
            con.close()


if __name__ == '__main__':
    build(Path(os.getenv('IC_PROJECT_ROOT', str(ROOT))),
          Path(os.getenv('IC_SCISCINET_PAPERS_FILE', str(SOURCE))),
          Path(os.getenv('IC_OPENALEX_IDS_FILE', str(IDS))),
          os.getenv('IC_SCISCINET_PMID_OVERWRITE', '0').lower() in ('1', 'true', 'yes'))
