# PMID-linked SciSciNet v2 subset

`build_sciscinet_papers_pmid.py` reads the OpenAlex work-ID CSV, selects nonempty
PMIDs, removes exact duplicate mappings, and normalizes work URLs to `W...` and
PMID URLs to numeric strings. Invalid nonempty IDs are skipped and exported with raw values and rejection
reasons. Work IDs with conflicting valid PMIDs still stop execution. A PMID associated with multiple work IDs is retained and
reported. Explicit CSV quoting handles DOIs containing commas.

The script joins this unique-work mapping to SciSciNet `paperid`, retaining
all matching source records and all original columns, with a new `PMID` string
column. Multiple source records per paper are preserved (for example, field
assignments). This is not yet a one-row-per-PMID regression table. There is no
year restriction or restriction to the project's SemMedDB analysis sample.

Defaults:

- SciSciNet: `/home/u23/jinyugao/research/data/source/sciscinet_v2/sciscinet_papers.parquet`
- OpenAlex: `/xdisk/sebratt/jinyugao/research/data/processed/openalex/flattened_snapshot_2025/openalex_works_ids.csv.gz`
- Project: `/xdisk/sebratt/jinyugao/research/projects/innovation_capacity`

Outputs under `data/processed/sciscinet_v2/`:

- `sciscinet_papers_pmid.parquet`
- `sciscinet_papers_pmid_rejected_mappings.csv`: invalid distinct mappings and
  reasons, written even if a later check fails. Refreshed on each attempted run.
- `sciscinet_papers_pmid_summary.csv`: skipped-invalid count, mapped/matched/unmatched works, match
  rate, PMID counts, multiple-work PMID count, output record counts and checks.

Dependency: DuckDB. Optional environment overrides: `IC_PROJECT_ROOT`,
`IC_SCISCINET_PAPERS_FILE`, `IC_OPENALEX_IDS_FILE`, `IC_DUCKDB_THREADS`,
`IC_DUCKDB_MEMORY_LIMIT`, `IC_SCISCINET_PMID_OVERWRITE=1`.
Existing outputs are protected by default. Failed data checks do not replace an
existing output. Temporary DuckDB files reside under the project output folder.

Create the log directory BEFORE submission because Slurm opens logs before the
job script runs:

```bash
cd /xdisk/sebratt/jinyugao/research/repos/innovation_capacity
mkdir -p /xdisk/sebratt/jinyugao/research/projects/innovation_capacity/logs/sciscinet_v2
sbatch jobs/sciscinet_v2/build_sciscinet_papers_pmid.slurm
```

Initial resources: 8 CPUs, 64 GB RAM (DuckDB 48 GB), 24 hours; tune from actual
usage. Inspect both logs and the summary after completion. The source need only
be scanned once to write matching records; output diagnostics scan the subset.
