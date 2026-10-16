# PMID edge annotation metrics pattern

Derive paper-level knowledge-type patterns from the existing annual 16-column
PMID edge annotation metrics. The original tables are not modified.

## Schema and interpretation

Each output Parquet has eight columns:

- PMID: VARCHAR identifier.
- PYEAR: INTEGER publication year.
- has_new_node, has_new_combination, has_new_relation: INTEGER 0/1 flags.
- has_new_triples: INTEGER, one if any of the three types is present.
- n_new_knowledge_types: INTEGER in 0–3.
- knowledge_type_pattern: VARCHAR categorical label.

Fixed display order: Repeated_Only, N, C, R, N+C, N+R, C+R, N+C+R.
N/C/R indicate presence of New Node/New Combination/New Relation respectively.
Every pattern other than Repeated_Only can also contain repeated triples.
Repeated_Only means zero new triples and at least one repeated triple.
Newness retains the upstream five-year reference-window definition. Pattern
labels are nominal categories, not ordinal scores. Papers without annotated
triples are absent upstream and remain absent here.

## Inputs and outputs

Under IC_PROJECT_ROOT/data/processed/knowledge_combination:

Input: edge_annotation_metrics/pmid_edge_annotation_metrics_YEAR.parquet.

Outputs under edge_annotation_metrics_pattern/:

- pmid_edge_annotation_metrics_pattern_YEAR.parquet
- pmid_edge_annotation_metrics_pattern_1980_2023.parquet
- summary/pmid_edge_annotation_metrics_pattern_summary_YEAR.csv
- summary/pmid_edge_annotation_metrics_pattern_summary_by_year.csv
- validation/pmid_edge_annotation_metrics_pattern_year_validation.csv
- validation/pmid_edge_annotation_metrics_pattern_validation_summary.csv

Annual summaries always contain eight rows with PYEAR, knowledge_type_pattern,
n_pmids, n_pmids_year, share_pmids, mean_n_triples, median_n_triples.
Absent categories have count/share zero and empty mean/median. An entirely empty
year has empty shares as well because the denominator is zero. Summaries use
paper counts, not triple-weighted counts. The full summary has 352 rows.
Parquets use ZSTD and preserve one row per PMID.

## Run on HPC

Prerequisite: all annual PMID metrics for 1980–2023 exist. Both build and combine
require DuckDB and retain access to these upstream files. Combine rechecks annual
patterns against upstream counts and recomputes summaries for verification.

```bash
cd /xdisk/sebratt/jinyugao/research/repos/innovation_capacity
mkdir -p /xdisk/sebratt/jinyugao/research/projects/innovation_capacity/logs/knowledge_combination/edge_annotation_metrics_pattern
if jid_pattern=$(sbatch --parsable jobs/knowledge_combination/edge_annotation_metrics_pattern/build_pmid_edge_annotation_metrics_pattern.slurm); then
    echo "Pattern array job: $jid_pattern"
    sbatch --dependency=afterok:${jid_pattern} jobs/knowledge_combination/edge_annotation_metrics_pattern/combine_pmid_edge_annotation_metrics_pattern.slurm
fi
```

Create the log directory BEFORE sbatch, since Slurm opens logs before executing
the job script. Array 0–43 maps to 1980–2023. Alternatively run the combine job
manually after all annual jobs succeed. To retry year 2000 use --array=20.
For direct Python runs set IC_FOCAL_YEAR instead of SLURM_ARRAY_TASK_ID.

IC_PROJECT_ROOT, IC_REPO_ROOT, IC_PYTHON, IC_DUCKDB_THREADS and
IC_DUCKDB_MEMORY_LIMIT follow the existing pipeline. Initial resource requests
are 4 CPUs, 16 GB, 2 hours per job, with DuckDB limited to 12 GB; adjust after
measuring actual HPC usage.

Existing files are protected by default. For intentional reruns set
IC_EDGE_ANNOTATION_METRICS_PATTERN_OVERWRITE=1. A failed replacement validation
leaves an older master intact; inspect the latest report/log, not only file
existence. After replacing annual files, rerun combine.

## Validation

Annual checks reject missing or invalid counts, incorrect count sums, invalid
years/PMIDs, and duplicate PMIDs. Output schema, flags, pattern labels, and PMID
sets are checked against the source. Summary mean/median use n_triples from the
source joined by PMID. Combine verifies every summary value, all annual inputs,
and global PMID uniqueness before publishing the master. Missing years or bad
results produce failure reports and a nonzero exit status. All-checks-passed
must be True; passed_years must be 44 and cross-year duplicate excess must be 0.

Local synthetic verification covered all eight modes, zero-count categories,
empty years, correct summaries, overwrite protection, missing inputs, invalid
counts, duplicate PMIDs, cross-year duplicates, and corrupted outputs/summaries.
Actual HPC performance and results have not yet been verified.
