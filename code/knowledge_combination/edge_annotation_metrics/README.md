# PMID edge annotation metrics

Two-stage pipeline using Python and DuckDB. Input annotations are already
PMID-exact-triple deduplicated and exclude self-loops. Each annual job verifies
these constraints before producing one row per PMID with 16 columns.

## Run on HPC

Sync the code and jobs to the repository first. Create the log directory before
submission because Slurm opens logs before executing the script.

```bash
cd /xdisk/sebratt/jinyugao/research/repos/innovation_capacity
mkdir -p /xdisk/sebratt/jinyugao/research/projects/innovation_capacity/logs/knowledge_combination/edge_annotation_metrics
sbatch jobs/knowledge_combination/edge_annotation_metrics/build_pmid_edge_annotation_metrics.slurm
```

The array has tasks 0–43, mapping to years 1980–2023. After all tasks succeed:

```bash
sbatch jobs/knowledge_combination/edge_annotation_metrics/combine_pmid_edge_annotation_metrics.slurm
```

Alternatively submit the combine job with `--dependency=afterok:JOB_ID`, replacing
JOB_ID with the array job ID. To rerun one year, use `sbatch --array=INDEX` with
INDEX equal to year minus 1980. Output replacement is disabled by default;
explicitly export `IC_EDGE_ANNOTATION_METRICS_OVERWRITE=1` when replacing results.
Existing successful outputs remain intact if a replacement run fails validation;
consult the latest job status and validation reports before using them.

For direct execution, set IC_FOCAL_YEAR instead of SLURM_ARRAY_TASK_ID.
IC_PROJECT_ROOT, IC_REPO_ROOT, IC_PYTHON, IC_DUCKDB_THREADS, and
IC_DUCKDB_MEMORY_LIMIT follow existing project conventions. Default resources
are 4 CPUs, 16 GB RAM, and 2 hours; tune after observing real HPC usage.

## Files

Under IC_PROJECT_ROOT/data/processed/knowledge_combination:

- Input: edge_annotation/edge_annotation_YEAR.parquet
- Annual output: edge_annotation_metrics/pmid_edge_annotation_metrics_YEAR.parquet
- Master: edge_annotation_metrics/pmid_edge_annotation_metrics_1980_2023.parquet
- Annual CSV: edge_annotation_metrics/summary/pmid_edge_annotation_metrics_summary_YEAR.csv
- Combined CSV: edge_annotation_metrics/summary/pmid_edge_annotation_metrics_summary_by_year.csv
- Checks: edge_annotation_metrics/validation/pmid_edge_annotation_metrics_year_validation.csv
- Overall: edge_annotation_metrics/validation/pmid_edge_annotation_metrics_validation_summary.csv

The master is written only after all 44 annual files and summaries validate,
and global PMID uniqueness is verified. Annual and master Parquets use ZSTD.
The combine stage concatenates paper rows; it does not aggregate papers together.

## Columns

PMID (string), PYEAR (integer), then six integer counts:
n_triples, n_new_node, n_new_combination, n_new_relation,
n_repeated_triple, n_new_triples.

Five double shares divide the corresponding counts by n_triples:
share_new_node, share_new_combination, share_new_relation,
share_repeated_triple, share_new_triples.

Three nullable double shares divide each new category count by n_new_triples:
share_new_node_among_new, share_new_combination_among_new,
share_new_relation_among_new. These are NULL when n_new_triples is zero.
All shares are fractions in [0,1], without rounding. Count variables count
classified triples, not distinct concepts. Only PMIDs with annotated triples
enter the table. Newness remains relative to the upstream five-year window.

Annual summaries report PMID totals (with/without new triples), input row counts,
and summed category counts. Validation reports check schema, year, identifiers,
count identities, ratios, NULL rules, summary consistency, and global uniqueness.
Floating-point comparisons use tolerance 1e-9. Input errors stop annual jobs;
combine failures write validation reports and exit nonzero.
