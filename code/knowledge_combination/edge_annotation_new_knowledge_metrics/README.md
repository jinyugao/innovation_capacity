# Edge annotation new knowledge metrics

Compute the same 17 metrics at PMID and year levels. The yearly table uses
cross-PMID distinct concepts, undirected pairs, and exact directed typed triples;
it is NOT the sum or mean of PMID metrics. Existing metrics remain unchanged.

## Inputs

Under IC_PROJECT_ROOT/data/processed/knowledge_combination:

- edge_annotation/edge_annotation_YEAR.parquet
- edge_annotation_metrics/pmid_edge_annotation_metrics_YEAR.parquet

The annotation already retains Boolean subject_seen_in_prior_window and
object_seen_in_prior_window flags. These identify new concepts without rebuilding
the prior window. pair_connected_in_prior_window and
exact_edge_seen_in_prior_window are also required to verify category consistency.
Missing flags cause an explicit failure rather than silently changing definitions.
Previous PMID metrics are mandatory: all six triple counts and five shares must
agree, with share tolerance 1e-9. PMID sets and years must also match.

## Definitions and columns

Concepts pool subject and object IDs, deduplicated within each scope.
Pairs sort the two endpoints and ignore direction and predicate. New Combination
pairs require both endpoints previously observed and no previous connection in
either direction. Pairs involving new concepts count in the denominator but not
in the New Combination numerator. Triples retain subject, predicate and object.
All data follow upstream valid non-self-loop filtering and the five-year window.

Six concept/pair columns:

```text
n_unique_concepts
n_unique_new_concepts
share_unique_new_concepts
n_unique_concept_pairs
n_unique_new_combination_pairs
share_unique_new_combination_pairs
```

Eleven triple columns (same meanings as previous PMID metrics):

```text
n_triples
n_new_node
n_new_combination
n_new_relation
n_repeated_triple
n_new_triples
share_new_node
share_new_combination
share_new_relation
share_repeated_triple
share_new_triples
```

Concept/pair shares divide corresponding subset counts by their unit totals.
All five triple shares divide by n_triples. n_new_triples sums the first three
categories. Counts use BIGINT; shares use DOUBLE fractions without rounding.
PMID outputs have PMID (VARCHAR), PYEAR (INTEGER), then 17 metrics: 19 columns.
Year outputs have PYEAR then 17 metrics: 18 columns. No among-new ratios or
pattern variables are included. Only PMIDs with annotated triples are included.
An empty year produces an empty PMID table and a year row with zero counts and
NULL shares (empty CSV fields); missing input files instead cause failure.

## Outputs

Under IC_PROJECT_ROOT/data/processed/knowledge_combination/edge_annotation_new_knowledge_metrics:

```text
pmid/pmid_edge_annotation_new_knowledge_metrics_YEAR.parquet
pmid/pmid_edge_annotation_new_knowledge_metrics_1980_2023.parquet
year/year_edge_annotation_new_knowledge_metrics_YEAR.csv
year/year_edge_annotation_new_knowledge_metrics_1980_2023.csv
validation/edge_annotation_new_knowledge_metrics_validation_YEAR.csv
validation/edge_annotation_new_knowledge_metrics_year_validation.csv
validation/edge_annotation_new_knowledge_metrics_validation_summary.csv
```

Parquets use ZSTD. The master year CSV has 44 rows. Intermediate concept/pair/
triple tables are temporary DuckDB tables, not permanent additional datasets.

## Run on HPC

Require Python with DuckDB. Sync this code to the HPC repository before running.
Create the log directory before sbatch. Array tasks 0-43 map to 1980-2023.

```bash
cd /xdisk/sebratt/jinyugao/research/repos/innovation_capacity
mkdir -p /xdisk/sebratt/jinyugao/research/projects/innovation_capacity/logs/knowledge_combination/edge_annotation_new_knowledge_metrics
if jid_new_knowledge=$(sbatch --parsable jobs/knowledge_combination/edge_annotation_new_knowledge_metrics/build_edge_annotation_new_knowledge_metrics.slurm); then
    echo "Annual metrics job: $jid_new_knowledge"
    sbatch --dependency=afterok:${jid_new_knowledge} jobs/knowledge_combination/edge_annotation_new_knowledge_metrics/combine_edge_annotation_new_knowledge_metrics.slurm
fi
```

Configuration: IC_PROJECT_ROOT, IC_REPO_ROOT, IC_PYTHON, IC_DUCKDB_THREADS,
IC_DUCKDB_MEMORY_LIMIT. Direct Python execution uses IC_FOCAL_YEAR when not an
array task. Default resources are 4 CPUs, 32 GB RAM and 4 hours; DuckDB uses
24 GB. These are initial estimates, not full-data benchmarks.

To intentionally replace results, set IC_NEW_KNOWLEDGE_METRICS_OVERWRITE=1.
To rerun year 2000 use --array=20. Rerun combine after replacing yearly files.
Default behavior protects existing outputs. A failed replacement may leave an
older master on disk; check current validation and logs, not file existence.

## Validation

Build checks required fields and Boolean historical flags; invalid rows,
self-loops, duplicate PMID triples, category/flag mismatches, and inconsistent
concept/pair/triple annotations cause failure. It verifies 19/18-column schemas,
count identities, ratio definitions, bounds, PMID set equality, and old-metric
agreement. New Combination pair count cannot exceed its triple count.
Build failures write annual reports and exit nonzero.

Combine checks all outputs and successful build reports, rechecks PMID triple
metrics against old data, validates annual ratios/counts, and verifies annual
unique counts lie between per-paper maxima and per-paper sums. It does not
recompute annual uniqueness from raw triples: that is done in the build step.
It verifies global PMID uniqueness and row totals before publishing masters.
The overall report must have passed_years=44, failed_years=0, year_table_rows=44,
cross_year_duplicate_pmid_excess_rows=0 and all_checks_passed=True.

Synthetic tests cover repeated cross-paper concepts/triples, reversed pair
directions, multiple predicates, new concepts, all categories, empty years,
old-metric disagreement, duplicates, inconsistent flags, and missing years.
Full HPC execution has not yet been performed.
