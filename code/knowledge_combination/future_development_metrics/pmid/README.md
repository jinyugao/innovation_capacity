# PMID future development metrics

One row per PMID, 44 columns, covering focal years 1980–2018 with the existing
five-year follow-up definitions. This module aggregates existing results; it does
not reconstruct future-year matches or modify upstream data.

## Inputs and source coverage

Four yearly Parquets under IC_PROJECT_ROOT/data/processed/knowledge_combination:

```text
edge_annotation/edge_annotation_YEAR.parquet
edge_transition/edge_transition_YEAR.parquet
future_development/new_node/future_development_new_node_YEAR.parquet
future_development/new_combination/future_development_new_combination_YEAR.parquet
```

All four are mandatory, including empty category files for years with no such
category. Input joins use PMID + PYEAR + subject_cui_primary + PREDICATE +
object_cui_primary. Annotation is the complete paper universe. Transition must
cover it exactly; each development file must cover precisely its corresponding
annotation category. Missing, extra or duplicate source records cause errors.
A missing expected development row is never silently converted to no development.

Existing historical flags, new-node flags, canonical pair keys, count/seen flags
and transition statuses are checked. Within a year, pair development and exact
triple outcomes must agree across papers. Self-loops and invalid keys are rejected.

## Definitions

nn = focal New Node; nc = New Combination; nr = New Relation.

- nn_future_nc: at least one new endpoint of a focal New Node triple appears
  in future New Combination records.
- nn_future_nr: at least one such new endpoint appears in future New Relation.
- nn_future_any: union of those two paths, not their sum.
- nc_future_nr: the focal New Combination undirected pair has future New Relation.

Future categories retain each future year's own historical-window definition.
A source triple with two new endpoints is positive when either endpoint matches;
its two development paths can involve different endpoints. Development does not
require the original exact triple to recur. Adoption uses that exact recurrence.
Neither flag proves that the source paper caused subsequent discoveries.

All triple counts below count qualifying SOURCE triples, not future triples or
distinct concepts. Pair metrics deduplicate within PMID by canonical undirected
pair. Multiple predicates or directions on the same pair count once there.

## Column groups (44 total)

1. PMID (VARCHAR), PYEAR (INTEGER), n_new_node, n_new_combination,
   n_unique_new_combination_pairs: 5 columns.
2. n_, share_, has_ for each of nn_future_nc, nn_future_nr, nn_future_any,
   nc_future_nr: 12 columns.
3. n_ and share_ for nn_dev_neither, nn_dev_nc_only, nn_dev_nr_only,
   nn_dev_both: 8 columns, a mutually exclusive New Node partition.
4. n_ and share_ for nn_neither_adopted_nor_developed, nn_adopted_only,
   nn_developed_only, nn_adopted_and_developed: 8 columns.
5. n_ and share_ for nc_neither_adopted_nor_developed, nc_adopted_only,
   nc_developed_only, nc_adopted_and_developed: 8 columns.
6. n_nc_pairs_future_nr, share_nc_pairs_future_nr, has_nc_pairs_future_nr:
   3 columns.

Counts are BIGINT. Proportions are DOUBLE fractions in [0,1], not rounded.
All nn proportions divide by n_new_node; nc triple proportions divide by
n_new_combination. The pair proportion divides by n_unique_new_combination_pairs.
Indicators are INTEGER 0/1, equal to the corresponding count >0.

All annotated PMIDs are retained, even when they have neither New Node nor
New Combination. Absent-category counts and indicators are zero; proportions
with zero denominators are NULL. No new-concept-level development rate is claimed.
An empty source year produces an empty 44-column Parquet and a zero-count summary.
The has_nc_pairs_future_nr and has_nc_future_nr indicators must agree.

## Outputs

Under data/processed/knowledge_combination/future_development_metrics/pmid:

```text
pmid_future_development_metrics_YEAR.parquet
pmid_future_development_metrics_1980_2018.parquet
summary/pmid_future_development_metrics_summary_YEAR.csv
summary/pmid_future_development_metrics_summary_by_year.csv
validation/pmid_future_development_metrics_validation_YEAR.csv
validation/pmid_future_development_metrics_year_validation.csv
validation/pmid_future_development_metrics_validation_summary.csv
```

Parquet uses ZSTD. Each annual summary reports source row count, PMID count,
counts of papers with each source category or positive indicator, and sums of
all count columns. These sums are NOT year-unique concept/pair/triple statistics.
The combined summary has 39 rows. No means of undefined shares are computed.

## HPC execution

Requires DuckDB in the existing Python environment. Sync code before running.
Create logs before submission, since Slurm opens them before executing the job.

```bash
cd /xdisk/sebratt/jinyugao/research/repos/innovation_capacity
mkdir -p /xdisk/sebratt/jinyugao/research/projects/innovation_capacity/logs/knowledge_combination/future_development_metrics/pmid
if jid_development=$(sbatch --parsable jobs/knowledge_combination/future_development_metrics/pmid/build_pmid_future_development_metrics.slurm); then
    echo "Annual future development metrics: $jid_development"
    sbatch --dependency=afterok:${jid_development} jobs/knowledge_combination/future_development_metrics/pmid/combine_pmid_future_development_metrics.slurm
fi
```

Array tasks 0–38 correspond to 1980–2018. Direct Python execution uses
IC_FOCAL_YEAR if SLURM_ARRAY_TASK_ID is absent. IC_PROJECT_ROOT, IC_REPO_ROOT,
IC_PYTHON, IC_DUCKDB_THREADS and IC_DUCKDB_MEMORY_LIMIT follow existing conventions.
Initial requests: 4 CPUs, 32 GB RAM (DuckDB 24 GB), 4 hours for each annual job,
8 hours for combine. These are estimates; full-data performance is unmeasured.

Set IC_PMID_FUTURE_DEVELOPMENT_METRICS_OVERWRITE=1 only for intentional replacement.
Use --array=20 for a 2000 retry. Rerun combine after replacing annual results.
Existing outputs are protected by default. A failed replacement may leave an
older master; verify current validation reports and logs, not file existence.

## Validation

Build verifies exact source coverage, keys, category/state consistency, 44-column
schema, denominator bounds, four-way partitions, overlap identities, ratio/NULL
rules and 0/1 indicators. It reconciles all PMID values against source aggregates.

Combine requires all 39 annual tables, summaries and successful build reports.
It rereads all four original inputs, one focal year at a time, to detect stale
or corrupted outputs, compares the summaries, then checks global PMID uniqueness
before publishing the master. Preserve the original inputs until verification.
It validates upstream flags and counts, but does not rederive the original
five-year matching from future annotation data.

Expected final report: passed_years=39, failed_years=0,
annual_row_sum=combined_rows=distinct_pmids,
cross_year_duplicate_pmid_excess_rows=0, all_checks_passed=True.

Local synthetic tests cover all development path and joint outcome states,
multiple directional triples sharing one pair, absent categories, empty years,
missing development records, duplicate sources, inconsistent pair outcomes,
cross-year duplicate PMIDs, corrupted output flags/summaries, and overwrite
protection. HPC results have not yet been produced or validated.
