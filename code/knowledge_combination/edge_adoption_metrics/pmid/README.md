# PMID edge adoption metrics

Aggregate existing exact-triple transitions to one row per PMID. Focal years are
1980–2018, each with the upstream five-year follow-up window. No year-unique or
country metrics are calculated. Summary totals are paper-level occurrence sums.

## Input and definitions

Input under IC_PROJECT_ROOT:

```text
data/processed/knowledge_combination/edge_transition/edge_transition_YEAR.parquet
```

Required fields: PMID, PYEAR, subject_cui_primary, PREDICATE,
object_cui_primary, edge_annotation, future_exact_triple_seen (Boolean),
future_five_year_transition.

A new-category triple is Adopted if the identical directed, typed triple appears
in the following five years; a Repeated Triple is Continued under that condition.
Otherwise its status is Disappeared. These are triple reappearance indicators,
not evidence of paper citation or direct transmission from a specific paper.
The script trusts upstream window construction, while validating internal state
consistency. No future annotation inputs are reread to reconstruct that window.

Input must already be PMID-exact-triple unique and exclude self-loops. Invalid
keys, wrong years, unknown categories/transitions, inconsistent seen/status
flags, duplicates, or disagreement across PMIDs on the same year's exact triple
cause failure rather than silent repair. Empty annual inputs retain the schema.

## Output: 27 columns

PMID is VARCHAR; PYEAR is INTEGER. Twelve BIGINT count columns:

```text
n_triples
n_new_node
n_new_combination
n_new_relation
n_repeated_triple
n_new_triples
n_new_node_adopted
n_new_combination_adopted
n_new_relation_adopted
n_new_triples_adopted
n_repeated_triple_continued
n_triples_reappeared
```

Seven DOUBLE proportions:

| Column | Numerator | Denominator |
|---|---|---|
| share_new_node_adopted | n_new_node_adopted | n_new_node |
| share_new_combination_adopted | n_new_combination_adopted | n_new_combination |
| share_new_relation_adopted | n_new_relation_adopted | n_new_relation |
| share_new_triples_adopted | n_new_triples_adopted | n_new_triples |
| share_repeated_triple_continued | n_repeated_triple_continued | n_repeated_triple |
| share_triples_reappeared | n_triples_reappeared | n_triples |
| share_adopted_new_triples_in_all | n_new_triples_adopted | n_triples |

Proportions are unrounded fractions in [0,1]. Zero category denominators produce
NULL, not zero. Papers without annotated triples are absent, so n_triples is
positive in every output row. A paper with no new triples has NULL
share_new_triples_adopted but zero share_adopted_new_triples_in_all.

Six INTEGER indicators equal one iff their corresponding outcome count is >0:

```text
has_new_node_adopted
has_new_combination_adopted
has_new_relation_adopted
has_new_triples_adopted
has_repeated_triple_continued
has_triples_reappeared
```

Absent categories have indicator 0. For conditional adoption regressions,
restrict to papers with positive corresponding category count. Indicators mean
at least one triple, not all triples or the entire paper.

## Files

Under data/processed/knowledge_combination/edge_adoption_metrics/pmid:

```text
pmid_edge_adoption_metrics_YEAR.parquet
pmid_edge_adoption_metrics_1980_2018.parquet
summary/pmid_edge_adoption_metrics_summary_YEAR.csv
summary/pmid_edge_adoption_metrics_summary_by_year.csv
validation/pmid_edge_adoption_metrics_validation_YEAR.csv
validation/pmid_edge_adoption_metrics_year_validation.csv
validation/pmid_edge_adoption_metrics_validation_summary.csv
```

Parquets use ZSTD. Annual summaries contain PYEAR, n_input_triples, n_pmids,
number of papers satisfying each of the six indicators, and sums of the twelve
count columns. They are not annual distinct-triple statistics. Full summaries
have 39 rows. Failure reports include an error message.

## HPC execution

Requires Python with DuckDB. Sync code before running. Create the log directory
before sbatch because Slurm opens logs before executing the script.

```bash
cd /xdisk/sebratt/jinyugao/research/repos/innovation_capacity
mkdir -p /xdisk/sebratt/jinyugao/research/projects/innovation_capacity/logs/knowledge_combination/edge_adoption_metrics/pmid
if jid_adoption=$(sbatch --parsable jobs/knowledge_combination/edge_adoption_metrics/pmid/build_pmid_edge_adoption_metrics.slurm); then
    echo "Annual adoption metrics: $jid_adoption"
    sbatch --dependency=afterok:${jid_adoption} jobs/knowledge_combination/edge_adoption_metrics/pmid/combine_pmid_edge_adoption_metrics.slurm
fi
```

Array tasks 0–38 map to 1980–2018. Direct Python execution uses IC_FOCAL_YEAR
when SLURM_ARRAY_TASK_ID is absent. IC_PROJECT_ROOT, IC_REPO_ROOT, IC_PYTHON,
IC_DUCKDB_THREADS and IC_DUCKDB_MEMORY_LIMIT follow existing conventions.
Initial resources are 4 CPUs and 16 GB (DuckDB 12 GB); annual jobs request 2 hours,
combine requests 8 hours because it rereads and verifies all transition inputs.
These are estimates; actual HPC resource use has not been measured.

Default behavior refuses to overwrite outputs. For intentional replacements set
IC_PMID_EDGE_ADOPTION_METRICS_OVERWRITE=1. A single-year retry for 2000 uses
--array=20. After replacing annual outputs, rerun combine. An older master may
remain following failed replacement; inspect current logs and validation reports.

## Validation

Annual jobs validate source data, exact 27-column schema, count identities,
outcome bounds, proportion/NULL rules, and 0/1 indicators. They reconcile PMID
sets and all output values with source-derived aggregates before publication.

Combine requires all 39 annual files, summaries and successful validation reports,
as well as original transition Parquets. It rebuilds source aggregates one year
at a time to detect corrupted/stale outputs, verifies summaries, then checks
cross-year PMID uniqueness and row accounting before writing the master.
Expected report: passed_years=39, failed_years=0,
annual_row_sum=combined_rows=distinct_pmids,
cross_year_duplicate_pmid_excess_rows=0, all_checks_passed=True.

Synthetic tests cover all four categories and valid transition states, absent
categories, non-adopted papers, empty years, 27-column order, successful merge,
missing years, duplicate inputs, inconsistent transitions across papers,
cross-year PMID duplication, corrupted flags/summaries, and overwrite protection.
Full-data HPC results have not yet been validated.
