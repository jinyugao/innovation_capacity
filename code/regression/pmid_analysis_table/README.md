# PMID analysis table

Build one paper-level master for descriptive analysis, correlations and regression.
Every row has a unique canonical PMID and a unique OpenAlex Work ID (W...).
This module does not estimate regressions or modify upstream files.

## Sample and inputs

All paths below are relative to `IC_PROJECT_ROOT`.

Six combined metric inputs under `data/processed/knowledge_combination/`:

| Module | File |
| --- | --- |
| `edge_annotation_metrics/` | `pmid_edge_annotation_metrics_1980_2023.parquet` |
| `edge_annotation_metrics_pattern/` | `pmid_edge_annotation_metrics_pattern_1980_2023.parquet` |
| `edge_annotation_new_knowledge_metrics/pmid/` | `pmid_edge_annotation_new_knowledge_metrics_1980_2023.parquet` |
| `edge_adoption_metrics/pmid/` | `pmid_edge_adoption_metrics_1980_2018.parquet` |
| `future_development_metrics/pmid/` | `pmid_future_development_metrics_1980_2018.parquet` |
| `edge_adoption_decomposition/pmid/` | `pmid_edge_adoption_decomposition_1980_2018.parquet` |

Other required inputs:

- `data/processed/sciscinet_v2/sciscinet_papers_pmid.parquet`: previously filtered
  SciSciNet papers, retaining all original paper-level fields plus PMID. No need
  to rescan the original full SciSciNet release.
- `data/processed/data_validation/semmeddb_openalex/pmid_summary.parquet`
- `data/processed/data_validation/semmeddb_openalex/pmid_work_year_details.parquet`
- `data/processed/data_validation/semmeddb_openalex/validation_summary.json`

These last three files come from the successful **global OpenAlex mapping audit**,
not from counting records in the SciSciNet subset. Its global cardinalities are
reused rather than rederived from the raw OpenAlex snapshot. Preserve that audit's
inputs/provenance. A passed audit is required; the complete audit PMID set and
research years must agree with annotation. Input size/mtime changes during the
build are rejected (these signatures are not content hashes).

Selection:

1. Start with all annotation PMIDs (1980-2023).
2. Keep only PMIDs having exactly one valid Work ID in the full audited mapping,
   where that Work ID is not shared with other PMIDs.
3. Require the audited Work ID to be present in SciSciNet, with matching PMID.
4. Require selected SciSciNet Work IDs to have exactly one record. Duplicates,
   including identical duplicates or multiple field assignments, **stop the job**
   rather than choosing or averaging a record.
5. Left-join remaining metric tables by canonical PMID and research year.

No filtering on year agreement, retraction, document type, positive citations,
positive new knowledge, or positive adoption. Hence each regression can later
apply and report its own sample restrictions. Sample flow and excluded PMID files
are saved. All input metric tables must be unique by normalized PMID; invalid
keys, out-of-range/mismatched years and omitted required columns stop the job.
Counts must be nonnegative finite integers, indicators must be 0/1, and non-NULL
proportions must be finite and between 0 and 1. Non-NULL OpenAlex/SciSciNet years
must have a four-digit integer representation; fractional years are rejected.

## Columns

- `pmid`, `workid`, `pyear`, `openalex_year`, `sciscinet_year`, `sciscinet_date`.
  `pyear` is the upstream analysis year, not a replacement of the other sources.
- Every SciSciNet field except source `paperid`/`PMID` is retained. `year` and
  `date` use the names above; all others use `sci_` plus the original name,
  including `sci_C3`, `sci_C5`, `sci_C10`, `sci_doi`, `sci_doctype`,
  `sci_team_size`, `sci_institution_count`, both overall citation-count fields,
  retraction status, disruption, atypicality, citation dynamics and other counts.
  Field meanings remain those of SciSciNet; no unverified equivalence is assumed
  between `cited_by_count` and `citation_count`. Additional source fields are
  retained automatically unless their output names collide with another field.
- All metrics from the six modules. Shared definitions are retained once, in
  source order annotation, pattern, new knowledge, adoption, development,
  decomposition. Before choosing one copy, overlapping values are checked on
  **all overlapping input PMIDs**, including excluded papers. Null status must
  agree; floating values use an absolute tolerance of 1e-9. Other values agree
  exactly. Shared fields are checked together per source-table pair to avoid
  repeating the same full PMID join for each field. A conflicting same-name
  field stops the job.
- Pairwise year-match flags and signed differences: OpenAlex minus pyear and
  SciSciNet minus pyear. Missing years produce NULL, not disagreement.
- Six `has_<module>_metrics` boolean flags indicate actual source-row presence.
  Upstream zero/NULL values are preserved. Missing source rows are not filled
  with zero; post-2018 outcomes remain NULL.
- `has_new_triples` retains the upstream 0/1 definition.
- `eligible_adoption_5y`: research year 1980-2018, whose five future calendar
  years are covered through 2023. This does not imply an outcome row is present;
  also check `has_adoption_metrics` / `has_development_metrics`.
- `eligible_c3`, `eligible_c5`, `eligible_c10`: `sciscinet_year + window <= cutoff`.
  The cutoff must be the verified last **fully covered citation calendar year**.
  These flags do not check whether the citation value is nonmissing. Without
  `--citation-window-end-year`, all three are NULL: a release label or available
  C10 value alone does not establish a complete observation window.

Newness uses the upstream preceding five-year window; exact triples preserve
subject/predicate/object direction, while concept pairs are undirected. Adoption
and development describe subsequent occurrence, not causal transmission.
Proportions with zero denominators retain upstream NULL. Citation and other
SciSciNet source values are not imputed, rescaled or automatically winsorized.
Future variables are not automatically treated as baseline controls.

No research-field, country, author or journal tables are merged by this module.
A field variable can be added later with an explicit paper-level assignment rule.

## Outputs

Default directory: `data/regression/pmid_analysis_table/`.

- `pmid_analysis_table_1980_2023.parquet`: ZSTD master, one PMID/Work ID per row.
- `variable_dictionary.csv`: English definitions, source, denominator for ratio
  metrics, coverage, missing-value rules and DuckDB data types. SciSciNet entries
  identify original fields and defer their detailed interpretation to its docs.
- `summary_by_year.csv`: selected papers, year agreement, source presence and
  nonmissing C3/C5/C10 counts. Nonmissing does not imply a complete window.
- `missingness.csv`: per-variable row and missing counts.
- `sample_flow.json`: input, mapping exclusions, missing-SciSciNet exclusions,
  final row count and number of columns.
- `excluded_mapping_pmids.parquet`: research PMID/year, cardinality and exclusion
  reason; no mapping, multiple Work IDs, or reverse ambiguity.
- `excluded_missing_sciscinet.parquet`: one-to-one mappings absent from SciSciNet.
- `validation_summary.json`: successful checks, duplicate-field comparisons,
  source paths/signatures, sample flow and the citation cutoff used.

Existing output directories are protected. Use `--output` with a fresh versioned
path for reruns. Temporary disk-backed DuckDB tables and spill files are written
beside the output and removed on completion/failure. Ensure substantial free
scratch/project space; full-scale resource needs have not yet been measured.
The complete result directory is published only after checks pass. A failure
before publication leaves no new result directory. Exceptions and logs identify
failed checks; no successful report is written for a failed build.

## HPC execution

Requires the existing project Python environment with DuckDB. This is a single
merge job, not an annual array. Initial resources: 4 CPUs, 64 GB (DuckDB 48 GB),
24 hours. Adjust after observing the full-data run.

```bash
cd /xdisk/sebratt/jinyugao/research/repos/innovation_capacity
mkdir -p /xdisk/sebratt/jinyugao/research/projects/innovation_capacity/logs/regression
sbatch jobs/regression/build_pmid_analysis_table.slurm
```

When the citation cutoff has been verified, pass it explicitly, for example
`--citation-window-end-year YEAR` after the Slurm script path, replacing YEAR with
that verified year. Default submission deliberately leaves citation eligibility
unknown; it still retains all C3/C5/C10 values.

All input paths have CLI overrides (`--annotation`, `--pattern`, `--new-knowledge`,
`--adoption`, `--development`, `--decomposition`, `--pmid-summary`,
`--work-details`, `--audit-validation`, `--sciscinet`). Use `--help` for defaults.
`IC_PROJECT_ROOT`, `IC_REPO_ROOT`, `IC_PYTHON`, `IC_DUCKDB_THREADS` and
`IC_DUCKDB_MEMORY_LIMIT` follow existing project conventions.

## Local verification

```bash
python -m unittest discover -s code/regression/pmid_analysis_table -v
```

Synthetic integration cases cover global one-to-one selection, reverse
cardinality, missing SciSciNet works, duplicate normalized PMIDs and SciSciNet
records, conflicting metrics, mismatched SciSciNet PMID, stale audit/source years,
unknown citation cutoff and exact window boundaries, NULL ratios, unavailable
post-2018 outcomes, source-row presence flags, failed audit and output protection.
Additional cases reject negative counts, out-of-range/NaN proportions, fractional
SciSciNet years, missing mapping details and shared Work IDs. All 18 tests pass.
A developer smoke test also passed using real metric/SciSciNet rows from 1980,
2018 and 2023 (35 retained rows, 130 columns, 26 duplicate-field checks); its
audit mapping was a synthetic fixture, so it does not validate production sample
selection. Full-data HPC execution and its output validation are still pending.
