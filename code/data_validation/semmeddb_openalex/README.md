# SemMedDB–OpenAlex mapping and year audit

This standalone audit reads no SciSciNet files. It checks whether publication-year differences coexist with unique/multiple OpenAlex work mappings; it does not infer causation or automatically select a correct work/year.

## Inputs and scope

The default cohort is the 1980–2023 Edge Annotation PMID table, not all PubMed records. Four HPC inputs:

1. Project `data/processed/knowledge_combination/edge_annotation_metrics/pmid_edge_annotation_metrics_1980_2023.parquet` (`PMID,PYEAR`).
2. `/home/u23/jinyugao/research/data/source/semmedVER43_R/semmedVER43_2024_R_CITATIONS.csv.gz`. The original five-column CSV is headerless: `PMID,ISSN,DP,EDAT,PYEAR`. Headered copies require `--citations-header`.
3. `/xdisk/sebratt/jinyugao/research/data/processed/openalex/flattened_snapshot_2025/openalex_works_ids.csv.gz` (`work_id,pmid,doi`).
4. Same OpenAlex directory: `openalex_works_publication_year.csv.gz` (`work_id,publication_year`). Verify snapshot provenance outside this script; identical directory names alone do not establish it.

CLI overrides: `--analysis`, `--citations`, `--ids`, `--years`, `--output`, `--sample-size`. `IC_PROJECT_ROOT` controls default project input/output paths. DuckDB threads and memory use `IC_DUCKDB_THREADS` and `IC_DUCKDB_MEMORY_LIMIT`.

## Definitions

- Normalize OpenAlex work URL prefixes and PubMed URL prefixes. Numeric PMID leading zeros are removed; all-zero or malformed identifiers are rejected. Invalid OpenAlex mappings are exported.
- Distinct PMID–work pairs determine multiplicity; repeated identical input rows do not inflate it. Global cardinalities are computed before cohort filtering, including work IDs shared by other PMIDs outside the cohort.
- The main output has **exactly one row per research PMID**. Analysis year is retained only if the PMID has one distinct valid analysis year. Conflicting analysis years are diagnosed and assigned NULL (not chosen arbitrarily); these PMIDs remain in totals and appear in a NULL-year annual group.
- SemMedDB/OpenAlex years are usable only when every source row for that key contains an explicit integer year (1000–2999) and all agree. Decimal and scientific-notation strings, missing years and conflicting source years are not silently repaired. Identical duplicate source rows preserve the same usable year.
- Mapping groups: `unmatched`, `single_work`, `multiple_works`.
- Exclusive year groups: `unmatched`; `semmed_year_unavailable`; `incomplete_openalex_years` (at least one work has no usable year); `all_match`; `some_match`; `none_match`. All/some/none classifications require complete OpenAlex years and a usable SemMedDB year. Incomplete cases still retain observed matching and different-work counts.
- Equality means **OpenAlex publication_year equals original SemMedDB PYEAR**, not merely the analysis year. `analysis_semmed_year_equal` checks the latter separately.
- `has_work_shared_with_other_pmids` is an independent flag; it does not hide PMID-to-many-work cases.
- Differences are OpenAlex year minus SemMedDB PYEAR. Per-PMID min/max differences, number of distinct OpenAlex years and matching/different/unavailable work counts remain available.
- DP and EDAT years are extracted from their leading four digits only. This lexical check does not fully resolve date ranges, electronic/print publication dates, or indexing history. Full raw dates are retained.
- DOI comparison lowercases identifiers and removes DOI URL prefixes. Distinct DOI counts are screening evidence only; DOI agreement does not prove identical articles. Works with conflicting DOI values are counted separately and not assigned an arbitrary DOI. Titles and full publication histories require additional original records or manual checking; this script does not fetch APIs.

## Output directory and files

Default: project `data/processed/data_validation/semmeddb_openalex/`.

Most useful small files to download first:

- `validation_summary.json`: input paths, cohort/global counts, maximum works per cohort PMID and row checks.
- `mapping_year_summary.csv`: mutually exclusive PMID mapping/year categories, shared-work flag, counts of PMID–work links, and counts of PMIDs with any observed year disagreement (including incomplete cases).
- `mapping_year_summary_by_year.csv`: same, by analysis year; these counts sum to the cohort total, including a NULL-year group if required.
- `global_work_multiplicity.csv`: distribution of works per PMID for all valid OpenAlex mappings, including the full global multi-work group.
- `cohort_work_multiplicity.csv` and `cohort_work_multiplicity_by_year.csv`: the same distribution in the research cohort, including unmatched PMIDs (zero works).
- `multiple_work_year_patterns.csv`: among multi-work PMIDs, distinct usable OpenAlex years, missing-work-year counts and match status. This separates multiple works sharing the same year from multiple works with different years.
- `year_difference_by_mapping_group.csv`: year differences by single/multiple mappings and analysis year; includes link counts and distinct PMID counts. **A multi-work PMID may appear at several offsets, so do not sum distinct-PMID counts over offsets as a total.**
- `semmed_date_comparison.csv`: one-PMID counts by analysis/PYEAR/DP/EDAT years; missing or conflicting years remain NULL.
- `multiple_work_doi_summary.csv`: DOI patterns among multi-work PMIDs.
- `mapping_year_summary_by_issn.csv`: descriptive groups by SemMedDB ISSN set. Multiple conflicting ISSNs are stored together, not interpreted as one verified journal.
- `inspection_pmids.csv` and `inspection_work_details.csv`: deterministic sample PMID classifications and all their work/date/DOI details. Up to `--sample-size` PMIDs per mapping/year category and difference stratum. These are diagnostic samples, not representative estimates. Paper-level DP or analysis-year discrepancies are also eligible even if OpenAlex and SemMedDB PYEAR match.

Full detail:

- `pmid_summary.parquet`: one row per cohort PMID, all classification/count fields.
- `pmid_work_year_details.parquet`: one row per distinct PMID–work pair; unmatched PMID gets one placeholder row. No artificial cross-products from repeated source keys. Original DP/EDAT and raw year sets included.
- `semmed_citation_dates.parquet`: distinct original date variants.
- `analysis_key_diagnostics.csv`, `openalex_year_key_diagnostics.csv`, `rejected_openalex_mappings.csv`: source anomalies.

`all_checks_passed` confirms completion and internal consistency, not that all sources agree. No source data, existing metrics or earlier three-source outputs are modified. Outputs are staged and the validation JSON is published last; a nonempty destination is protected.

## HPC

```bash
cd /xdisk/sebratt/jinyugao/research/repos/innovation_capacity
mkdir -p /xdisk/sebratt/jinyugao/research/projects/innovation_capacity/logs/data_validation
sbatch jobs/data_validation/audit_semmed_openalex.slurm
```

Single job (not year array): global mappings are scanned once, not 44 times. Initial estimate: 4 CPUs, 64 GB RAM, 24 hours; no full-data runtime benchmark yet. The output parent needs temporary disk capacity. CLI overrides can follow the Slurm filename. Choose a new empty output directory for a rerun.

## Validation

```bash
python code/data_validation/semmeddb_openalex/test_audit_semmed_openalex.py -b
```

Synthetic fixtures cover unique matches/differences; multi-work all/some/none/incomplete years; absent mappings/years; global inverse mapping conflicts; conflicting SemMedDB and analysis years; identical duplicate source rows; malformed IDs; strict invalid-year rejection; compressed/headered/headerless CSVs; quoted DOI commas; PMID leading zeros; preservation of raw date evidence; exact PMID/link totals; and overwrite protection. These tests do not replace the pending full HPC run.


## Memory fix after the first HPC run

The first full run exhausted the 48GB DuckDB limit while aggregating SemMedDB dates across roughly 19.5 million PMID groups. SemMedDB date strings, DOI strings, OpenAlex year strings and the final per-PMID counts now aggregate in sequential hash buckets (default 64). Records with the same grouping key always stay together. A disk-backed table is clustered by bucket before aggregation to allow scan pruning, then dropped after use. This adds temporary disk I/O while reducing aggregate state held in memory. No conflict/year rules change.

Override `--aggregation-buckets` or `IC_AUDIT_AGGREGATION_BUCKETS` (e.g. 128 for smaller batches). The Slurm default is now 4 CPUs and 48GB DuckDB memory within a 64GB job. No year array is needed. Validation metadata records the bucket count and DuckDB version. Tests compare one-bucket and seven-bucket outputs using bidirectional EXCEPT ALL, in addition to the existing source/conflict tests. Full HPC memory performance still requires rerunning.

Local memory smoke test: 100,000 unique PMIDs, 256MB DuckDB memory limit, one thread and 16 buckets completed with 100,000 all-match rows. This is not a full-data memory guarantee.
