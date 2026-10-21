# SemMedDB / OpenAlex / SciSciNet identifier and year audit

Read-only source audit using Python and DuckDB. No knowledge metrics, years or ID mappings are repaired. The full analysis cohort is retained, including ambiguous mappings and absent sources. No year equality condition is used in joins.

## Inputs

Defaults use HPC paths:

- Analysis: project `data/processed/knowledge_combination/edge_annotation_metrics/pmid_edge_annotation_metrics_1980_2023.parquet` (`PMID`, `PYEAR`).
- SemMedDB: `/home/u23/jinyugao/research/data/source/semmedVER43_R/semmedVER43_2024_R_CITATIONS.csv.gz`. Original CSV has **no header**, five columns in order `PMID,ISSN,DP,EDAT,PYEAR`. Use `--citations-header` only for a headered copy. No year is inferred as a replacement.
- OpenAlex IDs: `/xdisk/sebratt/jinyugao/research/data/processed/openalex/flattened_snapshot_2025/openalex_works_ids.csv.gz` (`work_id,pmid,doi`).
- OpenAlex years: same directory, `openalex_works_publication_year.csv.gz` (`work_id,publication_year`).
- SciSciNet: `/home/u23/jinyugao/research/data/source/sciscinet_v2/sciscinet_papers.parquet` (`paperid,year`). Use the original file, not only the earlier PMID-filtered output.

Override with `--analysis`, `--citations`, `--ids`, `--years`, `--sciscinet`, `--output`. The analysis path also accepts `IC_AUDIT_ANALYSIS_FILE`; citations accept `IC_AUDIT_CITATIONS_FILE`. Record and verify the actual snapshot versions independently: a shared directory name does not prove contemporaneous snapshots.

## Outputs

Default: project `data/processed/data_validation/semmeddb_openalex_sciscinet/`.

- `pmid_work_year_audit.parquet`: one row per distinct analysis PMID/year/work mapping; an unmatched PMID/year gets one row with null work. Includes source row counts, raw year sets, distinct-year counts, DOI(s), mapping cardinalities and four signed year differences.
- `semmed_citation_dates.parquet`: distinct original citation date variants for cohort PMIDs, retaining DP, EDAT and raw PYEAR.
- `match_summary_by_year.csv`: rows, distinct PMIDs and works by analysis year and diagnostic status.
- `year_difference_{semmed_analysis,openalex_semmed,sciscinet_semmed,sciscinet_openalex}.csv`: differences by analysis year, for globally one-to-one mappings, except `semmed_analysis`, which covers the whole cohort (including unmatched/ambiguous OpenAlex records) and counts each PMID once per analysis year. Each comparison requires its two valid unambiguous years; the third source need not be available. Differences follow the order in the filename, e.g. SciSciNet minus SemMedDB.
- `semmed_pyear_dp_edat_comparison.csv`: original PYEAR vs first four digits of DP and EDAT. This is a lexical year diagnostic, not a complete interpretation of date ranges or publication history. Conflicting citation records are retained; PMID counts across rows may overlap.
- `mapping_conflicts.csv`: mappings for works touched by the cohort, including any other PMID mapped to those works. Global ID cardinalities are calculated before cohort filtering. Duplicate identical mapping pairs do not count as distinct works.
- `{semmed,openalex,sciscinet}_key_diagnostics.csv`: duplicate source keys, conflicting years, missing/invalid years, and raw year sets. Source row totals expose identical duplicates even when years agree.
- `rejected_openalex_mappings.csv`: nonempty PMID mappings with malformed IDs, including the raw values. They are diagnosed, never silently joined.
- `inspection_sample.csv`: deterministic up-to-100 rows per status AND SciSciNet-minus-SemMedDB difference stratum (missing, zero, -1, +1, larger). Override `--sample-size`. These are inspection candidates, not verified publication matches.
- `validation_summary.json`: input paths, scope/cardinality counts and output row accounting. `all_checks_passed` means the audit completed and reconciled its rows, **not** that all records agree or that a particular source is correct.

PMID numeric strings are normalized by removing leading zeros. Empty cohorts and invalid analysis years fail explicitly. CITATIONS header presence and column order are verified rather than silently dropping a header row.

A source year is usable only when all rows at that key contain a valid explicit four-digit year (1000-2999; decimal strings and scientific notation are rejected) and agree on that year. Raw conflicting sets are retained, not resolved with a minimum or majority vote. Three-way equality statuses are prioritised: analysis/SemMedDB mismatch; missing mapping; ambiguous mapping; missing/conflicting source years; then the equality pattern. Full source fields remain available regardless of this primary label. Do not sum distinct PMID counts over mutually overlapping work groups or multiple analysis years. A missing year, or absent work, cannot be treated as zero. No title/date API lookups occur.

## Running on HPC

```bash
cd /xdisk/sebratt/jinyugao/research/repos/innovation_capacity
mkdir -p /xdisk/sebratt/jinyugao/research/projects/innovation_capacity/logs/data_validation
sbatch jobs/data_validation/audit_semmed_openalex_sciscinet.slurm
```

To use a different input path, pass CLI flags after the Slurm filename. For a rerun, select a new empty `--output` directory. Nonempty outputs are protected. Results are staged and published only after row checks pass. Full OpenAlex ID mappings are scanned for global cardinalities; only cohort-related years and SciSciNet rows are retained. This is one large streaming/out-of-core job, not a year array, to avoid rescanning global files 44 times. Initial resource settings are 8 CPUs, 64 GB RAM, 24 hours; full HPC performance has not been measured. Temporary disk capacity is also required in the output parent.

## Local synthetic validation

```bash
python code/data_validation/semmeddb_openalex_sciscinet/test_audit.py
```

Covers correct matches, both mapping cardinality directions, missing matches/sources, URL normalization, quoted DOI commas, conflicting/invalid years, signed year differences, malformed identifiers, row accounting and overwrite protection. Additional tests cover all three-way equality patterns, analysis/SemMedDB mismatches, strict invalid-year rejection, missing source records, header options, identical duplicates, leading-zero PMIDs, empty cohorts and unmatched-PMID inclusion in the SemMedDB comparison. Full-source disagreements still require HPC execution and manual PubMed/OpenAlex record inspection. Agreement between OpenAlex and SciSciNet is not independent confirmation: SciSciNet uses OpenAlex.
