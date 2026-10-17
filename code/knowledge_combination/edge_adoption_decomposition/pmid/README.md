# PMID new-knowledge production and subsequent-adoption decomposition

This module creates one row per PMID for focal years 1980–2018. It decomposes
the share of subsequently adopted new knowledge in all knowledge output into:

```text
share_new_triples * share_new_triples_adopted
    = share_adopted_new_triples_in_all
```

`Adopted` means that the same directed, predicate-specific triple reappears in
publications during the subsequent five years. It is a triple-level outcome and
does not attribute adoption or citation to a particular focal PMID.

## Input

Under `IC_PROJECT_ROOT`:

```text
data/processed/knowledge_combination/edge_adoption_metrics/pmid/
    pmid_edge_adoption_metrics_YEAR.parquet
```

The upstream table must contain one row per PMID and the three required counts.
The script also verifies its two existing adoption shares before calculating the
decomposition. Only focal years through 2018 are included because a complete
five-year follow-up window is required.

## PMID output

Annual and combined ZSTD Parquets contain eight columns:

```text
PMID
PYEAR
n_triples
n_new_triples
n_new_triples_adopted
share_new_triples
share_new_triples_adopted
share_adopted_new_triples_in_all
```

`share_new_triples_adopted` is NULL when `n_new_triples=0`; the other two
shares are zero in that case. Papers without annotated triples are absent from
the upstream data.

Files are written under:

```text
data/processed/knowledge_combination/edge_adoption_decomposition/pmid/
    pmid_edge_adoption_decomposition_YEAR.parquet
    pmid_edge_adoption_decomposition_1980_2018.parquet
    summary/pmid_edge_adoption_decomposition_summary_YEAR.csv
    summary/pmid_edge_adoption_decomposition_summary_by_year.csv
    validation/pmid_edge_adoption_decomposition_validation_YEAR.csv
    validation/pmid_edge_adoption_decomposition_year_validation.csv
    validation/pmid_edge_adoption_decomposition_validation_summary.csv
```

Annual summaries include PMID counts, summed denominators and numerators,
paper-weighted means and medians, and pooled annual ratios. The paper mean and
pooled ratio are distinct estimands. Validation checks the schema, count bounds,
zero-denominator rules, source agreement, the decomposition identity, annual
completeness, row accounting, and global PMID uniqueness.

## HPC execution

After the upstream PMID edge-adoption metrics have completed successfully:

```bash
cd /xdisk/sebratt/jinyugao/research/repos/innovation_capacity
mkdir -p /xdisk/sebratt/jinyugao/research/projects/innovation_capacity/logs/knowledge_combination/edge_adoption_decomposition/pmid
if jid_decomposition=$(sbatch --parsable jobs/knowledge_combination/edge_adoption_decomposition/pmid/build_pmid_edge_adoption_decomposition.slurm); then
    echo "Annual decomposition: $jid_decomposition"
    sbatch --dependency=afterok:${jid_decomposition} jobs/knowledge_combination/edge_adoption_decomposition/pmid/combine_pmid_edge_adoption_decomposition.slurm
fi
```

Array tasks 0–38 map to 1980–2018. For intentional replacement, export
`IC_PMID_EDGE_ADOPTION_DECOMPOSITION_OVERWRITE=1`. Direct Python execution uses
`IC_FOCAL_YEAR` when `SLURM_ARRAY_TASK_ID` is absent.
