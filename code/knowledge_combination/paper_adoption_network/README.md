# PMID-level knowledge adoption network

The arrow is **future adopter PMID -> focal PMID**. An edge means exact knowledge recurrence, not a citation or proven learning from a particular paper. Main weights are raw counts of distinct shared new triples; no fractional attribution is used. No OpenAlex, SciSciNet, country or citation data are required. Existing metrics and draft files remain unchanged.

## Definition

For focal year t (1980–2018), a directed edge B -> A exists if A is a focal-year paper, B is published in t+1 through t+5, and B reports at least one exact triple classified as New Node, New Combination or New Relation in year t. Exact means the same subject primary CUI, predicate and object primary CUI, in the same direction. Future-year category is not used to label the edge. No same-year edges are built. Repeated Triple does not contribute to this new-knowledge network.

Each directed PMID pair has one row. Count each matching triple once within the pair. A triple shared by several focal papers connects the future paper to all of them; there is no attempt to choose one original discoverer. Newness remains relative to the previous five years. All time assignments use SemMedDB.

## Inputs

Under `IC_PROJECT_ROOT/data/processed/knowledge_combination/`:

- `edge_transition/edge_transition_YEAR.parquet` for focal year t;
- `edge_annotation/edge_annotation_YEAR.parquet` for t and all five subsequent years;
- `edge_adoption_metrics/pmid/pmid_edge_adoption_metrics_YEAR.parquet` for t (mandatory validation reference).

Required annotation columns: `PMID,PYEAR,subject_cui_primary,PREDICATE,object_cui_primary,edge_annotation`. Transition additionally requires Boolean `future_exact_triple_seen` and `future_five_year_transition`. Every annual file, including empty years with the expected schema, is required. Inputs must be PMID/exact-triple unique, exclude self-loops, have valid categories and contain exactly their declared year. Same-year triples must have consistent category labels across papers. A PMID must not occur in several publication years.

A focal paper with zero new triples is retained; papers without any eligible annotated triples are outside the sample. Source annotation and transition keys/categories must agree exactly. The five-year annotation scan independently reconstructs adoption and is compared to transition flags plus existing per-PMID category/new counts, category adoption counts and `has_new_triples_adopted`. Inconsistent inputs fail instead of silently creating a smaller network.

## Output paths

Base: project `data/processed/knowledge_combination/paper_adoption_network/`.

Annual build:

```text
edges/pmid_adoption_network_edges_YEAR.parquet
nodes/pmid_adoption_network_focal_nodes_YEAR.parquet
triple_pmid/pmid_adoption_network_triple_pmid_YEAR.parquet
summary/pmid_adoption_network_summary_YEAR.json
validation/pmid_adoption_network_validation_YEAR.json
```

Master combine (default full range):

```text
edges/pmid_adoption_network_edges_1980_2018.parquet
nodes/pmid_adoption_network_nodes_1980_2023.parquet
summary/pmid_adoption_network_summary_1980_2018.csv
validation/pmid_adoption_network_validation_1980_2018.json
```

Parquets use ZSTD. Full expanded paper-pair–triple rows are NOT permanently saved. Annual triple–PMID trace files remain separate because their cohort context differs; they are not globally deduplicated across focal years.

### Edge columns (9)

`adopter_pmid,focal_pmid,adopter_year,focal_year,year_gap` followed by:

- `n_shared_new_node_triples`
- `n_shared_new_combination_triples`
- `n_shared_new_relation_triples`
- `n_shared_new_triples` (sum of the three above)

Counts are BIGINT, years/gap INTEGER, PMIDs VARCHAR. Adopter = origin of arrow; focal = destination. The labels deliberately avoid ambiguous source/target conventions.

### Trace columns

`focal_year,role,PMID,PYEAR,subject_cui_primary,PREDICATE,object_cui_primary,focal_edge_annotation,n_focal_pmids`.

`role=focal_new` retains ALL new focal triple/PMID occurrences, including those not adopted. `role=future_matched` retains future triple/PMID occurrences matching a focal new triple, with the focal category. `n_focal_pmids` is the number of focal-year papers reporting this exact triple (parallel reporting), not a fractional edge weight. For a given cohort and role, rows are PMID/exact-triple unique. The same future occurrence can legitimately appear in more than one focal cohort's trace file.

### Master node columns

One row per PMID with eligible annotated triples, including papers not present on any edge:

- `PMID,PYEAR,n_triples,n_new_node,n_new_combination,n_new_relation,n_new_triples`;
- `focal_eligible`, `adopter_eligible` and two observation-coverage flags;
- three category-specific adopted-new counts, `n_new_triples_adopted`, `has_new_triples_adopted`;
- `focal_status`: `no_new_triples`, `new_triples_not_adopted`, `new_triples_adopted` or `target_only`;
- `in_degree,in_strength,out_degree,out_strength` for edges present in THIS network.

In-degree = distinct future papers pointing to a focal paper. In-strength = shared-new-triple counts summed over those papers. These differ from distinct adopted triples: one triple reported in ten future papers gives adopted-triple count 1, in-degree 10 and in-strength 10.

For the default range, 1980–2018 nodes are focal-eligible with complete five-year future follow-up; 2019–2023 nodes are target-only (future adopter role), and their focal adoption measures are NULL, not zero. Observed in/out degrees remain numeric, but zero in-degree for target-only nodes is structural and not a no-adoption finding. `adopter_eligible` is true after 1980; 1980 nodes cannot be future endpoints of a retained cohort.

`has_complete_five_year_focal_coverage` is true for adopter years 1985–2019: all five preceding focal cohorts are included. Earlier and later adopters have incomplete outgoing opportunities. Do not interpret raw out-degrees as equally observed across all years. Isolates are retained.

## Execution and resources

Python with DuckDB is required. Four CPUs, 64GB job memory, 48GB DuckDB cap, 24 hours per task are initial settings, not full-data benchmarks. Temporary disk space in the output base is required. The build groups edges in 64 sequential focal-PMID hash buckets to limit aggregation state. Each key stays in one bucket, so all contributions to a directed paper pair are summed together. Larger bucket counts can reduce aggregation memory but increase scans/runtime. Configure `IC_PMID_NETWORK_BUCKETS` or `--aggregation-buckets`, and `IC_DUCKDB_THREADS/IC_DUCKDB_MEMORY_LIMIT`.

### First: estimate scale without expanding paper pairs

```bash
cd /xdisk/sebratt/jinyugao/research/repos/innovation_capacity
mkdir -p /xdisk/sebratt/jinyugao/research/projects/innovation_capacity/logs/knowledge_combination/paper_adoption_network
sbatch jobs/knowledge_combination/paper_adoption_network/profile_pmid_adoption_network.slurm
```

Array indices 0,20,38 profile focal years 1980,2000,2018 sequentially. Files:
`summary/pmid_adoption_network_profile_YEAR.json`.

The profiler still validates inputs/reconstructs adoption, but does not generate edges/nodes/trace or mark annual build success. It reports source/future triple-paper counts, per-triple maxima and `n_triple_paper_pair_occurrences = sum(n_focal_pmids * n_future_pmids)`. This is the exact total future edge weight and an upper bound on distinct edge count, NOT the distinct edge count itself. A common triple may generate many pairs. No common triples are dropped, sampled or capped. Inspect these profiles before deciding full-run memory/time settings.

### Build and combine after the scale check

```bash
if network_job=$(sbatch --parsable jobs/knowledge_combination/paper_adoption_network/build_pmid_adoption_network.slurm); then
    sbatch --dependency=afterok:${network_job} jobs/knowledge_combination/paper_adoption_network/combine_pmid_adoption_network.slurm
fi
```

Build array 0–38 maps to 1980–2018; at most three tasks run simultaneously. Combine is a separate single job and requires all 39 successful annual builds. For one trial year use `sbatch --array=20 .../build_pmid_adoption_network.slurm` (year 2000), then run other years before full combine. CLI flags may follow the Slurm filename. Direct Python supports `--year`, `IC_FOCAL_YEAR`, or array index.

Build CLI: `--project-root`, `--year`, `--aggregation-buckets`, `--profile-only`, `--overwrite`. Combine CLI: `--project-root`, `--start-year` (1980), `--end-year` (2018), `--overwrite`. A custom range is useful for tests; node span ends five years after the last focal year. Full names adapt to the selected range.

Existing outputs are protected. `--overwrite` stages replacement files first and publishes the validation JSON last. On failed runs, inspect the job error/log; an older master may still exist. Original input tables are never replaced.

## Validation

Build verifies raw keys, categories, years, transition flags, duplicate triples, focal annotation/transition equality, and reconstructed adoption against the existing PMID metrics. The sum of all edge weights must equal the independent per-triple count product. In-degree positivity must match each focal PMID's adoption indicator.

Combine checks every annual success report, source file sizes/mtime, annual output sizes/mtime, global pair uniqueness, node PMID uniqueness, edge endpoints/years, sum identities and annual/global degrees. Every annotated PMID in the node span is retained. Source/output size+mtime signatures detect ordinary stale/changed files; they are not cryptographic checksums and moving copies while changing timestamps can invalidate them. Run combine in the original output tree. Input changes during build/combine cause failure. The combined CSV includes zero-edge years.

```bash
python code/knowledge_combination/paper_adoption_network/test_pmid_adoption_network.py -b
```

Tests cover parallel focal reporters, multiple shared triples, correct arrow direction/category counts, exact direction matching, zero-incoming and isolated papers, target-only missing adoption measures, empty focal years, missing inputs, duplicate future triples, disagreements with prior metrics, profile-only behavior, one-vs-seven bucket equivalence, stale outputs and a multi-year merge with a zero-edge year. A fan-out test reconstructs 100,000 edges from 200 parallel focal papers and 500 future papers under a 256MB DuckDB cap (one thread, eight aggregation buckets). Full HPC execution remains pending.


## Additional review checks

Input scans project only the columns used by this network. Future-year annotations are validated and appended one year at a time; temporary annual copies are dropped before the next year. Annual directed PMID pairs are checked explicitly for uniqueness. Combine rechecks the complete original-source signature set immediately before publishing (including transition and previous metric files), not just the annotation and annual-output files. Tests also cover +5-year inclusion, +6-year exclusion, every input year being empty, future PMIDs duplicated across publication years and stale source metrics.
