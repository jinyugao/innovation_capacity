# BiomedBERT Run Order

This checklist records the intended HPC execution order for the BiomedBERT
second-layer link-prediction experiment. It is intentionally written as a
manual `sbatch` checklist rather than an automatic submission script, so each
stage can be checked before the next one starts.

Local repo:

```text
/Users/jinyugao/Documents/research/repos/innovation_capacity/
```

HPC repo:

```text
/xdisk/sebratt/jinyugao/repos/innovation_capacity/
```

HPC project directory:

```text
/xdisk/sebratt/jinyugao/projects/innovation_capacity/
```

## Before Running

Confirm the upstream data files exist on HPC.

Required SemMedDB files:

```text
data/interim/semmedVER43_R/
  semmedVER43_2024_R_predications_with_pyear_filtered.csv.gz

data/interim/semmedVER43_R/split_predications_with_pyear_filtered_by_pyear/
  semmedVER43_R_predications_with_pyear_filtered_{1975..2019}.csv.gz
```

Required first-layer and candidate-edge files:

```text
data/interim/link_prediction/edge_annotation/first_layer/
  semmedVER43_R_predications_with_pyear_filtered_first_layer_edge_annotation_{1980..2019}.csv.gz

data/interim/link_prediction/candidate_edges/two_hop_candidate_edges/
  two_hop_candidate_edges_prior_5y_{1980..2019}.csv.gz
```

The focal prediction years are 1980-2019. The 1975-1979 split files are needed
because the 1980 focal year uses a prior five-year history window.

Confirmed GPU settings on HPC:

```bash
#SBATCH --account=sebratt
#SBATCH --partition=gpu_standard
#SBATCH --gres=gpu:1
```

The GPU test job successfully ran on a Tesla V100S-PCIE-32GB node. Use the GPU
job variants for steps that run BiomedBERT model inference.

Create the Slurm log directories before submitting jobs. Slurm opens stdout and
stderr before the job script can run `mkdir -p`, so these directories must
already exist.

```bash
bash jobs/link_prediction/biomedbert/create_biomedbert_log_dirs.sh
```

After syncing the latest repo changes to HPC, run the read-only preflight check
from the HPC repo root:

```bash
/xdisk/sebratt/jinyugao/envs/innovation_capacity/bin/python code/link_prediction/biomedbert/check_biomedbert_hpc_preflight.py
```

This checks required repo files, Slurm log directories, upstream input files,
Python packages, and existing outputs that may block reruns because scripts use
`OVERWRITE = False`.

## Stage 1: Shared Preparation

Run these once, in order.

```bash
sbatch jobs/link_prediction/biomedbert/preparation/build_biomedbert_cui_name_frequencies.slurm
sbatch jobs/link_prediction/biomedbert/preparation/select_biomedbert_cui_labels.slurm
sbatch jobs/link_prediction/biomedbert/preparation/build_biomedbert_cui_embeddings_gpu.slurm
```

Expected outputs:

```text
data/interim/biomedbert_link_prediction/cui_labels/
  biomedbert_cui_name_frequencies.csv.gz
  biomedbert_cui_labels.csv.gz

data/interim/biomedbert_link_prediction/cui_embeddings/
  biomedbert_cui_embeddings.npz
  biomedbert_cui_embedding_metadata.csv.gz
```

Notes:

- `build_biomedbert_cui_embeddings_gpu.slurm` is the recommended job because it
  runs BiomedBERT inference.
- `build_biomedbert_cui_embeddings.slurm` is kept as a CPU fallback.

## Stage 2: Candidate Coverage Checks

New-combination coverage can run after first-layer annotations and two-hop
candidate edges are available.

```bash
sbatch jobs/link_prediction/biomedbert/new_combination/build_biomedbert_new_combination_candidate_coverage_summary.slurm
```

Expected output:

```text
results/link_prediction/biomedbert/new_combination/
  biomedbert_new_combination_candidate_coverage_summary.csv
```

New-relation candidate triples must be built before new-relation coverage or
new-relation scoring.

```bash
sbatch jobs/link_prediction/biomedbert/new_relation/build_biomedbert_new_relation_candidate_triples.slurm
```

Expected output:

```text
data/interim/biomedbert_link_prediction/new_relation/candidate_triples/
  biomedbert_new_relation_candidate_triples_{1980..2019}.csv.gz
```

Then run new-relation candidate coverage:

```bash
sbatch jobs/link_prediction/biomedbert/new_relation/build_biomedbert_new_relation_candidate_coverage_summary.slurm
```

Expected output:

```text
results/link_prediction/biomedbert/new_relation/
  biomedbert_new_relation_candidate_coverage_summary.csv
```

## Stage 3: Main Scoring

Run scoring after shared preparation is complete.

New-combination scoring:

```bash
sbatch jobs/link_prediction/biomedbert/new_combination/score_biomedbert_candidate_edges.slurm
```

Expected output:

```text
data/interim/biomedbert_link_prediction/candidate_edges/
  biomedbert_scored_candidate_edges_{1980..2019}.csv.gz
```

New-relation scoring:

```bash
sbatch jobs/link_prediction/biomedbert/new_relation/score_biomedbert_new_relation_candidate_triples_gpu.slurm
```

Expected output:

```text
data/interim/biomedbert_link_prediction/new_relation/scored_candidate_triples/
  biomedbert_new_relation_scored_candidate_triples_{1980..2019}.csv.gz
```

Notes:

- New-combination scoring uses precomputed CUI embeddings and NumPy vector
  operations, so it remains a CPU job.
- `score_biomedbert_new_relation_candidate_triples_gpu.slurm` is the recommended
  new-relation scoring job because it encodes candidate/reference triple texts
  with BiomedBERT.
- `score_biomedbert_new_relation_candidate_triples.slurm` is kept as a CPU
  fallback.
- Robustness thresholds do not require rerunning scoring.

## Stage 4: Main Top-10% Prediction

Run after the corresponding scored files exist.

```bash
sbatch jobs/link_prediction/biomedbert/new_combination/build_biomedbert_new_combination_top_10pct_predicted_edges.slurm
sbatch jobs/link_prediction/biomedbert/new_relation/build_biomedbert_new_relation_top_10pct_predicted_triples.slurm
```

Expected outputs:

```text
data/interim/link_prediction/predicted_edges/biomedbert/
  biomedbert_new_combination_predicted_edges_top_10pct_{1980..2019}.csv.gz

data/interim/link_prediction/predicted_triples/biomedbert/new_relation/
  biomedbert_new_relation_predicted_triples_top_10pct_{1980..2019}.csv.gz
```

## Stage 5: Robustness Prediction

Run after the corresponding scored files exist. These scripts write top 1%, 5%,
and 20% robustness predicted sets.

```bash
sbatch jobs/link_prediction/biomedbert/new_combination/build_biomedbert_new_combination_robustness_predicted_edges.slurm
sbatch jobs/link_prediction/biomedbert/new_relation/build_biomedbert_new_relation_robustness_predicted_triples.slurm
```

Expected outputs:

```text
data/interim/link_prediction/predicted_edges/biomedbert/new_combination/robustness/{1pct,5pct,20pct}/
  biomedbert_new_combination_robustness_predicted_edges_top_{percentile}_{1980..2019}.csv.gz

data/interim/link_prediction/predicted_triples/biomedbert/new_relation/robustness/{1pct,5pct,20pct}/
  biomedbert_new_relation_robustness_predicted_triples_top_{percentile}_{1980..2019}.csv.gz
```

## Stage 6: Main Top-10% Annotation

Run after main top-10% predicted files exist.

```bash
sbatch jobs/link_prediction/biomedbert/new_combination/annotate_biomedbert_new_combination_top_10pct_predications.slurm
sbatch jobs/link_prediction/biomedbert/new_relation/annotate_biomedbert_new_relation_top_10pct_predications.slurm
```

Expected outputs:

```text
data/interim/link_prediction/annotated_predications/biomedbert/new_combination/10pct/
  semmedVER43_R_predications_with_pyear_filtered_biomedbert_new_combination_top_10pct_annotated_{1980..2019}.csv.gz

data/interim/link_prediction/annotated_predications/biomedbert/new_relation/10pct/
  semmedVER43_R_predications_with_pyear_filtered_biomedbert_new_relation_top_10pct_annotated_{1980..2019}.csv.gz
```

## Stage 7: Robustness Annotation

Run after robustness predicted files exist.

```bash
sbatch jobs/link_prediction/biomedbert/new_combination/annotate_biomedbert_new_combination_robustness_predications.slurm
sbatch jobs/link_prediction/biomedbert/new_relation/annotate_biomedbert_new_relation_robustness_predications.slurm
```

Expected outputs:

```text
data/interim/link_prediction/annotated_predications/biomedbert/new_combination/robustness/{1pct,5pct,20pct}/
  semmedVER43_R_predications_with_pyear_filtered_biomedbert_new_combination_robustness_top_{percentile}_annotated_{1980..2019}.csv.gz

data/interim/link_prediction/annotated_predications/biomedbert/new_relation/robustness/{1pct,5pct,20pct}/
  semmedVER43_R_predications_with_pyear_filtered_biomedbert_new_relation_robustness_top_{percentile}_annotated_{1980..2019}.csv.gz
```

## Stage 8: Evaluation Summaries

Run after the corresponding annotation and predicted files exist.

Main top-10% summaries:

```bash
sbatch jobs/link_prediction/biomedbert/new_combination/build_biomedbert_new_combination_evaluation_summary.slurm
sbatch jobs/link_prediction/biomedbert/new_relation/build_biomedbert_new_relation_evaluation_summary.slurm
```

Robustness summaries:

```bash
sbatch jobs/link_prediction/biomedbert/new_combination/build_biomedbert_new_combination_robustness_evaluation_summary.slurm
sbatch jobs/link_prediction/biomedbert/new_relation/build_biomedbert_new_relation_robustness_evaluation_summary.slurm
```

Expected outputs:

```text
results/link_prediction/biomedbert/new_combination/
  biomedbert_new_combination_evaluation_summary.csv
  robustness/biomedbert_new_combination_robustness_evaluation_summary.csv

results/link_prediction/biomedbert/new_relation/
  biomedbert_new_relation_evaluation_summary.csv
  robustness/biomedbert_new_relation_robustness_evaluation_summary.csv
```

## Dependency Summary

Minimal dependency graph:

```text
upstream SemMedDB + first-layer + two-hop files
-> shared preparation
-> new-combination coverage
-> new-combination scoring
-> main 10% new-combination prediction
-> main 10% new-combination annotation
-> main new-combination evaluation

new-combination scoring
-> robustness new-combination prediction
-> robustness new-combination annotation
-> robustness new-combination evaluation

upstream SemMedDB + first-layer + CUI labels
-> new-relation candidate triples
-> new-relation coverage
-> new-relation scoring
-> main 10% new-relation prediction
-> main 10% new-relation annotation
-> main new-relation evaluation

new-relation scoring
-> robustness new-relation prediction
-> robustness new-relation annotation
-> robustness new-relation evaluation
```

## Do Not Forget

- Confirm 1975-2019 yearly split files before submitting arrays.
- Create BiomedBERT Slurm log directories before submitting jobs.
- Confirm GPU availability before changing the embedding or new-relation
  scoring jobs.
- Do not rerun jobs with existing outputs unless the corresponding Python
  script has `OVERWRITE = True` or outputs have been deliberately archived or
  removed.
- Keep the top-10% main analysis and 1%/5%/20% robustness outputs separate.
