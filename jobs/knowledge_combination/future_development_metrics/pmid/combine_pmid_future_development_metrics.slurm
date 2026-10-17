#!/bin/bash
#SBATCH --job-name=combine_pmid_future_development_metrics
#SBATCH --output=/xdisk/sebratt/jinyugao/research/projects/innovation_capacity/logs/knowledge_combination/future_development_metrics/pmid/combine_pmid_future_development_metrics_%j.out
#SBATCH --error=/xdisk/sebratt/jinyugao/research/projects/innovation_capacity/logs/knowledge_combination/future_development_metrics/pmid/combine_pmid_future_development_metrics_%j.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=32gb
#SBATCH --time=08:00:00
#SBATCH --account=sebratt
#SBATCH --partition=standard

set -euo pipefail

# Create the SBATCH log directory BEFORE submitting this job.
export IC_PROJECT_ROOT="${IC_PROJECT_ROOT:-/xdisk/sebratt/jinyugao/research/projects/innovation_capacity}"
export IC_REPO_ROOT="${IC_REPO_ROOT:-/xdisk/sebratt/jinyugao/research/repos/innovation_capacity}"
export IC_PYTHON="${IC_PYTHON:-/xdisk/sebratt/jinyugao/research/envs/innovation_capacity/bin/python}"
export IC_DUCKDB_THREADS="${IC_DUCKDB_THREADS:-${SLURM_CPUS_PER_TASK:-4}}"
export IC_DUCKDB_MEMORY_LIMIT="${IC_DUCKDB_MEMORY_LIMIT:-24GB}"

if [ ! -x "$IC_PYTHON" ]; then
    echo "Missing Python environment: $IC_PYTHON"
    exit 1
fi
cd "$IC_REPO_ROOT"
echo "IC_PROJECT_ROOT=$IC_PROJECT_ROOT"
echo "IC_REPO_ROOT=$IC_REPO_ROOT"
echo "IC_PMID_FUTURE_DEVELOPMENT_METRICS_OVERWRITE=${IC_PMID_FUTURE_DEVELOPMENT_METRICS_OVERWRITE:-0}"
"$IC_PYTHON" -u code/knowledge_combination/future_development_metrics/pmid/combine_pmid_future_development_metrics.py
