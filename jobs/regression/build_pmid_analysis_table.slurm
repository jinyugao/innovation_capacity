#!/bin/bash
#SBATCH --job-name=pmid_analysis_table
#SBATCH --output=/xdisk/sebratt/jinyugao/research/projects/innovation_capacity/logs/regression/pmid_analysis_table_%j.out
#SBATCH --error=/xdisk/sebratt/jinyugao/research/projects/innovation_capacity/logs/regression/pmid_analysis_table_%j.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=64gb
#SBATCH --time=24:00:00
#SBATCH --account=sebratt
#SBATCH --partition=standard
set -euo pipefail
export IC_PROJECT_ROOT="${IC_PROJECT_ROOT:-/xdisk/sebratt/jinyugao/research/projects/innovation_capacity}"
export IC_REPO_ROOT="${IC_REPO_ROOT:-/xdisk/sebratt/jinyugao/research/repos/innovation_capacity}"
export IC_PYTHON="${IC_PYTHON:-/xdisk/sebratt/jinyugao/research/envs/innovation_capacity/bin/python}"
export IC_DUCKDB_THREADS="${IC_DUCKDB_THREADS:-${SLURM_CPUS_PER_TASK:-4}}"
export IC_DUCKDB_MEMORY_LIMIT="${IC_DUCKDB_MEMORY_LIMIT:-48GB}"
cd "$IC_REPO_ROOT"
"$IC_PYTHON" -u code/regression/pmid_analysis_table/build_pmid_analysis_table.py "$@"
