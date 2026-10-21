#!/bin/bash
#SBATCH --job-name=semmed_openalex_audit
#SBATCH --output=/xdisk/sebratt/jinyugao/research/projects/innovation_capacity/logs/data_validation/semmed_openalex_audit_%j.out
#SBATCH --error=/xdisk/sebratt/jinyugao/research/projects/innovation_capacity/logs/data_validation/semmed_openalex_audit_%j.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=64gb
#SBATCH --time=24:00:00
#SBATCH --account=sebratt
#SBATCH --partition=standard
set -euo pipefail
export IC_PROJECT_ROOT="${IC_PROJECT_ROOT:-/xdisk/sebratt/jinyugao/research/projects/innovation_capacity}"
export IC_REPO_ROOT="${IC_REPO_ROOT:-/xdisk/sebratt/jinyugao/research/repos/innovation_capacity}"
export IC_PYTHON="${IC_PYTHON:-/xdisk/sebratt/jinyugao/research/envs/innovation_capacity/bin/python}"
export IC_DUCKDB_THREADS="${IC_DUCKDB_THREADS:-${SLURM_CPUS_PER_TASK:-8}}"
export IC_DUCKDB_MEMORY_LIMIT="${IC_DUCKDB_MEMORY_LIMIT:-48GB}"
cd "$IC_REPO_ROOT"
"$IC_PYTHON" -u code/data_validation/semmeddb_openalex/audit_semmed_openalex.py "$@"
