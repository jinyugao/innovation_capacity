#!/bin/bash
#SBATCH --job-name=sciscinet_papers_pmid
#SBATCH --output=/xdisk/sebratt/jinyugao/research/projects/innovation_capacity/logs/sciscinet_v2/build_sciscinet_papers_pmid_%j.out
#SBATCH --error=/xdisk/sebratt/jinyugao/research/projects/innovation_capacity/logs/sciscinet_v2/build_sciscinet_papers_pmid_%j.err
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
"$IC_PYTHON" -u code/sciscinet_v2/build_sciscinet_papers_pmid.py
