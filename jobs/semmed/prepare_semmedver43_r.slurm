#!/bin/bash
#SBATCH --job-name=prepare_semmedver43_r
#SBATCH --output=/xdisk/sebratt/jinyugao/research/projects/innovation_capacity/logs/semmed/prepare_semmedver43_r/prepare_semmedver43_r_%j.out
#SBATCH --error=/xdisk/sebratt/jinyugao/research/projects/innovation_capacity/logs/semmed/prepare_semmedver43_r/prepare_semmedver43_r_%j.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=128gb
#SBATCH --time=24:00:00
#SBATCH --account=sebratt
#SBATCH --partition=standard

set -euo pipefail

export IC_SEMMED_SOURCE_DIR="${IC_SEMMED_SOURCE_DIR:-/home/u23/jinyugao/research/data/source/semmedVER43_R}"
export IC_PROJECT_ROOT="${IC_PROJECT_ROOT:-/xdisk/sebratt/jinyugao/research/projects/innovation_capacity}"
export IC_REPO_ROOT="${IC_REPO_ROOT:-/xdisk/sebratt/jinyugao/research/repos/innovation_capacity}"
export IC_PYTHON="${IC_PYTHON:-/home/u23/jinyugao/research/envs/innovation_capacity/bin/python}"
export IC_DUCKDB_THREADS="${IC_DUCKDB_THREADS:-${SLURM_CPUS_PER_TASK:-8}}"
export IC_DUCKDB_MEMORY_LIMIT="${IC_DUCKDB_MEMORY_LIMIT:-110GB}"

LOG_DIR="$IC_PROJECT_ROOT/logs/semmed/prepare_semmedver43_r"

mkdir -p "$LOG_DIR"

if [ ! -x "$IC_PYTHON" ]; then
    echo "Missing Python environment: $IC_PYTHON"
    echo "Create it with: python3 -m venv /home/u23/jinyugao/research/envs/innovation_capacity"
    echo "Then install dependencies with: $IC_PYTHON -m pip install pandas duckdb"
    exit 1
fi

"$IC_PYTHON" - <<'PY'
import duckdb

print(f"duckdb: {duckdb.__version__}")
PY

cd "$IC_REPO_ROOT"

echo "IC_SEMMED_SOURCE_DIR=$IC_SEMMED_SOURCE_DIR"
echo "IC_PROJECT_ROOT=$IC_PROJECT_ROOT"
echo "IC_REPO_ROOT=$IC_REPO_ROOT"
echo "IC_PYTHON=$IC_PYTHON"
echo "IC_DUCKDB_THREADS=$IC_DUCKDB_THREADS"
echo "IC_DUCKDB_MEMORY_LIMIT=$IC_DUCKDB_MEMORY_LIMIT"

"$IC_PYTHON" -u code/semmed/prepare_semmedver43_r.py
