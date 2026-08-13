#!/bin/bash
#SBATCH --job-name=split_semmeddb_analysis_sample
#SBATCH --output=/xdisk/sebratt/jinyugao/research/projects/innovation_capacity/logs/knowledge_combination/sample_construction/split_semmeddb_analysis_sample_by_pyear_%j.out
#SBATCH --error=/xdisk/sebratt/jinyugao/research/projects/innovation_capacity/logs/knowledge_combination/sample_construction/split_semmeddb_analysis_sample_by_pyear_%j.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=64gb
#SBATCH --time=04:00:00
#SBATCH --account=sebratt
#SBATCH --partition=standard

set -euo pipefail

export IC_PROJECT_ROOT="${IC_PROJECT_ROOT:-/xdisk/sebratt/jinyugao/research/projects/innovation_capacity}"
export IC_REPO_ROOT="${IC_REPO_ROOT:-/xdisk/sebratt/jinyugao/research/repos/innovation_capacity}"
export IC_PYTHON="${IC_PYTHON:-/xdisk/sebratt/jinyugao/research/envs/innovation_capacity/bin/python}"
export IC_DUCKDB_THREADS="${IC_DUCKDB_THREADS:-${SLURM_CPUS_PER_TASK:-8}}"
export IC_DUCKDB_MEMORY_LIMIT="${IC_DUCKDB_MEMORY_LIMIT:-50GB}"

LOG_DIR="$IC_PROJECT_ROOT/logs/knowledge_combination/sample_construction"

mkdir -p "$LOG_DIR"

if [ ! -x "$IC_PYTHON" ]; then
    echo "Missing Python environment: $IC_PYTHON"
    echo "Create it with: python3 -m venv /xdisk/sebratt/jinyugao/research/envs/innovation_capacity"
    echo "Then install dependencies with: $IC_PYTHON -m pip install pandas duckdb"
    exit 1
fi

"$IC_PYTHON" - <<'PY'
import duckdb

print(f"duckdb: {duckdb.__version__}")
PY

cd "$IC_REPO_ROOT"

echo "IC_PROJECT_ROOT=$IC_PROJECT_ROOT"
echo "IC_REPO_ROOT=$IC_REPO_ROOT"
echo "IC_PYTHON=$IC_PYTHON"
echo "IC_DUCKDB_THREADS=$IC_DUCKDB_THREADS"
echo "IC_DUCKDB_MEMORY_LIMIT=$IC_DUCKDB_MEMORY_LIMIT"
echo "IC_SPLIT_ANALYSIS_SAMPLE_OVERWRITE=${IC_SPLIT_ANALYSIS_SAMPLE_OVERWRITE:-0}"

"$IC_PYTHON" -u code/knowledge_combination/sample_construction/split_semmeddb_analysis_sample_by_pyear.py
