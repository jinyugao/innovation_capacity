#!/bin/bash
#SBATCH --job-name=build_new_node_future_development
#SBATCH --output=/xdisk/sebratt/jinyugao/research/projects/innovation_capacity/logs/knowledge_combination/future_development/build_new_node_future_development_%A_%a.out
#SBATCH --error=/xdisk/sebratt/jinyugao/research/projects/innovation_capacity/logs/knowledge_combination/future_development/build_new_node_future_development_%A_%a.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=64gb
#SBATCH --time=04:00:00
#SBATCH --account=sebratt
#SBATCH --partition=standard
#SBATCH --array=0-38

set -euo pipefail

export IC_PROJECT_ROOT="${IC_PROJECT_ROOT:-/xdisk/sebratt/jinyugao/research/projects/innovation_capacity}"
export IC_REPO_ROOT="${IC_REPO_ROOT:-/xdisk/sebratt/jinyugao/research/repos/innovation_capacity}"
export IC_PYTHON="${IC_PYTHON:-/xdisk/sebratt/jinyugao/research/envs/innovation_capacity/bin/python}"
export IC_DUCKDB_THREADS="${IC_DUCKDB_THREADS:-4}"
export IC_DUCKDB_MEMORY_LIMIT="${IC_DUCKDB_MEMORY_LIMIT:-56GB}"

LOG_DIR="$IC_PROJECT_ROOT/logs/knowledge_combination/future_development"

mkdir -p "$LOG_DIR"

if [ ! -x "$IC_PYTHON" ]; then
    echo "Missing Python environment: $IC_PYTHON"
    echo "Create it with: python3 -m venv /xdisk/sebratt/jinyugao/research/envs/innovation_capacity"
    echo "Then install dependencies with: $IC_PYTHON -m pip install duckdb pyarrow"
    exit 1
fi

"$IC_PYTHON" - <<'PY'
import duckdb
import pyarrow

print(f"duckdb: {duckdb.__version__}")
print(f"pyarrow: {pyarrow.__version__}")
PY

cd "$IC_REPO_ROOT"

echo "IC_PROJECT_ROOT=$IC_PROJECT_ROOT"
echo "IC_REPO_ROOT=$IC_REPO_ROOT"
echo "IC_PYTHON=$IC_PYTHON"
echo "IC_DUCKDB_THREADS=$IC_DUCKDB_THREADS"
echo "IC_DUCKDB_MEMORY_LIMIT=$IC_DUCKDB_MEMORY_LIMIT"
echo "IC_NEW_NODE_FUTURE_DEVELOPMENT_OVERWRITE=${IC_NEW_NODE_FUTURE_DEVELOPMENT_OVERWRITE:-0}"
echo "SLURM_ARRAY_TASK_ID=${SLURM_ARRAY_TASK_ID:-}"

"$IC_PYTHON" -u code/knowledge_combination/future_development/build_new_node_future_development.py
