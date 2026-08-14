#!/bin/bash
#SBATCH --job-name=build_parallel_discovery_summary
#SBATCH --output=/xdisk/sebratt/jinyugao/research/projects/innovation_capacity/logs/knowledge_combination/parallel_discovery/build_parallel_discovery_summary_%j.out
#SBATCH --error=/xdisk/sebratt/jinyugao/research/projects/innovation_capacity/logs/knowledge_combination/parallel_discovery/build_parallel_discovery_summary_%j.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=96gb
#SBATCH --time=04:00:00
#SBATCH --account=sebratt
#SBATCH --partition=standard

set -euo pipefail

export IC_PROJECT_ROOT="${IC_PROJECT_ROOT:-/xdisk/sebratt/jinyugao/research/projects/innovation_capacity}"
export IC_REPO_ROOT="${IC_REPO_ROOT:-/xdisk/sebratt/jinyugao/research/repos/innovation_capacity}"
export IC_PYTHON="${IC_PYTHON:-/xdisk/sebratt/jinyugao/research/envs/innovation_capacity/bin/python}"
export IC_DUCKDB_THREADS="${IC_DUCKDB_THREADS:-${SLURM_CPUS_PER_TASK:-8}}"
export IC_DUCKDB_MEMORY_LIMIT="${IC_DUCKDB_MEMORY_LIMIT:-84GB}"
export IC_PARALLEL_DISCOVERY_TOP_N="${IC_PARALLEL_DISCOVERY_TOP_N:-20}"
export IC_PARALLEL_DISCOVERY_EXAMPLE_PMID_LIMIT="${IC_PARALLEL_DISCOVERY_EXAMPLE_PMID_LIMIT:-20}"

LOG_DIR="$IC_PROJECT_ROOT/logs/knowledge_combination/parallel_discovery"

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
echo "IC_PARALLEL_DISCOVERY_OVERWRITE=${IC_PARALLEL_DISCOVERY_OVERWRITE:-0}"
echo "IC_PARALLEL_DISCOVERY_TOP_N=$IC_PARALLEL_DISCOVERY_TOP_N"
echo "IC_PARALLEL_DISCOVERY_EXAMPLE_PMID_LIMIT=$IC_PARALLEL_DISCOVERY_EXAMPLE_PMID_LIMIT"

"$IC_PYTHON" -u code/knowledge_combination/parallel_discovery/build_parallel_discovery_summary.py
