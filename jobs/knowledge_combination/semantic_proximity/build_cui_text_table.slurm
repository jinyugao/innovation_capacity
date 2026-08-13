#!/bin/bash
#SBATCH --job-name=build_cui_text_table
#SBATCH --output=/xdisk/sebratt/jinyugao/research/projects/innovation_capacity/logs/knowledge_combination/semantic_proximity/build_cui_text_table_%j.out
#SBATCH --error=/xdisk/sebratt/jinyugao/research/projects/innovation_capacity/logs/knowledge_combination/semantic_proximity/build_cui_text_table_%j.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=32gb
#SBATCH --time=01:00:00
#SBATCH --account=sebratt
#SBATCH --partition=standard

set -euo pipefail

export IC_PROJECT_ROOT="${IC_PROJECT_ROOT:-/xdisk/sebratt/jinyugao/research/projects/innovation_capacity}"
export IC_REPO_ROOT="${IC_REPO_ROOT:-/xdisk/sebratt/jinyugao/research/repos/innovation_capacity}"
export IC_PYTHON="${IC_PYTHON:-/xdisk/sebratt/jinyugao/research/envs/innovation_capacity/bin/python}"
export IC_DUCKDB_THREADS="${IC_DUCKDB_THREADS:-${SLURM_CPUS_PER_TASK:-4}}"
export IC_DUCKDB_MEMORY_LIMIT="${IC_DUCKDB_MEMORY_LIMIT:-24GB}"

LOG_DIR="$IC_PROJECT_ROOT/logs/knowledge_combination/semantic_proximity"

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
echo "IC_CUI_TEXT_TABLE_OVERWRITE=${IC_CUI_TEXT_TABLE_OVERWRITE:-0}"

"$IC_PYTHON" -u code/knowledge_combination/semantic_proximity/build_cui_text_table.py
