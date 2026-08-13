#!/bin/bash
#SBATCH --job-name=combine_new_combination_semantic_proximity_summaries
#SBATCH --output=/xdisk/sebratt/jinyugao/research/projects/innovation_capacity/logs/knowledge_combination/semantic_proximity/combine_new_combination_semantic_proximity_summaries_%j.out
#SBATCH --error=/xdisk/sebratt/jinyugao/research/projects/innovation_capacity/logs/knowledge_combination/semantic_proximity/combine_new_combination_semantic_proximity_summaries_%j.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=8gb
#SBATCH --time=00:20:00
#SBATCH --account=sebratt
#SBATCH --partition=standard

set -euo pipefail

export IC_PROJECT_ROOT="${IC_PROJECT_ROOT:-/xdisk/sebratt/jinyugao/research/projects/innovation_capacity}"
export IC_REPO_ROOT="${IC_REPO_ROOT:-/xdisk/sebratt/jinyugao/research/repos/innovation_capacity}"
export IC_PYTHON="${IC_PYTHON:-/xdisk/sebratt/jinyugao/research/envs/innovation_capacity/bin/python}"

LOG_DIR="$IC_PROJECT_ROOT/logs/knowledge_combination/semantic_proximity"

mkdir -p "$LOG_DIR"

if [ ! -x "$IC_PYTHON" ]; then
    echo "Missing Python environment: $IC_PYTHON"
    echo "Create it with: python3 -m venv /xdisk/sebratt/jinyugao/research/envs/innovation_capacity"
    exit 1
fi

cd "$IC_REPO_ROOT"

echo "IC_PROJECT_ROOT=$IC_PROJECT_ROOT"
echo "IC_REPO_ROOT=$IC_REPO_ROOT"
echo "IC_PYTHON=$IC_PYTHON"
echo "IC_NEW_COMBINATION_SEMANTIC_PROXIMITY_SUMMARY_OVERWRITE=${IC_NEW_COMBINATION_SEMANTIC_PROXIMITY_SUMMARY_OVERWRITE:-0}"

"$IC_PYTHON" -u code/knowledge_combination/semantic_proximity/combine_new_combination_semantic_proximity_summaries.py
