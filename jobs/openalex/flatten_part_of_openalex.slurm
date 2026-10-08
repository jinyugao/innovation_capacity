#!/bin/bash
#SBATCH --job-name=flatten_part_of_openalex
#SBATCH --output=/xdisk/sebratt/jinyugao/research/projects/innovation_capacity/logs/openalex/flatten_part_of_openalex_%j.out
#SBATCH --error=/xdisk/sebratt/jinyugao/research/projects/innovation_capacity/logs/openalex/flatten_part_of_openalex_%j.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=128gb
#SBATCH --time=48:00:00
#SBATCH --account=sebratt
#SBATCH --partition=standard

set -euo pipefail

export IC_PROJECT_ROOT="${IC_PROJECT_ROOT:-/xdisk/sebratt/jinyugao/research/projects/innovation_capacity}"
export IC_REPO_ROOT="${IC_REPO_ROOT:-/xdisk/sebratt/jinyugao/research/repos/innovation_capacity}"
export IC_PYTHON="${IC_PYTHON:-/xdisk/sebratt/jinyugao/research/envs/innovation_capacity/bin/python}"
export IC_OPENALEX_SNAPSHOT_DIR="${IC_OPENALEX_SNAPSHOT_DIR:-/contrib/datasets/openalex-snapshot}"
export IC_OPENALEX_PROCESSED_DIR="${IC_OPENALEX_PROCESSED_DIR:-/xdisk/sebratt/jinyugao/research/data/processed/openalex/flattened_snapshot_2025}"
export IC_OPENALEX_WRITE_REFERENCED_WORKS="${IC_OPENALEX_WRITE_REFERENCED_WORKS:-0}"

if [ ! -x "$IC_PYTHON" ]; then
    echo "Missing Python environment: $IC_PYTHON"
    echo "Run scripts/setup_hpc_python_env.sh after cloning the repository."
    exit 1
fi

LOG_DIR="$IC_PROJECT_ROOT/logs/openalex"

mkdir -p "$LOG_DIR"

cd "$IC_REPO_ROOT"

echo "IC_OPENALEX_SNAPSHOT_DIR=$IC_OPENALEX_SNAPSHOT_DIR"
echo "IC_OPENALEX_PROCESSED_DIR=$IC_OPENALEX_PROCESSED_DIR"
echo "IC_OPENALEX_WRITE_REFERENCED_WORKS=$IC_OPENALEX_WRITE_REFERENCED_WORKS"

"$IC_PYTHON" -u code/openalex/flatten_part_of_openalex.py
