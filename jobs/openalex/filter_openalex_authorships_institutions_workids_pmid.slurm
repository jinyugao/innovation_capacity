#!/bin/bash
#SBATCH --job-name=filter_openalex_authorships_institutions_workids_pmid
#SBATCH --output=/xdisk/sebratt/jinyugao/research/projects/innovation_capacity/logs/openalex/filter_openalex_authorships_institutions_workids_pmid_%j.out
#SBATCH --error=/xdisk/sebratt/jinyugao/research/projects/innovation_capacity/logs/openalex/filter_openalex_authorships_institutions_workids_pmid_%j.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=64gb
#SBATCH --time=12:00:00
#SBATCH --account=sebratt
#SBATCH --partition=standard

set -euo pipefail

export IC_PROJECT_ROOT="${IC_PROJECT_ROOT:-/xdisk/sebratt/jinyugao/research/projects/innovation_capacity}"
export IC_REPO_ROOT="${IC_REPO_ROOT:-/xdisk/sebratt/jinyugao/research/repos/innovation_capacity}"
export IC_PYTHON="${IC_PYTHON:-/xdisk/sebratt/jinyugao/research/envs/innovation_capacity/bin/python}"
export IC_OPENALEX_PROCESSED_DIR="${IC_OPENALEX_PROCESSED_DIR:-/xdisk/sebratt/jinyugao/research/data/processed/openalex/flattened_snapshot_2025}"

if [ ! -x "$IC_PYTHON" ]; then
    echo "Missing Python environment: $IC_PYTHON"
    echo "Run scripts/setup_hpc_python_env.sh after cloning the repository."
    exit 1
fi

LOG_DIR="$IC_PROJECT_ROOT/logs/openalex"

mkdir -p "$LOG_DIR"

cd "$IC_REPO_ROOT"

echo "IC_OPENALEX_PROCESSED_DIR=$IC_OPENALEX_PROCESSED_DIR"

"$IC_PYTHON" -u code/openalex/filter_openalex_authorships_institutions_workids_pmid.py
