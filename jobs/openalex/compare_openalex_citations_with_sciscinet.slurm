#!/bin/bash
#SBATCH --job-name=compare_openalex_citations_sciscinet_2010
#SBATCH --output=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/compare_openalex_citations_sciscinet_2010_%j.out
#SBATCH --error=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/compare_openalex_citations_sciscinet_2010_%j.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=256gb
#SBATCH --time=72:00:00
#SBATCH --account=sebratt
#SBATCH --partition=standard

PYTHON=/xdisk/sebratt/jinyugao/envs/innovation_capacity/bin/python

if [ ! -x "$PYTHON" ]; then
    echo "Missing Python environment: $PYTHON"
    exit 1
fi

if ! "$PYTHON" -c "import duckdb" 2>/dev/null; then
    echo "Missing Python package: duckdb"
    echo "Install it with:"
    echo "/xdisk/sebratt/jinyugao/envs/innovation_capacity/bin/pip install duckdb"
    exit 1
fi

REPO_DIR=/xdisk/sebratt/jinyugao/repos/innovation_capacity
LOG_DIR=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs

mkdir -p "$LOG_DIR"

cd "$REPO_DIR"
"$PYTHON" -u code/openalex/compare_openalex_citations_with_sciscinet.py
