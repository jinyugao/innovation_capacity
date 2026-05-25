#!/bin/bash
#SBATCH --job-name=build_openalex_work_citations
#SBATCH --output=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/build_openalex_work_citations/build_openalex_work_citations_%j.out
#SBATCH --error=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/build_openalex_work_citations/build_openalex_work_citations_%j.err
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
    echo "Create it with: python3 -m venv /xdisk/sebratt/jinyugao/envs/innovation_capacity"
    echo "Then install dependencies with: /xdisk/sebratt/jinyugao/envs/innovation_capacity/bin/pip install pandas"
    exit 1
fi

REPO_DIR=/xdisk/sebratt/jinyugao/repos/innovation_capacity
LOG_DIR=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/build_openalex_work_citations

mkdir -p "$LOG_DIR"

cd "$REPO_DIR"
"$PYTHON" code/openalex/build_openalex_work_citations.py
