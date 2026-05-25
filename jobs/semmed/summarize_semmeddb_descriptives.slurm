#!/bin/bash
#SBATCH --job-name=summarize_semmeddb_descriptives
#SBATCH --output=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/summarize_semmeddb_descriptives/summarize_semmeddb_descriptives_%j.out
#SBATCH --error=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/summarize_semmeddb_descriptives/summarize_semmeddb_descriptives_%j.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=256gb
#SBATCH --time=36:00:00
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
LOG_DIR=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/summarize_semmeddb_descriptives

mkdir -p "$LOG_DIR"

cd "$REPO_DIR"
"$PYTHON" code/semmed/summarize_semmeddb_descriptives.py
