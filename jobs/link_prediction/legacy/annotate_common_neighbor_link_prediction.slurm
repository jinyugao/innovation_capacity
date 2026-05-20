#!/bin/bash
#SBATCH --job-name=annotate_common_neighbor_link_prediction
#SBATCH --output=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/annotate_common_neighbor_link_prediction/annotate_common_neighbor_link_prediction_%A_%a.out
#SBATCH --error=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/annotate_common_neighbor_link_prediction/annotate_common_neighbor_link_prediction_%A_%a.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=64gb
#SBATCH --time=12:00:00
#SBATCH --account=sebratt
#SBATCH --partition=standard
#SBATCH --array=0-39

PYTHON=/xdisk/sebratt/jinyugao/envs/innovation_capacity/bin/python

if [ ! -x "$PYTHON" ]; then
    echo "Missing Python environment: $PYTHON"
    echo "Create it with: python3 -m venv /xdisk/sebratt/jinyugao/envs/innovation_capacity"
    echo "Then install dependencies with: /xdisk/sebratt/jinyugao/envs/innovation_capacity/bin/pip install pandas"
    exit 1
fi

REPO_DIR=/xdisk/sebratt/jinyugao/repos/innovation_capacity
LOG_DIR=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/annotate_common_neighbor_link_prediction

mkdir -p "$LOG_DIR"

cd "$REPO_DIR"
"$PYTHON" code/link_prediction/legacy/annotate_common_neighbor_link_prediction.py
