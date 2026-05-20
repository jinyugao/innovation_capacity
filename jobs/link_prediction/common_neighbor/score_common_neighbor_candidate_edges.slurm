#!/bin/bash
#SBATCH --job-name=score_common_neighbor_candidate_edges
#SBATCH --output=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/score_common_neighbor_candidate_edges/score_common_neighbor_candidate_edges_%A_%a.out
#SBATCH --error=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/score_common_neighbor_candidate_edges/score_common_neighbor_candidate_edges_%A_%a.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=128gb
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
LOG_DIR=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/score_common_neighbor_candidate_edges

mkdir -p "$LOG_DIR"

cd "$REPO_DIR"
"$PYTHON" code/link_prediction/common_neighbor/score_common_neighbor_candidate_edges.py
