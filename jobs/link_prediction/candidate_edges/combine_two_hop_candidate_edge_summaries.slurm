#!/bin/bash
#SBATCH --job-name=combine_two_hop_candidate_edge_summaries
#SBATCH --output=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/combine_two_hop_candidate_edge_summaries/combine_two_hop_candidate_edge_summaries_%j.out
#SBATCH --error=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/combine_two_hop_candidate_edge_summaries/combine_two_hop_candidate_edge_summaries_%j.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=8gb
#SBATCH --time=00:30:00
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
LOG_DIR=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/combine_two_hop_candidate_edge_summaries

mkdir -p "$LOG_DIR"

cd "$REPO_DIR"
"$PYTHON" code/link_prediction/candidate_edges/combine_two_hop_candidate_edge_summaries.py
