#!/bin/bash
#SBATCH --job-name=combine_first_layer_edge_annotation_pmid_profile_citation_correlations
#SBATCH --output=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/combine_first_layer_edge_annotation_pmid_profile_citation_correlations_%j.out
#SBATCH --error=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/combine_first_layer_edge_annotation_pmid_profile_citation_correlations_%j.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=16gb
#SBATCH --time=02:00:00
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
LOG_DIR=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs

mkdir -p "$LOG_DIR"

cd "$REPO_DIR"
"$PYTHON" -u code/link_prediction/summary/combine_first_layer_edge_annotation_pmid_profile_citation_correlations.py
