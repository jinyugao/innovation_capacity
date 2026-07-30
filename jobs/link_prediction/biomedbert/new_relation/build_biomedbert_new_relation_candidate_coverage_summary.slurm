#!/bin/bash
#SBATCH --job-name=build_biomedbert_new_relation_candidate_coverage_summary
#SBATCH --output=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/build_biomedbert_new_relation_candidate_coverage_summary/build_biomedbert_new_relation_candidate_coverage_summary_%j.out
#SBATCH --error=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/build_biomedbert_new_relation_candidate_coverage_summary/build_biomedbert_new_relation_candidate_coverage_summary_%j.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=64gb
#SBATCH --time=04:00:00
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
LOG_DIR=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/build_biomedbert_new_relation_candidate_coverage_summary

mkdir -p "$LOG_DIR"

cd "$REPO_DIR"
"$PYTHON" code/link_prediction/biomedbert/new_relation/build_biomedbert_new_relation_candidate_coverage_summary.py
