#!/bin/bash
#SBATCH --job-name=build_common_neighbor_link_prediction_candidates
#SBATCH --output=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/build_common_neighbor_link_prediction_candidates/build_common_neighbor_link_prediction_candidates_%A_%a.out
#SBATCH --error=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/build_common_neighbor_link_prediction_candidates/build_common_neighbor_link_prediction_candidates_%A_%a.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=128gb
#SBATCH --time=12:00:00
#SBATCH --account=sebratt
#SBATCH --partition=standard
#SBATCH --array=0-39

module load python/3.9

REPO_DIR=/xdisk/sebratt/jinyugao/repos/innovation_capacity
LOG_DIR=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/build_common_neighbor_link_prediction_candidates

mkdir -p "$LOG_DIR"

cd "$REPO_DIR"
python code/link_prediction/common_neighbor/build_common_neighbor_link_prediction_candidates.py
