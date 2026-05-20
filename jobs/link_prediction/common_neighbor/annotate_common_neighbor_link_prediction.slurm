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

module load python/3.9

REPO_DIR=/xdisk/sebratt/jinyugao/repos/innovation_capacity
LOG_DIR=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/annotate_common_neighbor_link_prediction

mkdir -p "$LOG_DIR"

cd "$REPO_DIR"
python code/link_prediction/common_neighbor/annotate_common_neighbor_link_prediction.py
