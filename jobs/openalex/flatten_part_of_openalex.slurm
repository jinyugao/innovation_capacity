#!/bin/bash
#SBATCH --job-name=flatten_part_of_openalex
#SBATCH --output=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/flatten_part_of_openalex/flatten_part_of_openalex_%j.out
#SBATCH --error=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/flatten_part_of_openalex/flatten_part_of_openalex_%j.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=128gb
#SBATCH --time=48:00:00
#SBATCH --account=sebratt
#SBATCH --partition=standard

module load python/3.9

REPO_DIR=/xdisk/sebratt/jinyugao/repos/innovation_capacity
LOG_DIR=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/flatten_part_of_openalex

mkdir -p "$LOG_DIR"

cd "$REPO_DIR"
python code/openalex/flatten_part_of_openalex.py
