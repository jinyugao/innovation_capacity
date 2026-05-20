#!/bin/bash
#SBATCH --job-name=flatten_openalex_to_csvgz
#SBATCH --output=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/flatten_openalex_to_csvgz/flatten_openalex_to_csvgz_%j.out
#SBATCH --error=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/flatten_openalex_to_csvgz/flatten_openalex_to_csvgz_%j.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=128gb
#SBATCH --time=72:00:00
#SBATCH --account=sebratt
#SBATCH --partition=standard

module load python/3.9

REPO_DIR=/xdisk/sebratt/jinyugao/repos/innovation_capacity
LOG_DIR=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/flatten_openalex_to_csvgz

mkdir -p "$LOG_DIR"

cd "$REPO_DIR"
python code/openalex/flatten_openalex_to_csvgz.py
