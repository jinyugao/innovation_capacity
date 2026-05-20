#!/bin/bash
#SBATCH --job-name=prepare_semmedver43_r
#SBATCH --output=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/prepare_semmedver43_r/prepare_semmedver43_r_%j.out
#SBATCH --error=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/prepare_semmedver43_r/prepare_semmedver43_r_%j.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=128gb
#SBATCH --time=24:00:00
#SBATCH --account=sebratt
#SBATCH --partition=standard

module load python/3.9

REPO_DIR=/xdisk/sebratt/jinyugao/repos/innovation_capacity
LOG_DIR=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/prepare_semmedver43_r

mkdir -p "$LOG_DIR"

cd "$REPO_DIR"
python code/semmed/prepare_semmedver43_r.py
