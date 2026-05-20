#!/bin/bash
#SBATCH --job-name=split_semmedver43_r_predications_by_pyear
#SBATCH --output=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/split_semmedver43_r_predications_by_pyear/split_semmedver43_r_predications_by_pyear_%j.out
#SBATCH --error=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/split_semmedver43_r_predications_by_pyear/split_semmedver43_r_predications_by_pyear_%j.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=128gb
#SBATCH --time=24:00:00
#SBATCH --account=sebratt
#SBATCH --partition=standard

module load python/3.9

REPO_DIR=/xdisk/sebratt/jinyugao/repos/innovation_capacity
LOG_DIR=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/split_semmedver43_r_predications_by_pyear

mkdir -p "$LOG_DIR"

cd "$REPO_DIR"
python code/semmed/split_semmedver43_r_predications_by_pyear.py
