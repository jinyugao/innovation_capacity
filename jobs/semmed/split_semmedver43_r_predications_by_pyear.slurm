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

PYTHON=/xdisk/sebratt/jinyugao/envs/innovation_capacity/bin/python

if [ ! -x "$PYTHON" ]; then
    echo "Missing Python environment: $PYTHON"
    echo "Create it with: python3 -m venv /xdisk/sebratt/jinyugao/envs/innovation_capacity"
    echo "Then install dependencies with: /xdisk/sebratt/jinyugao/envs/innovation_capacity/bin/pip install pandas"
    exit 1
fi

REPO_DIR=/xdisk/sebratt/jinyugao/repos/innovation_capacity
LOG_DIR=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/split_semmedver43_r_predications_by_pyear

mkdir -p "$LOG_DIR"

cd "$REPO_DIR"
"$PYTHON" code/semmed/split_semmedver43_r_predications_by_pyear.py
