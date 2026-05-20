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

PYTHON=/xdisk/sebratt/jinyugao/envs/innovation_capacity/bin/python

if [ ! -x "$PYTHON" ]; then
    echo "Missing Python environment: $PYTHON"
    echo "Create it with: python3 -m venv /xdisk/sebratt/jinyugao/envs/innovation_capacity"
    echo "Then install dependencies with: /xdisk/sebratt/jinyugao/envs/innovation_capacity/bin/pip install pandas"
    exit 1
fi

REPO_DIR=/xdisk/sebratt/jinyugao/repos/innovation_capacity
LOG_DIR=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/prepare_semmedver43_r

mkdir -p "$LOG_DIR"

cd "$REPO_DIR"
"$PYTHON" code/semmed/prepare_semmedver43_r.py
