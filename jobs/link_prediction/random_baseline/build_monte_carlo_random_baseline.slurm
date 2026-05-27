#!/bin/bash
#SBATCH --job-name=build_monte_carlo_random_baseline
#SBATCH --output=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/build_monte_carlo_random_baseline_%j.out
#SBATCH --error=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/build_monte_carlo_random_baseline_%j.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=8gb
#SBATCH --time=01:00:00
#SBATCH --account=sebratt
#SBATCH --partition=standard

PYTHON=/xdisk/sebratt/jinyugao/envs/innovation_capacity/bin/python

if [ ! -x "$PYTHON" ]; then
    echo "Missing Python environment: $PYTHON"
    echo "Create it with: python3 -m venv /xdisk/sebratt/jinyugao/envs/innovation_capacity"
    echo "Then install dependencies with: /xdisk/sebratt/jinyugao/envs/innovation_capacity/bin/pip install pandas numpy"
    exit 1
fi

REPO_DIR=/xdisk/sebratt/jinyugao/repos/innovation_capacity
LOG_DIR=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/build_monte_carlo_random_baseline

mkdir -p "$LOG_DIR"

cd "$REPO_DIR"
"$PYTHON" code/link_prediction/random_baseline/build_monte_carlo_random_baseline.py
