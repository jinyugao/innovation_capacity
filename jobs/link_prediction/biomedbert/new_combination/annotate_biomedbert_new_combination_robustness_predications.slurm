#!/bin/bash
#SBATCH --job-name=annotate_biomedbert_new_combination_robustness_predications
#SBATCH --output=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/annotate_biomedbert_new_combination_robustness_predications/annotate_biomedbert_new_combination_robustness_predications_%A_%a.out
#SBATCH --error=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/annotate_biomedbert_new_combination_robustness_predications/annotate_biomedbert_new_combination_robustness_predications_%A_%a.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=64gb
#SBATCH --time=08:00:00
#SBATCH --account=sebratt
#SBATCH --partition=standard
#SBATCH --array=0-39

PYTHON=/xdisk/sebratt/jinyugao/envs/innovation_capacity/bin/python

if [ ! -x "$PYTHON" ]; then
    echo "Missing Python environment: $PYTHON"
    echo "Create it with: python3 -m venv /xdisk/sebratt/jinyugao/envs/innovation_capacity"
    echo "Then install dependencies with: /xdisk/sebratt/jinyugao/envs/innovation_capacity/bin/pip install pandas"
    exit 1
fi

REPO_DIR=/xdisk/sebratt/jinyugao/repos/innovation_capacity
LOG_DIR=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/annotate_biomedbert_new_combination_robustness_predications

mkdir -p "$LOG_DIR"

cd "$REPO_DIR"
"$PYTHON" code/link_prediction/biomedbert/new_combination/annotate_biomedbert_new_combination_robustness_predications.py
