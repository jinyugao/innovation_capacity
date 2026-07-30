#!/bin/bash
#SBATCH --job-name=build_biomedbert_cui_embeddings
#SBATCH --output=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/build_biomedbert_cui_embeddings/build_biomedbert_cui_embeddings_%j.out
#SBATCH --error=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/build_biomedbert_cui_embeddings/build_biomedbert_cui_embeddings_%j.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=128gb
#SBATCH --time=48:00:00
#SBATCH --account=sebratt
#SBATCH --partition=standard

PYTHON=/xdisk/sebratt/jinyugao/envs/innovation_capacity/bin/python
export HF_HOME=/xdisk/sebratt/jinyugao/envs/huggingface_cache

if [ ! -x "$PYTHON" ]; then
    echo "Missing Python environment: $PYTHON"
    echo "Create it with: python3 -m venv /xdisk/sebratt/jinyugao/envs/innovation_capacity"
    echo "Then install dependencies with: /xdisk/sebratt/jinyugao/envs/innovation_capacity/bin/pip install pandas numpy torch transformers"
    exit 1
fi

REPO_DIR=/xdisk/sebratt/jinyugao/repos/innovation_capacity
LOG_DIR=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/build_biomedbert_cui_embeddings

mkdir -p "$LOG_DIR"

cd "$REPO_DIR"
"$PYTHON" code/link_prediction/biomedbert/preparation/build_biomedbert_cui_embeddings.py
