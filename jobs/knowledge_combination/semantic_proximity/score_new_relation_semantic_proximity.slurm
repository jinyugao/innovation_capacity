#!/bin/bash
#SBATCH --job-name=score_new_relation_semantic_proximity
#SBATCH --output=/xdisk/sebratt/jinyugao/research/projects/innovation_capacity/logs/knowledge_combination/semantic_proximity/score_new_relation_semantic_proximity_%A_%a.out
#SBATCH --error=/xdisk/sebratt/jinyugao/research/projects/innovation_capacity/logs/knowledge_combination/semantic_proximity/score_new_relation_semantic_proximity_%A_%a.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=96gb
#SBATCH --time=12:00:00
#SBATCH --account=sebratt
#SBATCH --partition=gpu_standard
#SBATCH --gres=gpu:1
#SBATCH --array=0-39

set -euo pipefail

export IC_PROJECT_ROOT="${IC_PROJECT_ROOT:-/xdisk/sebratt/jinyugao/research/projects/innovation_capacity}"
export IC_REPO_ROOT="${IC_REPO_ROOT:-/xdisk/sebratt/jinyugao/research/repos/innovation_capacity}"
export IC_PYTHON="${IC_PYTHON:-/xdisk/sebratt/jinyugao/research/envs/innovation_capacity/bin/python}"
export HF_HOME="${HF_HOME:-/xdisk/sebratt/jinyugao/research/envs/huggingface_cache}"
export REQUIRE_CUDA="${REQUIRE_CUDA:-1}"
export IC_NEW_RELATION_BIOMEDBERT_BATCH_SIZE="${IC_NEW_RELATION_BIOMEDBERT_BATCH_SIZE:-128}"
export IC_NEW_RELATION_BIOMEDBERT_MAX_LENGTH="${IC_NEW_RELATION_BIOMEDBERT_MAX_LENGTH:-96}"
export IC_SEMANTIC_PROXIMITY_BATCH_SIZE="${IC_SEMANTIC_PROXIMITY_BATCH_SIZE:-500000}"

LOG_DIR="$IC_PROJECT_ROOT/logs/knowledge_combination/semantic_proximity"

mkdir -p "$LOG_DIR"

if [ ! -x "$IC_PYTHON" ]; then
    echo "Missing Python environment: $IC_PYTHON"
    echo "Create it with: python3 -m venv /xdisk/sebratt/jinyugao/research/envs/innovation_capacity"
    echo "Then install dependencies with: $IC_PYTHON -m pip install numpy pandas duckdb pyarrow torch transformers tokenizers safetensors"
    exit 1
fi

"$IC_PYTHON" - <<'PY'
import duckdb
import numpy
import pandas
import pyarrow
import torch
import transformers

print(f"duckdb: {duckdb.__version__}")
print(f"numpy: {numpy.__version__}")
print(f"pandas: {pandas.__version__}")
print(f"pyarrow: {pyarrow.__version__}")
print(f"torch: {torch.__version__}")
print(f"cuda available: {torch.cuda.is_available()}")
print(f"cuda version: {torch.version.cuda}")
if torch.cuda.is_available():
    print(f"gpu: {torch.cuda.get_device_name(0)}")
print(f"transformers: {transformers.__version__}")
PY

cd "$IC_REPO_ROOT"

echo "Host: $(hostname)"
echo "CUDA_VISIBLE_DEVICES=${CUDA_VISIBLE_DEVICES:-}"
echo "IC_PROJECT_ROOT=$IC_PROJECT_ROOT"
echo "IC_REPO_ROOT=$IC_REPO_ROOT"
echo "IC_PYTHON=$IC_PYTHON"
echo "HF_HOME=$HF_HOME"
echo "REQUIRE_CUDA=$REQUIRE_CUDA"
echo "IC_NEW_RELATION_BIOMEDBERT_BATCH_SIZE=$IC_NEW_RELATION_BIOMEDBERT_BATCH_SIZE"
echo "IC_NEW_RELATION_BIOMEDBERT_MAX_LENGTH=$IC_NEW_RELATION_BIOMEDBERT_MAX_LENGTH"
echo "IC_SEMANTIC_PROXIMITY_BATCH_SIZE=$IC_SEMANTIC_PROXIMITY_BATCH_SIZE"
echo "IC_NEW_RELATION_SEMANTIC_PROXIMITY_OVERWRITE=${IC_NEW_RELATION_SEMANTIC_PROXIMITY_OVERWRITE:-0}"
echo "SLURM_ARRAY_TASK_ID=${SLURM_ARRAY_TASK_ID:-}"

"$IC_PYTHON" -u code/knowledge_combination/semantic_proximity/score_new_relation_semantic_proximity.py
