#!/bin/bash
set -euo pipefail

# Create or update the Python environment used by the active HPC workflows.

XDISK_RESEARCH_ROOT="${IC_XDISK_RESEARCH_ROOT:-/xdisk/sebratt/jinyugao/research}"
ENV_DIR="${IC_ENV_DIR:-$XDISK_RESEARCH_ROOT/envs/innovation_capacity}"
REPO_DIR="${IC_REPO_ROOT:-$XDISK_RESEARCH_ROOT/repos/innovation_capacity}"
PROJECT_ROOT="${IC_PROJECT_ROOT:-$XDISK_RESEARCH_ROOT/projects/innovation_capacity}"
HF_HOME="${HF_HOME:-$XDISK_RESEARCH_ROOT/envs/huggingface_cache}"
MODEL_NAME="${IC_BIOMEDBERT_MODEL_NAME:-microsoft/BiomedNLP-BiomedBERT-base-uncased-abstract-fulltext}"
SYSTEM_PYTHON="${IC_SYSTEM_PYTHON:-python3}"
DOWNLOAD_BIOMEDBERT="${IC_DOWNLOAD_BIOMEDBERT:-1}"

if ! command -v "$SYSTEM_PYTHON" >/dev/null 2>&1; then
    echo "$SYSTEM_PYTHON was not found on this system."
    echo "Run 'module spider python' on HPC and load an available Python module."
    exit 1
fi

if [ ! -d "$REPO_DIR" ]; then
    echo "Missing repository directory: $REPO_DIR"
    echo "Clone the GitHub repository before running this script."
    exit 1
fi

mkdir -p \
    "$XDISK_RESEARCH_ROOT/data" \
    "$XDISK_RESEARCH_ROOT/envs" \
    "$XDISK_RESEARCH_ROOT/repos" \
    "$XDISK_RESEARCH_ROOT/projects" \
    "$PROJECT_ROOT" \
    "$HF_HOME"

# Slurm opens output and error files before the job script can create them.
mkdir -p \
    "$PROJECT_ROOT/logs/semmed/prepare_semmedver43_r" \
    "$PROJECT_ROOT/logs/knowledge_combination/sample_construction" \
    "$PROJECT_ROOT/logs/knowledge_combination/edge_annotation" \
    "$PROJECT_ROOT/logs/knowledge_combination/semantic_proximity" \
    "$PROJECT_ROOT/logs/knowledge_combination/edge_transition" \
    "$PROJECT_ROOT/logs/knowledge_combination/future_development" \
    "$PROJECT_ROOT/logs/knowledge_combination/country_adoption" \
    "$PROJECT_ROOT/logs/knowledge_combination/parallel_discovery" \
    "$PROJECT_ROOT/logs/openalex"

if [ ! -d "$ENV_DIR" ]; then
    echo "Creating Python environment: $ENV_DIR"
    "$SYSTEM_PYTHON" -m venv "$ENV_DIR"
else
    echo "Updating existing Python environment: $ENV_DIR"
fi

PYTHON="$ENV_DIR/bin/python"

"$PYTHON" -m pip install --upgrade pip setuptools wheel
"$PYTHON" -m pip install \
    numpy==2.0.2 \
    pandas==2.3.3 \
    duckdb==1.4.5 \
    pyarrow==21.0.0 \
    torch==2.8.0 \
    transformers==4.57.6 \
    tokenizers \
    safetensors \
    tqdm

export HF_HOME

"$PYTHON" - <<'PY'
import duckdb
import numpy
import pandas
import pyarrow
import safetensors
import tokenizers
import torch
import tqdm
import transformers

print("Environment check passed.")
print(f"numpy: {numpy.__version__}")
print(f"pandas: {pandas.__version__}")
print(f"duckdb: {duckdb.__version__}")
print(f"pyarrow: {pyarrow.__version__}")
print(f"torch: {torch.__version__}")
print(f"cuda available on this node: {torch.cuda.is_available()}")
print(f"transformers: {transformers.__version__}")
print(f"tokenizers: {tokenizers.__version__}")
print(f"safetensors: {safetensors.__version__}")
print(f"tqdm: {tqdm.__version__}")
PY

if [[ "$DOWNLOAD_BIOMEDBERT" =~ ^(1|true|TRUE|yes|YES)$ ]]; then
    "$PYTHON" - <<PY
from huggingface_hub import snapshot_download

model_name = "$MODEL_NAME"
print(f"Downloading or checking cached BiomedBERT model: {model_name}")
snapshot_download(
    repo_id=model_name,
    allow_patterns=[
        "config.json",
        "model.safetensors",
        "pytorch_model.bin",
        "special_tokens_map.json",
        "tokenizer.json",
        "tokenizer_config.json",
        "vocab.txt",
    ],
)
print("BiomedBERT model cache is ready.")
PY
else
    echo "Skipping BiomedBERT cache setup because IC_DOWNLOAD_BIOMEDBERT=$DOWNLOAD_BIOMEDBERT."
fi

echo "HPC Python environment setup complete."
echo "IC_REPO_ROOT=$REPO_DIR"
echo "IC_PROJECT_ROOT=$PROJECT_ROOT"
echo "IC_PYTHON=$PYTHON"
echo "HF_HOME=$HF_HOME"
