#!/bin/bash
set -euo pipefail

HPC_JOB_DIR=/xdisk/sebratt/jinyugao/repos/innovation_capacity/jobs/link_prediction/biomedbert
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

if [ -d "$HPC_JOB_DIR" ]; then
    JOB_DIR="$HPC_JOB_DIR"
else
    JOB_DIR="$SCRIPT_DIR"
fi

echo "Creating BiomedBERT Slurm log directories from: $JOB_DIR"

find "$JOB_DIR" -name "*.slurm" -type f -print0 |
    while IFS= read -r -d "" slurm_file; do
        sed -n 's/^#SBATCH --\(output\|error\)=//p' "$slurm_file"
    done |
    while IFS= read -r log_file; do
        log_dir=$(dirname "$log_file")
        mkdir -p "$log_dir"
        echo "$log_dir"
    done |
    sort -u

echo "BiomedBERT Slurm log directories are ready."
