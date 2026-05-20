#!/bin/bash
#SBATCH --job-name=build_openalex_authorships_institutions_workids
#SBATCH --output=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/build_openalex_authorships_institutions_workids/build_openalex_authorships_institutions_workids_%j.out
#SBATCH --error=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/build_openalex_authorships_institutions_workids/build_openalex_authorships_institutions_workids_%j.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=128gb
#SBATCH --time=24:00:00
#SBATCH --account=sebratt
#SBATCH --partition=standard

module load python/3.9

REPO_DIR=/xdisk/sebratt/jinyugao/repos/innovation_capacity
LOG_DIR=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/build_openalex_authorships_institutions_workids

mkdir -p "$LOG_DIR"

cd "$REPO_DIR"
python code/openalex/build_openalex_authorships_institutions_workids.py
