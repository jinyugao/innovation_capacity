#!/bin/bash
#SBATCH --job-name=build_openalex_first_last_author_institutions_pmid
#SBATCH --output=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/build_openalex_first_last_author_institutions_pmid/build_openalex_first_last_author_institutions_pmid_%j.out
#SBATCH --error=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/build_openalex_first_last_author_institutions_pmid/build_openalex_first_last_author_institutions_pmid_%j.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=64gb
#SBATCH --time=12:00:00
#SBATCH --account=sebratt
#SBATCH --partition=standard

module load python/3.9

REPO_DIR=/xdisk/sebratt/jinyugao/repos/innovation_capacity
LOG_DIR=/xdisk/sebratt/jinyugao/projects/innovation_capacity/logs/build_openalex_first_last_author_institutions_pmid

mkdir -p "$LOG_DIR"

cd "$REPO_DIR"
python code/openalex/build_openalex_first_last_author_institutions_pmid.py
