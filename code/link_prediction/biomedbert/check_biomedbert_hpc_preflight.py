"""Check whether the BiomedBERT HPC run is ready to start.

This script is intentionally read-only. Run it on HPC from the repository root
before submitting the BiomedBERT jobs.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path


REPO_DIR = Path("/xdisk/sebratt/jinyugao/repos/innovation_capacity")
PROJECT_DIR = Path("/xdisk/sebratt/jinyugao/projects/innovation_capacity")
INTERIM_DIR = PROJECT_DIR / "data/interim"
RESULTS_DIR = PROJECT_DIR / "results"

BASE_YEAR = 1980
N_YEARS = 40
PREHISTORY_START_YEAR = 1975
FINAL_YEAR = BASE_YEAR + N_YEARS - 1
ROBUSTNESS_PERCENTILES = [1, 5, 20]


REQUIRED_REPO_FILES = [
    "jobs/link_prediction/biomedbert/RUN_ORDER.md",
    "jobs/link_prediction/biomedbert/create_biomedbert_log_dirs.sh",
    "code/link_prediction/biomedbert/check_biomedbert_hpc_preflight.py",
    "jobs/link_prediction/biomedbert/preparation/build_biomedbert_cui_name_frequencies.slurm",
    "jobs/link_prediction/biomedbert/preparation/select_biomedbert_cui_labels.slurm",
    "jobs/link_prediction/biomedbert/preparation/build_biomedbert_cui_embeddings.slurm",
    "jobs/link_prediction/biomedbert/preparation/build_biomedbert_cui_embeddings_gpu.slurm",
    "jobs/link_prediction/biomedbert/new_combination/score_biomedbert_candidate_edges.slurm",
    "jobs/link_prediction/biomedbert/new_combination/build_biomedbert_new_combination_candidate_coverage_summary.slurm",
    "jobs/link_prediction/biomedbert/new_combination/build_biomedbert_new_combination_top_10pct_predicted_edges.slurm",
    "jobs/link_prediction/biomedbert/new_combination/annotate_biomedbert_new_combination_top_10pct_predications.slurm",
    "jobs/link_prediction/biomedbert/new_combination/build_biomedbert_new_combination_evaluation_summary.slurm",
    "jobs/link_prediction/biomedbert/new_combination/build_biomedbert_new_combination_robustness_predicted_edges.slurm",
    "jobs/link_prediction/biomedbert/new_combination/annotate_biomedbert_new_combination_robustness_predications.slurm",
    "jobs/link_prediction/biomedbert/new_combination/build_biomedbert_new_combination_robustness_evaluation_summary.slurm",
    "jobs/link_prediction/biomedbert/new_relation/build_biomedbert_new_relation_candidate_triples.slurm",
    "jobs/link_prediction/biomedbert/new_relation/build_biomedbert_new_relation_candidate_coverage_summary.slurm",
    "jobs/link_prediction/biomedbert/new_relation/score_biomedbert_new_relation_candidate_triples.slurm",
    "jobs/link_prediction/biomedbert/new_relation/score_biomedbert_new_relation_candidate_triples_gpu.slurm",
    "jobs/link_prediction/biomedbert/new_relation/build_biomedbert_new_relation_top_10pct_predicted_triples.slurm",
    "jobs/link_prediction/biomedbert/new_relation/annotate_biomedbert_new_relation_top_10pct_predications.slurm",
    "jobs/link_prediction/biomedbert/new_relation/build_biomedbert_new_relation_evaluation_summary.slurm",
    "jobs/link_prediction/biomedbert/new_relation/build_biomedbert_new_relation_robustness_predicted_triples.slurm",
    "jobs/link_prediction/biomedbert/new_relation/annotate_biomedbert_new_relation_robustness_predications.slurm",
    "jobs/link_prediction/biomedbert/new_relation/build_biomedbert_new_relation_robustness_evaluation_summary.slurm",
    "code/link_prediction/biomedbert/preparation/build_biomedbert_cui_name_frequencies.py",
    "code/link_prediction/biomedbert/preparation/select_biomedbert_cui_labels.py",
    "code/link_prediction/biomedbert/preparation/build_biomedbert_cui_embeddings.py",
    "code/link_prediction/biomedbert/new_combination/score_biomedbert_candidate_edges.py",
    "code/link_prediction/biomedbert/new_combination/build_biomedbert_new_combination_candidate_coverage_summary.py",
    "code/link_prediction/biomedbert/new_combination/build_biomedbert_new_combination_top_10pct_predicted_edges.py",
    "code/link_prediction/biomedbert/new_combination/annotate_biomedbert_new_combination_top_10pct_predications.py",
    "code/link_prediction/biomedbert/new_combination/build_biomedbert_new_combination_evaluation_summary.py",
    "code/link_prediction/biomedbert/new_combination/build_biomedbert_new_combination_robustness_predicted_edges.py",
    "code/link_prediction/biomedbert/new_combination/annotate_biomedbert_new_combination_robustness_predications.py",
    "code/link_prediction/biomedbert/new_combination/build_biomedbert_new_combination_robustness_evaluation_summary.py",
    "code/link_prediction/biomedbert/new_relation/build_biomedbert_new_relation_candidate_triples.py",
    "code/link_prediction/biomedbert/new_relation/build_biomedbert_new_relation_candidate_coverage_summary.py",
    "code/link_prediction/biomedbert/new_relation/score_biomedbert_new_relation_candidate_triples.py",
    "code/link_prediction/biomedbert/new_relation/build_biomedbert_new_relation_top_10pct_predicted_triples.py",
    "code/link_prediction/biomedbert/new_relation/annotate_biomedbert_new_relation_top_10pct_predications.py",
    "code/link_prediction/biomedbert/new_relation/build_biomedbert_new_relation_evaluation_summary.py",
    "code/link_prediction/biomedbert/new_relation/build_biomedbert_new_relation_robustness_predicted_triples.py",
    "code/link_prediction/biomedbert/new_relation/annotate_biomedbert_new_relation_robustness_predications.py",
    "code/link_prediction/biomedbert/new_relation/build_biomedbert_new_relation_robustness_evaluation_summary.py",
]

REQUIRED_PACKAGES = ["pandas", "numpy", "torch", "transformers"]


def yearly_paths(template: Path, start_year: int, end_year: int) -> list[Path]:
    return [
        Path(str(template).format(year=year))
        for year in range(start_year, end_year + 1)
    ]


def status_line(ok: bool, label: str) -> str:
    return f"[{'OK' if ok else 'MISSING'}] {label}"


def percentile_label(percentile: int) -> str:
    return f"{percentile}pct"


def check_files(label: str, paths: list[Path], fatal: bool = True) -> bool:
    missing = [path for path in paths if not path.exists()]
    print(status_line(not missing, label))
    print(f"  checked: {len(paths):,}")

    if missing:
        print(f"  missing: {len(missing):,}")
        for path in missing[:20]:
            print(f"    {path}")
        if len(missing) > 20:
            print(f"    ... {len(missing) - 20:,} more")
    return not (fatal and missing)


def check_packages() -> bool:
    missing = [
        package
        for package in REQUIRED_PACKAGES
        if importlib.util.find_spec(package) is None
    ]
    print(status_line(not missing, "Python package availability"))
    if missing:
        print("  missing packages:")
        for package in missing:
            print(f"    {package}")
    else:
        print(f"  packages: {', '.join(REQUIRED_PACKAGES)}")
    return not missing


def required_log_dirs(slurm_files: list[Path]) -> list[Path]:
    log_dirs = set()
    for slurm_file in slurm_files:
        if not slurm_file.exists():
            continue
        for line in slurm_file.read_text().splitlines():
            for prefix in ["#SBATCH --output=", "#SBATCH --error="]:
                if line.startswith(prefix):
                    log_dirs.add(Path(line.removeprefix(prefix)).parent)
    return sorted(log_dirs)


def build_outputs_that_block_reruns() -> list[Path]:
    outputs = [
        INTERIM_DIR
        / "biomedbert_link_prediction/cui_labels/biomedbert_cui_name_frequencies.csv.gz",
        INTERIM_DIR
        / "biomedbert_link_prediction/cui_labels/biomedbert_cui_labels.csv.gz",
        INTERIM_DIR
        / "biomedbert_link_prediction/cui_embeddings/biomedbert_cui_embeddings.npz",
        INTERIM_DIR
        / "biomedbert_link_prediction/cui_embeddings/biomedbert_cui_embedding_metadata.csv.gz",
        RESULTS_DIR
        / "link_prediction/biomedbert/new_combination/biomedbert_new_combination_candidate_coverage_summary.csv",
        RESULTS_DIR
        / "link_prediction/biomedbert/new_combination/biomedbert_new_combination_evaluation_summary.csv",
        RESULTS_DIR
        / "link_prediction/biomedbert/new_combination/robustness/biomedbert_new_combination_robustness_evaluation_summary.csv",
        RESULTS_DIR
        / "link_prediction/biomedbert/new_relation/biomedbert_new_relation_candidate_coverage_summary.csv",
        RESULTS_DIR
        / "link_prediction/biomedbert/new_relation/biomedbert_new_relation_evaluation_summary.csv",
        RESULTS_DIR
        / "link_prediction/biomedbert/new_relation/robustness/biomedbert_new_relation_robustness_evaluation_summary.csv",
    ]

    outputs.extend(
        yearly_paths(
            INTERIM_DIR
            / "biomedbert_link_prediction/candidate_edges/"
            "biomedbert_scored_candidate_edges_{year}.csv.gz",
            BASE_YEAR,
            FINAL_YEAR,
        )
    )
    outputs.extend(
        yearly_paths(
            INTERIM_DIR
            / "link_prediction/predicted_edges/biomedbert/"
            "biomedbert_new_combination_predicted_edges_top_10pct_{year}.csv.gz",
            BASE_YEAR,
            FINAL_YEAR,
        )
    )
    outputs.extend(
        yearly_paths(
            INTERIM_DIR
            / "link_prediction/annotated_predications/biomedbert/new_combination/10pct/"
            "semmedVER43_R_predications_with_pyear_filtered_biomedbert_new_combination_top_10pct_annotated_{year}.csv.gz",
            BASE_YEAR,
            FINAL_YEAR,
        )
    )
    outputs.extend(
        yearly_paths(
            INTERIM_DIR
            / "biomedbert_link_prediction/new_relation/candidate_triples/"
            "biomedbert_new_relation_candidate_triples_{year}.csv.gz",
            BASE_YEAR,
            FINAL_YEAR,
        )
    )
    outputs.extend(
        yearly_paths(
            INTERIM_DIR
            / "biomedbert_link_prediction/new_relation/scored_candidate_triples/"
            "biomedbert_new_relation_scored_candidate_triples_{year}.csv.gz",
            BASE_YEAR,
            FINAL_YEAR,
        )
    )
    outputs.extend(
        yearly_paths(
            INTERIM_DIR
            / "link_prediction/predicted_triples/biomedbert/new_relation/"
            "biomedbert_new_relation_predicted_triples_top_10pct_{year}.csv.gz",
            BASE_YEAR,
            FINAL_YEAR,
        )
    )
    outputs.extend(
        yearly_paths(
            INTERIM_DIR
            / "link_prediction/annotated_predications/biomedbert/new_relation/10pct/"
            "semmedVER43_R_predications_with_pyear_filtered_biomedbert_new_relation_top_10pct_annotated_{year}.csv.gz",
            BASE_YEAR,
            FINAL_YEAR,
        )
    )

    for percentile in ROBUSTNESS_PERCENTILES:
        label = percentile_label(percentile)
        outputs.extend(
            yearly_paths(
                INTERIM_DIR
                / "link_prediction/predicted_edges/biomedbert/new_combination/robustness/"
                f"{label}/biomedbert_new_combination_robustness_predicted_edges_top_{label}_"
                "{year}.csv.gz",
                BASE_YEAR,
                FINAL_YEAR,
            )
        )
        outputs.extend(
            yearly_paths(
                INTERIM_DIR
                / "link_prediction/annotated_predications/biomedbert/new_combination/robustness/"
                f"{label}/semmedVER43_R_predications_with_pyear_filtered_biomedbert_new_combination_robustness_top_{label}_annotated_"
                "{year}.csv.gz",
                BASE_YEAR,
                FINAL_YEAR,
            )
        )
        outputs.extend(
            yearly_paths(
                INTERIM_DIR
                / "link_prediction/predicted_triples/biomedbert/new_relation/robustness/"
                f"{label}/biomedbert_new_relation_robustness_predicted_triples_top_{label}_"
                "{year}.csv.gz",
                BASE_YEAR,
                FINAL_YEAR,
            )
        )
        outputs.extend(
            yearly_paths(
                INTERIM_DIR
                / "link_prediction/annotated_predications/biomedbert/new_relation/robustness/"
                f"{label}/semmedVER43_R_predications_with_pyear_filtered_biomedbert_new_relation_robustness_top_{label}_annotated_"
                "{year}.csv.gz",
                BASE_YEAR,
                FINAL_YEAR,
            )
        )

    return outputs


def check_torch_cuda() -> None:
    if importlib.util.find_spec("torch") is None:
        return

    import torch

    print("[INFO] Torch/CUDA")
    print(f"  torch version: {torch.__version__}")
    print(f"  cuda available in current process: {torch.cuda.is_available()}")
    if torch.cuda.is_available():
        print(f"  cuda device count: {torch.cuda.device_count()}")
        print(f"  current device: {torch.cuda.get_device_name(0)}")
    else:
        print("  note: CUDA can be false on login nodes; confirm inside GPU jobs.")


def main() -> None:
    print("BiomedBERT HPC preflight")
    print(f"Repo dir: {REPO_DIR}")
    print(f"Project dir: {PROJECT_DIR}")
    print(f"Focal years: {BASE_YEAR}-{FINAL_YEAR}")
    print()

    required_repo_files = [REPO_DIR / path for path in REQUIRED_REPO_FILES]
    required_slurm_files = [
        path for path in required_repo_files if path.suffix == ".slurm"
    ]
    required_inputs = [
        INTERIM_DIR
        / "semmedVER43_R/semmedVER43_2024_R_predications_with_pyear_filtered.csv.gz"
    ]
    split_files = yearly_paths(
        INTERIM_DIR
        / "semmedVER43_R/split_predications_with_pyear_filtered_by_pyear/"
        "semmedVER43_R_predications_with_pyear_filtered_{year}.csv.gz",
        PREHISTORY_START_YEAR,
        FINAL_YEAR,
    )
    first_layer_files = yearly_paths(
        INTERIM_DIR
        / "link_prediction/edge_annotation/first_layer/"
        "semmedVER43_R_predications_with_pyear_filtered_first_layer_edge_annotation_{year}.csv.gz",
        BASE_YEAR,
        FINAL_YEAR,
    )
    two_hop_files = yearly_paths(
        INTERIM_DIR
        / "link_prediction/candidate_edges/two_hop_candidate_edges/"
        "two_hop_candidate_edges_prior_5y_{year}.csv.gz",
        BASE_YEAR,
        FINAL_YEAR,
    )

    outputs_that_block_reruns = build_outputs_that_block_reruns()
    existing_outputs = [path for path in outputs_that_block_reruns if path.exists()]

    ok = True
    ok &= check_files("Required repo files", required_repo_files)
    ok &= check_files(
        "BiomedBERT Slurm log directories",
        required_log_dirs(required_slurm_files),
    )
    ok &= check_files("Filtered SemMedDB full file", required_inputs)
    ok &= check_files("Yearly filtered SemMedDB split files", split_files)
    ok &= check_files("First-layer annotation files", first_layer_files)
    ok &= check_files("Two-hop candidate edge files", two_hop_files)
    ok &= check_packages()
    check_torch_cuda()

    print(status_line(True, "Existing outputs that may block reruns"))
    if existing_outputs:
        print(f"  found: {len(existing_outputs):,}")
        for path in existing_outputs:
            print(f"    {path}")
        print("  note: scripts use OVERWRITE = False by default.")
    else:
        print("  none among checked BiomedBERT outputs")

    print()
    if ok:
        print("PRECHECK PASSED: required code, inputs, and packages are present.")
    else:
        print("PRECHECK FAILED: fix missing items before submitting jobs.")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
