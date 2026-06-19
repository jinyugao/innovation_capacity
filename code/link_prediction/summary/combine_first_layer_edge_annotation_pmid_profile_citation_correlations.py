"""Combine yearly PMID-profile citation correlation summaries."""

from __future__ import annotations

from pathlib import Path

import pandas as pd


PROJECT_DIR = Path("/xdisk/sebratt/jinyugao/projects/innovation_capacity")
INTERIM_DIR = PROJECT_DIR / "data/interim"
RESULTS_DIR = PROJECT_DIR / "results/edge_annotation_transition/citation_correlation"

INPUT_DIR = (
    INTERIM_DIR
    / "link_prediction/summary/first_layer/"
    "edge_annotation_pmid_profile_citation/correlation_summary_by_year"
)
INPUT_FILE_PREFIX = (
    "semmedVER43_R_first_layer_edge_annotation_pmid_profile_citation_correlation"
)
OUTPUT_FILE = (
    RESULTS_DIR
    / "first_layer_edge_annotation_pmid_profile_citation_correlation_by_year.csv"
)

BASE_YEAR = 1980
N_YEARS = 40
OVERWRITE = False


def input_file_for_year(year: int) -> Path:
    return INPUT_DIR / f"{INPUT_FILE_PREFIX}_{year}.csv"


def check_output(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not OVERWRITE:
        raise FileExistsError(
            "Output file already exists. Set OVERWRITE = True to replace it:\n"
            f"{path}"
        )
    if path.exists() and OVERWRITE:
        path.unlink()


def main() -> None:
    check_output(OUTPUT_FILE)
    frames = []
    missing = []
    for year in range(BASE_YEAR, BASE_YEAR + N_YEARS):
        input_file = input_file_for_year(year)
        if not input_file.exists():
            missing.append(str(input_file))
            continue
        frames.append(pd.read_csv(input_file))

    if missing:
        preview = "\n".join(missing[:20])
        if len(missing) > 20:
            preview += f"\n... and {len(missing) - 20} more"
        raise FileNotFoundError(f"Missing yearly correlation file(s):\n{preview}")

    combined = pd.concat(frames, ignore_index=True)
    combined.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved combined correlation summary to {OUTPUT_FILE}")
    print(f"Rows: {len(combined):,}")


if __name__ == "__main__":
    main()
