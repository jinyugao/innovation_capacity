"""Select one text label for each CUI for BiomedBERT embeddings."""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd


INPUT_FILE = Path(
    "/xdisk/sebratt/jinyugao/projects/innovation_capacity/data/interim/"
    "biomedbert_link_prediction/cui_labels/biomedbert_cui_name_frequencies.csv.gz"
)
OUTPUT_DIR = Path(
    "/xdisk/sebratt/jinyugao/projects/innovation_capacity/data/interim/"
    "biomedbert_link_prediction/cui_labels"
)
OUTPUT_FILE = OUTPUT_DIR / "biomedbert_cui_labels.csv.gz"

OVERWRITE = False


def check_input(input_file: Path) -> None:
    if not input_file.exists():
        raise FileNotFoundError(f"Missing required input file: {input_file}")


def check_output(output_file: Path) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    if output_file.exists() and not OVERWRITE:
        raise FileExistsError(
            "Output file already exists. Set OVERWRITE = True to replace it:\n"
            f"{output_file}"
        )
    if output_file.exists() and OVERWRITE:
        output_file.unlink()


def is_numeric_cui(cui: str) -> bool:
    return bool(re.fullmatch(r"\d+", cui))


def is_truncated_name(name: str) -> bool:
    return name.endswith("-")


def is_short_name(name: str) -> bool:
    return len(name.replace(" ", "")) < 4


def label_quality_rank(name: str) -> int:
    if is_truncated_name(name):
        return 2
    if is_short_name(name):
        return 1
    return 0


def label_quality_flag(cui: str, name: str, selected_rank: int) -> str:
    if is_numeric_cui(cui):
        return "numeric_cui_gene_symbol"
    if is_truncated_name(name):
        return "truncated_name"
    if is_short_name(name):
        return "short_name"
    if selected_rank == 1:
        return "selected_most_frequent"
    return "selected_non_truncated_or_longer_name"


def select_labels(input_file: Path, output_file: Path) -> None:
    df = pd.read_csv(
        input_file,
        compression="gzip",
        dtype={"cui": "string", "cui_name": "string"},
    )
    df = df.dropna(subset=["cui", "cui_name", "n_occurrences"]).copy()
    df["cui"] = df["cui"].astype("string").str.strip()
    df["cui_name"] = df["cui_name"].astype("string").str.strip()
    df["n_occurrences"] = pd.to_numeric(df["n_occurrences"], errors="coerce")
    df = df.dropna(subset=["n_occurrences"])
    df = df[(df["cui"] != "") & (df["cui_name"] != "")]

    df = df.sort_values(
        by=["cui", "n_occurrences", "cui_name"],
        ascending=[True, False, True],
        kind="mergesort",
    )
    df["frequency_rank"] = df.groupby("cui").cumcount() + 1
    df["quality_rank"] = df["cui_name"].map(label_quality_rank)
    df = df.sort_values(
        by=["cui", "quality_rank", "n_occurrences", "cui_name"],
        ascending=[True, True, False, True],
        kind="mergesort",
    )
    df["selection_rank_after_quality_filter"] = df.groupby("cui").cumcount() + 1

    selected = df.groupby("cui", sort=False).head(1).copy()
    selected["name_quality_flag"] = [
        label_quality_flag(cui, name, rank)
        for cui, name, rank in zip(
            selected["cui"],
            selected["cui_name"],
            selected["frequency_rank"],
        )
    ]

    selected = selected.rename(
        columns={
            "cui_name": "selected_cui_name",
            "n_occurrences": "selected_name_occurrences",
        }
    )
    selected = selected[
        [
            "cui",
            "selected_cui_name",
            "selected_name_occurrences",
            "frequency_rank",
            "selection_rank_after_quality_filter",
            "name_quality_flag",
        ]
    ]
    selected.to_csv(output_file, index=False, compression="gzip")

    print(f"Saved selected CUI labels to {output_file}")
    print(f"Selected labels: {len(selected):,}")
    print("Name quality flags:")
    print(selected["name_quality_flag"].value_counts(dropna=False).to_string())


def main() -> None:
    check_input(INPUT_FILE)
    check_output(OUTPUT_FILE)
    select_labels(INPUT_FILE, OUTPUT_FILE)


if __name__ == "__main__":
    main()
