"""Build CUI-name frequency counts for BiomedBERT link prediction.

This script reads the filtered SemMedDB predication file and collects every
observed CUI-name pair from the subject and object columns. The output is used
to select one text label for each CUI before building BiomedBERT embeddings.
"""

from __future__ import annotations

from collections import Counter
from pathlib import Path

import pandas as pd


INPUT_FILE = Path(
    "/xdisk/sebratt/jinyugao/projects/innovation_capacity/data/interim/semmedVER43_R/"
    "semmedVER43_2024_R_predications_with_pyear_filtered.csv.gz"
)
OUTPUT_DIR = Path(
    "/xdisk/sebratt/jinyugao/projects/innovation_capacity/data/interim/"
    "biomedbert_link_prediction/cui_labels"
)
OUTPUT_FILE = OUTPUT_DIR / "biomedbert_cui_name_frequencies.csv.gz"

CHUNK_SIZE = 1_000_000
OVERWRITE = False

SUBJECT_NAME_COLUMN = "SUBJECT_NAME"
OBJECT_NAME_COLUMN = "OBJECT_NAME"

SUBJECT_CUI_COLUMN_CANDIDATES = ["subject_cui_primary", "subj_cui", "SUBJECT_CUI"]
OBJECT_CUI_COLUMN_CANDIDATES = ["object_cui_primary", "obj_cui", "OBJECT_CUI"]


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


def choose_column(columns: set[str], candidates: list[str], label: str) -> str:
    for candidate in candidates:
        if candidate in columns:
            return candidate
    raise ValueError(
        f"Could not find a {label} column. Tried: {', '.join(candidates)}"
    )


def resolve_input_columns(input_file: Path) -> tuple[str, str]:
    header = pd.read_csv(input_file, compression="gzip", nrows=0)
    columns = set(header.columns)
    subject_cui_column = choose_column(
        columns,
        SUBJECT_CUI_COLUMN_CANDIDATES,
        "subject CUI",
    )
    object_cui_column = choose_column(
        columns,
        OBJECT_CUI_COLUMN_CANDIDATES,
        "object CUI",
    )

    for name_column in [SUBJECT_NAME_COLUMN, OBJECT_NAME_COLUMN]:
        if name_column not in columns:
            raise ValueError(f"Missing required name column: {name_column}")

    print(f"Using subject CUI column: {subject_cui_column}")
    print(f"Using object CUI column: {object_cui_column}")
    return subject_cui_column, object_cui_column


def standardize_cui_name_pairs(
    chunk: pd.DataFrame,
    cui_column: str,
    name_column: str,
) -> pd.DataFrame:
    pairs = chunk[[cui_column, name_column]].rename(
        columns={cui_column: "cui", name_column: "cui_name"}
    )
    pairs = pairs.dropna(subset=["cui", "cui_name"]).copy()
    pairs["cui"] = pairs["cui"].astype("string").str.strip()
    pairs["cui_name"] = pairs["cui_name"].astype("string").str.strip()
    pairs = pairs[(pairs["cui"] != "") & (pairs["cui_name"] != "")]
    return pairs


def build_cui_name_frequencies(input_file: Path, output_file: Path) -> None:
    subject_cui_column, object_cui_column = resolve_input_columns(input_file)
    counts: Counter[tuple[str, str]] = Counter()
    total_rows = 0
    total_pairs = 0

    reader = pd.read_csv(
        input_file,
        compression="gzip",
        chunksize=CHUNK_SIZE,
        usecols=[
            subject_cui_column,
            SUBJECT_NAME_COLUMN,
            object_cui_column,
            OBJECT_NAME_COLUMN,
        ],
        dtype={
            subject_cui_column: "string",
            SUBJECT_NAME_COLUMN: "string",
            object_cui_column: "string",
            OBJECT_NAME_COLUMN: "string",
        },
    )

    for chunk_number, chunk in enumerate(reader, start=1):
        subject_pairs = standardize_cui_name_pairs(
            chunk,
            subject_cui_column,
            SUBJECT_NAME_COLUMN,
        )
        object_pairs = standardize_cui_name_pairs(
            chunk,
            object_cui_column,
            OBJECT_NAME_COLUMN,
        )
        pairs = pd.concat([subject_pairs, object_pairs], ignore_index=True)

        grouped = pairs.groupby(["cui", "cui_name"], sort=False).size()
        counts.update(grouped.to_dict())

        total_rows += len(chunk)
        total_pairs += len(pairs)
        print(
            f"Chunk {chunk_number:,}: read {total_rows:,} predication rows; "
            f"collected {total_pairs:,} CUI-name observations."
        )

    rows = [
        {"cui": cui, "cui_name": cui_name, "n_occurrences": n_occurrences}
        for (cui, cui_name), n_occurrences in counts.items()
    ]
    output = pd.DataFrame(rows)

    if output.empty:
        output = pd.DataFrame(columns=["cui", "cui_name", "n_occurrences"])
    else:
        output = output.sort_values(
            by=["cui", "n_occurrences", "cui_name"],
            ascending=[True, False, True],
            kind="mergesort",
        )

    output.to_csv(output_file, index=False, compression="gzip")
    print(f"Saved CUI-name frequencies to {output_file}")
    print(f"Unique CUI-name pairs: {len(output):,}")
    print(f"Unique CUIs: {output['cui'].nunique():,}")


def main() -> None:
    check_input(INPUT_FILE)
    check_output(OUTPUT_FILE)
    build_cui_name_frequencies(INPUT_FILE, OUTPUT_FILE)


if __name__ == "__main__":
    main()
