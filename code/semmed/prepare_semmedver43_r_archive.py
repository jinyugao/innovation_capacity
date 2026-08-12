"""This script reads the SemMedDB PREDICATION and CITATIONS source files, appends
publication year (PYEAR) to each predication by PMID, and writes two
project-level interim outputs:

1. A full predication file with PYEAR appended.
2. A filtered predication file that keeps records where both subject and object
   novelty values are non-missing and non-zero, and adds primary subject/object
   CUI fields extracted from pipe-delimited CUI columns.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


INPUT_DIR = Path("/xdisk/sebratt/jinyugao/data/source/semmedVER43_R")
OUTPUT_DIR = Path(
    "/xdisk/sebratt/jinyugao/projects/innovation_capacity/data/interim/semmedVER43_R"
)

PREDICATION_FILE = INPUT_DIR / "semmedVER43_2024_R_PREDICATION.csv.gz"
CITATIONS_FILE = INPUT_DIR / "semmedVER43_2024_R_CITATIONS.csv.gz"

FULL_OUTPUT_FILE = OUTPUT_DIR / "semmedVER43_2024_R_predications_with_pyear.csv.gz"
FILTERED_OUTPUT_FILE = (
    OUTPUT_DIR / "semmedVER43_2024_R_predications_with_pyear_filtered.csv.gz"
)

CHUNK_SIZE = 100_000
OVERWRITE = False

CITATION_COLUMNS = ["PMID", "ISSN", "DP", "EDAT", "PYEAR"]
CITATION_KEEP_COLUMNS = ["PMID", "PYEAR"]

PREDICATION_COLUMNS = [
    "PREDICATION_ID",
    "SENTENCE_ID",
    "PMID",
    "PREDICATE",
    "SUBJECT_CUI",
    "SUBJECT_NAME",
    "SUBJECT_SEMTYPE",
    "SUBJECT_NOVELTY",
    "OBJECT_CUI",
    "OBJECT_NAME",
    "OBJECT_SEMTYPE",
    "OBJECT_NOVELTY",
    "M",
    "N",
    "O",
]
PREDICATION_KEEP_COLUMNS = PREDICATION_COLUMNS[:-3]

ID_DTYPE = {
    "PREDICATION_ID": "string",
    "SENTENCE_ID": "string",
    "PMID": "string",
}


def check_inputs(predication_file: Path, citations_file: Path) -> None:
    missing_files = [
        str(path) for path in [predication_file, citations_file] if not path.exists()
    ]
    if missing_files:
        missing = "\n".join(missing_files)
        raise FileNotFoundError(f"Missing required input file(s):\n{missing}")


def check_outputs(output_files: list[Path], overwrite: bool) -> None:
    existing_files = [str(path) for path in output_files if path.exists()]
    if existing_files and not overwrite:
        existing = "\n".join(existing_files)
        raise FileExistsError(
            "Output file(s) already exist. Set OVERWRITE = True to replace them:\n"
            f"{existing}"
        )
    for path in output_files:
        if path.exists() and overwrite:
            path.unlink()


def load_pyear_map(citations_file: Path) -> pd.Series:
    citations = pd.read_csv(
        citations_file,
        compression="gzip",
        header=None,
        names=CITATION_COLUMNS,
        usecols=CITATION_KEEP_COLUMNS,
        dtype={"PMID": "string", "PYEAR": "string"},
        encoding="latin1",
    )

    duplicate_rows = citations.duplicated(subset=["PMID"], keep=False)
    if duplicate_rows.any():
        duplicate_pmids = citations.loc[duplicate_rows, "PMID"].nunique()
        conflicting_pmids = (
            citations.loc[duplicate_rows]
            .dropna(subset=["PMID"])
            .groupby("PMID")["PYEAR"]
            .nunique(dropna=False)
        )
        conflicting_pmids = conflicting_pmids[conflicting_pmids > 1]

        if not conflicting_pmids.empty:
            examples = ", ".join(conflicting_pmids.index.astype(str).head(10))
            raise ValueError(
                "CITATIONS contains duplicate PMID values with conflicting PYEAR "
                f"values. Example PMID(s): {examples}"
            )

        print(
            "CITATIONS contains duplicate PMID rows with consistent PYEAR values; "
            f"dropping duplicates for {duplicate_pmids:,} PMID(s)."
        )
        citations = citations.drop_duplicates(subset=["PMID"], keep="first")

    return citations.set_index("PMID")["PYEAR"]


def add_primary_cuis(chunk: pd.DataFrame) -> pd.DataFrame:
    filtered = chunk.copy()
    filtered["subject_cui_primary"] = (
        filtered["SUBJECT_CUI"].astype("string").str.split("|").str[0]
    )
    filtered["object_cui_primary"] = (
        filtered["OBJECT_CUI"].astype("string").str.split("|").str[0]
    )
    return filtered


def filter_by_novelty(chunk: pd.DataFrame) -> pd.DataFrame:
    filtered = add_primary_cuis(chunk)
    filtered["SUBJECT_NOVELTY"] = pd.to_numeric(
        filtered["SUBJECT_NOVELTY"], errors="coerce"
    )
    filtered["OBJECT_NOVELTY"] = pd.to_numeric(
        filtered["OBJECT_NOVELTY"], errors="coerce"
    )
    return filtered[
        filtered["SUBJECT_NOVELTY"].notna()
        & filtered["OBJECT_NOVELTY"].notna()
        & (filtered["SUBJECT_NOVELTY"] != 0)
        & (filtered["OBJECT_NOVELTY"] != 0)
    ].copy()


def prepare_semmed_predications(
    predication_file: Path,
    pyear_by_pmid: pd.Series,
    full_output_file: Path,
    filtered_output_file: Path,
    chunk_size: int,
) -> None:
    total_rows = 0
    total_missing_pyear = 0
    total_filtered_rows = 0

    reader = pd.read_csv(
        predication_file,
        compression="gzip",
        chunksize=chunk_size,
        header=None,
        names=PREDICATION_COLUMNS,
        usecols=PREDICATION_KEEP_COLUMNS,
        dtype=ID_DTYPE,
        encoding="latin1",
    )

    for chunk_number, chunk in enumerate(reader, start=1):
        chunk["PYEAR"] = chunk["PMID"].map(pyear_by_pmid)

        chunk_rows = len(chunk)
        missing_pyear = int(chunk["PYEAR"].isna().sum())
        total_rows += chunk_rows
        total_missing_pyear += missing_pyear

        chunk.to_csv(
            full_output_file,
            mode="a",
            index=False,
            compression="gzip",
            header=(chunk_number == 1),
        )

        filtered_chunk = filter_by_novelty(chunk)
        filtered_rows = len(filtered_chunk)
        total_filtered_rows += filtered_rows

        filtered_chunk.to_csv(
            filtered_output_file,
            mode="a",
            index=False,
            compression="gzip",
            header=(chunk_number == 1),
        )

        print(
            f"Chunk {chunk_number:,}: processed {chunk_rows:,} rows; "
            f"missing PYEAR {missing_pyear:,}; filtered rows {filtered_rows:,}."
        )

    print("Processing complete.")
    print(f"Total predication rows processed: {total_rows:,}")
    print(f"Rows missing PYEAR: {total_missing_pyear:,}")
    print(f"Rows written to filtered output: {total_filtered_rows:,}")


def main() -> None:
    input_dir = INPUT_DIR
    output_dir = OUTPUT_DIR
    predication_file = input_dir / PREDICATION_FILE.name
    citations_file = input_dir / CITATIONS_FILE.name
    full_output_file = output_dir / FULL_OUTPUT_FILE.name
    filtered_output_file = output_dir / FILTERED_OUTPUT_FILE.name

    check_inputs(predication_file, citations_file)
    output_dir.mkdir(parents=True, exist_ok=True)
    check_outputs([full_output_file, filtered_output_file], OVERWRITE)

    print(f"Reading citation years from {citations_file}")
    pyear_by_pmid = load_pyear_map(citations_file)
    print(f"Loaded PYEAR values for {len(pyear_by_pmid):,} unique PMID(s).")

    print(f"Processing predications from {predication_file}")
    prepare_semmed_predications(
        predication_file=predication_file,
        pyear_by_pmid=pyear_by_pmid,
        full_output_file=full_output_file,
        filtered_output_file=filtered_output_file,
        chunk_size=CHUNK_SIZE,
    )


if __name__ == "__main__":
    main()
