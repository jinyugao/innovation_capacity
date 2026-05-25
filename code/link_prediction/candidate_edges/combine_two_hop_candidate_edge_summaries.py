"""Combine yearly two-hop candidate edge summary files into one table."""

from __future__ import annotations

from pathlib import Path

import pandas as pd


SUMMARY_DIR = Path(
    "/xdisk/sebratt/jinyugao/projects/innovation_capacity/data/interim/"
    "link_prediction/candidate_edges/summary"
)
INPUT_FILE_PATTERN = "two_hop_candidate_edge_summary_{year}.csv"
OUTPUT_FILE = SUMMARY_DIR / "two_hop_candidate_edge_summary_all_years.csv"
BASE_YEAR = 1980
END_YEAR = 2019


def summary_file_for_year(year: int) -> Path:
    return SUMMARY_DIR / INPUT_FILE_PATTERN.format(year=year)


def main() -> None:
    years = list(range(BASE_YEAR, END_YEAR + 1))
    missing_files = [year for year in years if not summary_file_for_year(year).exists()]

    if missing_files:
        raise FileNotFoundError(
            "Missing yearly candidate-edge summary files for years: "
            f"{missing_files}"
        )

    summaries = []
    for year in years:
        input_file = summary_file_for_year(year)
        df = pd.read_csv(input_file)
        if df.empty:
            raise ValueError(f"Summary file is empty: {input_file}")
        summaries.append(df)

    combined = pd.concat(summaries, ignore_index=True).sort_values("pyear")
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(OUTPUT_FILE, index=False)

    print(
        "Combined yearly two-hop candidate-edge summaries: "
        f"{len(combined):,} rows saved to {OUTPUT_FILE}"
    )


if __name__ == "__main__":
    main()
