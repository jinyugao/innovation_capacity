"""Build a Monte Carlo random baseline for link prediction.

This baseline asks whether common-neighbor top-10% predictions outperform random
selection from the same yearly two-hop candidate edge set. For each focal year,
we use the same candidate universe and the same predicted-set size as the main
common-neighbor top-10% specification.

At the edge level, uniform sampling without replacement from a candidate set of
size N, containing M actual new-combination edges, with sample size k has a
hypergeometric distribution. Drawing true-positive counts from that distribution
is equivalent to Monte Carlo edge sampling but avoids repeatedly reading huge
candidate-edge files.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd


PROJECT_DIR = Path("/xdisk/sebratt/jinyugao/projects/innovation_capacity")
RESULT_DIR = PROJECT_DIR / "results/link_prediction"
OUTPUT_DIR = RESULT_DIR / "random_baseline"

CANDIDATE_EVALUATION_FILE = (
    RESULT_DIR
    / "candidate_edges"
    / "two_hop_candidate_edge_evaluation_summary.csv"
)
CANDIDATE_EVALUATION_FALLBACK_FILE = (
    RESULT_DIR / "two_hop_candidate_edge_evaluation_summary.csv"
)
LINK_PREDICTION_EVALUATION_FILE = RESULT_DIR / "link_prediction_evaluation_summary.csv"

METHOD = "common_neighbor"
TOP_PERCENTILE = 10
N_REPLICATES = 100
RANDOM_SEED = 20260527
OVERWRITE = False

OUTPUT_PREFIX = f"monte_carlo_random_baseline_{METHOD}_top_{TOP_PERCENTILE}pct"
REPLICATE_OUTPUT_FILE = OUTPUT_DIR / f"{OUTPUT_PREFIX}_replicates.csv"
SUMMARY_BY_YEAR_OUTPUT_FILE = OUTPUT_DIR / f"{OUTPUT_PREFIX}_summary_by_year.csv"
OVERALL_SUMMARY_OUTPUT_FILE = OUTPUT_DIR / f"{OUTPUT_PREFIX}_overall_summary.csv"


def candidate_evaluation_file() -> Path:
    if CANDIDATE_EVALUATION_FILE.exists():
        return CANDIDATE_EVALUATION_FILE
    return CANDIDATE_EVALUATION_FALLBACK_FILE


def check_inputs(paths: list[Path]) -> None:
    missing_files = [str(path) for path in paths if not path.exists()]
    if missing_files:
        missing = "\n".join(missing_files)
        raise FileNotFoundError(f"Missing required input file(s):\n{missing}")


def check_outputs(paths: list[Path], overwrite: bool) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    existing_files = [str(path) for path in paths if path.exists()]
    if existing_files and not overwrite:
        existing = "\n".join(existing_files)
        raise FileExistsError(
            "Output file(s) already exist. Set OVERWRITE = True to replace them:\n"
            f"{existing}"
        )
    if overwrite:
        for path in paths:
            if path.exists():
                path.unlink()


def safe_divide(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return float("nan")
    return numerator / denominator


def f1_score(precision: float, recall: float) -> float:
    if pd.isna(precision) or pd.isna(recall) or precision + recall == 0:
        return float("nan")
    return 2 * precision * recall / (precision + recall)


def read_inputs(
    candidate_eval_file: Path,
    link_prediction_eval_file: Path,
) -> pd.DataFrame:
    candidate_eval = pd.read_csv(candidate_eval_file)
    link_eval = pd.read_csv(link_prediction_eval_file)

    link_eval = link_eval[
        (link_eval["method"] == METHOD)
        & (link_eval["top_percentile"] == TOP_PERCENTILE)
    ].copy()

    if link_eval.empty:
        raise ValueError(
            f"No link-prediction evaluation rows found for method={METHOD}, "
            f"top_percentile={TOP_PERCENTILE}."
        )

    candidate_columns = [
        "pyear",
        "n_two_hop_candidate_edges",
        "n_actual_new_combination_edges",
    ]
    link_columns = [
        "pyear",
        "n_predicted_edges",
        "n_expected_new_combination_edges",
        "precision",
        "recall",
        "f1_score",
    ]

    merged = candidate_eval[candidate_columns].merge(
        link_eval[link_columns],
        on="pyear",
        how="inner",
        validate="one_to_one",
    )
    merged = merged.sort_values("pyear").reset_index(drop=True)

    if merged.empty:
        raise ValueError("No overlapping years found between input summaries.")

    return merged


def validate_inputs(df: pd.DataFrame) -> None:
    for row in df.itertuples(index=False):
        n_candidate_edges = int(row.n_two_hop_candidate_edges)
        n_actual_new_edges = int(row.n_actual_new_combination_edges)
        n_predicted_edges = int(row.n_predicted_edges)
        n_observed_hits = int(row.n_expected_new_combination_edges)

        if n_candidate_edges < 0 or n_actual_new_edges < 0 or n_predicted_edges < 0:
            raise ValueError(f"Negative count found for pyear={row.pyear}.")
        if n_actual_new_edges > n_candidate_edges:
            raise ValueError(
                f"Actual new edge count exceeds candidate edge count for "
                f"pyear={row.pyear}."
            )
        if n_predicted_edges > n_candidate_edges:
            raise ValueError(
                f"Predicted edge count exceeds candidate edge count for "
                f"pyear={row.pyear}."
            )
        if n_observed_hits > n_actual_new_edges or n_observed_hits > n_predicted_edges:
            raise ValueError(
                f"Observed hit count is inconsistent with actual/predicted counts "
                f"for pyear={row.pyear}."
            )


def build_replicates(df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for row in df.itertuples(index=False):
        pyear = int(row.pyear)
        n_candidate_edges = int(row.n_two_hop_candidate_edges)
        n_actual_new_edges = int(row.n_actual_new_combination_edges)
        n_predicted_edges = int(row.n_predicted_edges)
        observed_hits = int(row.n_expected_new_combination_edges)
        observed_precision = float(row.precision)
        observed_recall = float(row.recall)
        observed_f1 = float(row.f1_score)

        rng = np.random.default_rng(RANDOM_SEED + pyear * 1000 + TOP_PERCENTILE)
        random_hits = rng.hypergeometric(
            ngood=n_actual_new_edges,
            nbad=n_candidate_edges - n_actual_new_edges,
            nsample=n_predicted_edges,
            size=N_REPLICATES,
        )

        for replicate, n_random_hits in enumerate(random_hits, start=1):
            n_random_hits = int(n_random_hits)
            random_precision = safe_divide(n_random_hits, n_predicted_edges)
            random_recall = safe_divide(n_random_hits, n_actual_new_edges)
            random_f1 = f1_score(random_precision, random_recall)

            rows.append(
                {
                    "pyear": pyear,
                    "method": METHOD,
                    "top_percentile": TOP_PERCENTILE,
                    "replicate": replicate,
                    "n_candidate_edges": n_candidate_edges,
                    "n_actual_new_combination_edges": n_actual_new_edges,
                    "n_random_predicted_edges": n_predicted_edges,
                    "n_random_expected_new_combination_edges": n_random_hits,
                    "random_precision": random_precision,
                    "random_recall": random_recall,
                    "random_f1_score": random_f1,
                    "observed_predicted_edges": n_predicted_edges,
                    "observed_expected_new_combination_edges": observed_hits,
                    "observed_precision": observed_precision,
                    "observed_recall": observed_recall,
                    "observed_f1_score": observed_f1,
                    "random_seed": RANDOM_SEED,
                }
            )

        print(
            f"pyear={pyear}: simulated {N_REPLICATES:,} random baseline "
            "replicates."
        )

    return pd.DataFrame(rows)


def summarize_by_year(replicates: pd.DataFrame) -> pd.DataFrame:
    random_metrics = [
        "n_random_expected_new_combination_edges",
        "random_precision",
        "random_recall",
        "random_f1_score",
    ]
    observed_metrics = [
        "observed_predicted_edges",
        "observed_expected_new_combination_edges",
        "observed_precision",
        "observed_recall",
        "observed_f1_score",
    ]
    fixed_metrics = [
        "n_candidate_edges",
        "n_actual_new_combination_edges",
        "n_random_predicted_edges",
    ]

    random_summary = replicates.groupby("pyear")[random_metrics].agg(
        ["mean", "std", "min", "median", "max"]
    )
    random_summary.columns = ["_".join(col) for col in random_summary.columns]
    random_summary = random_summary.reset_index()

    quantiles = (
        replicates.groupby("pyear")[random_metrics]
        .quantile([0.05, 0.95])
        .unstack(level=-1)
    )
    quantiles.columns = [f"{metric}_p{int(q * 100):02d}" for metric, q in quantiles.columns]
    quantiles = quantiles.reset_index()

    observed_summary = replicates.groupby("pyear")[observed_metrics + fixed_metrics].first()
    observed_summary = observed_summary.reset_index()

    summary = observed_summary.merge(random_summary, on="pyear", how="left").merge(
        quantiles, on="pyear", how="left"
    )
    summary["precision_lift_over_random_mean"] = (
        summary["observed_precision"] / summary["random_precision_mean"]
    )
    summary["recall_lift_over_random_mean"] = (
        summary["observed_recall"] / summary["random_recall_mean"]
    )
    summary["f1_lift_over_random_mean"] = (
        summary["observed_f1_score"] / summary["random_f1_score_mean"]
    )
    summary["expected_edges_lift_over_random_mean"] = (
        summary["observed_expected_new_combination_edges"]
        / summary["n_random_expected_new_combination_edges_mean"]
    )
    return summary.sort_values("pyear")


def summarize_overall(replicates: pd.DataFrame, summary_by_year: pd.DataFrame) -> pd.DataFrame:
    unweighted_random_precision = summary_by_year["random_precision_mean"].mean()
    unweighted_random_recall = summary_by_year["random_recall_mean"].mean()
    unweighted_random_f1 = summary_by_year["random_f1_score_mean"].mean()
    unweighted_observed_precision = summary_by_year["observed_precision"].mean()
    unweighted_observed_recall = summary_by_year["observed_recall"].mean()
    unweighted_observed_f1 = summary_by_year["observed_f1_score"].mean()

    total_random_hits = replicates["n_random_expected_new_combination_edges"].sum()
    total_random_predicted = replicates["n_random_predicted_edges"].sum()
    total_random_actual = replicates["n_actual_new_combination_edges"].sum()
    weighted_random_precision = safe_divide(total_random_hits, total_random_predicted)
    weighted_random_recall = safe_divide(total_random_hits, total_random_actual)
    weighted_random_f1 = f1_score(weighted_random_precision, weighted_random_recall)

    year_once = summary_by_year.drop_duplicates(subset=["pyear"])
    total_observed_hits = year_once["observed_expected_new_combination_edges"].sum()
    total_observed_predicted = year_once["observed_predicted_edges"].sum()
    total_observed_actual = year_once["n_actual_new_combination_edges"].sum()
    weighted_observed_precision = safe_divide(total_observed_hits, total_observed_predicted)
    weighted_observed_recall = safe_divide(total_observed_hits, total_observed_actual)
    weighted_observed_f1 = f1_score(weighted_observed_precision, weighted_observed_recall)

    rows = [
        {
            "summary_type": "unweighted_year_mean",
            "method": METHOD,
            "top_percentile": TOP_PERCENTILE,
            "n_years": summary_by_year["pyear"].nunique(),
            "n_replicates_per_year": N_REPLICATES,
            "observed_precision": unweighted_observed_precision,
            "random_precision": unweighted_random_precision,
            "precision_lift_over_random": safe_divide(
                unweighted_observed_precision, unweighted_random_precision
            ),
            "observed_recall": unweighted_observed_recall,
            "random_recall": unweighted_random_recall,
            "recall_lift_over_random": safe_divide(
                unweighted_observed_recall, unweighted_random_recall
            ),
            "observed_f1_score": unweighted_observed_f1,
            "random_f1_score": unweighted_random_f1,
            "f1_lift_over_random": safe_divide(
                unweighted_observed_f1, unweighted_random_f1
            ),
        },
        {
            "summary_type": "pooled_edge_weighted",
            "method": METHOD,
            "top_percentile": TOP_PERCENTILE,
            "n_years": summary_by_year["pyear"].nunique(),
            "n_replicates_per_year": N_REPLICATES,
            "observed_precision": weighted_observed_precision,
            "random_precision": weighted_random_precision,
            "precision_lift_over_random": safe_divide(
                weighted_observed_precision, weighted_random_precision
            ),
            "observed_recall": weighted_observed_recall,
            "random_recall": weighted_random_recall,
            "recall_lift_over_random": safe_divide(
                weighted_observed_recall, weighted_random_recall
            ),
            "observed_f1_score": weighted_observed_f1,
            "random_f1_score": weighted_random_f1,
            "f1_lift_over_random": safe_divide(weighted_observed_f1, weighted_random_f1),
        },
    ]
    return pd.DataFrame(rows)


def main() -> None:
    candidate_eval_file = candidate_evaluation_file()
    check_inputs([candidate_eval_file, LINK_PREDICTION_EVALUATION_FILE])
    check_outputs(
        [REPLICATE_OUTPUT_FILE, SUMMARY_BY_YEAR_OUTPUT_FILE, OVERALL_SUMMARY_OUTPUT_FILE],
        OVERWRITE,
    )

    input_summary = read_inputs(candidate_eval_file, LINK_PREDICTION_EVALUATION_FILE)
    validate_inputs(input_summary)
    replicates = build_replicates(input_summary)
    summary_by_year = summarize_by_year(replicates)
    overall_summary = summarize_overall(replicates, summary_by_year)

    replicates.to_csv(REPLICATE_OUTPUT_FILE, index=False)
    summary_by_year.to_csv(SUMMARY_BY_YEAR_OUTPUT_FILE, index=False)
    overall_summary.to_csv(OVERALL_SUMMARY_OUTPUT_FILE, index=False)

    print(f"Saved replicate-level random baseline to {REPLICATE_OUTPUT_FILE}")
    print(f"Saved yearly random baseline summary to {SUMMARY_BY_YEAR_OUTPUT_FILE}")
    print(f"Saved overall random baseline summary to {OVERALL_SUMMARY_OUTPUT_FILE}")
    print("Overall summary:")
    print(overall_summary.to_string(index=False))


if __name__ == "__main__":
    main()
