import pandas as pd
from pathlib import Path

from context import lucille
from lucille.github.pr_analyzer import filter_prs


def test_mkPRSubsetFile():
    # read test data from a CSV file
    test_file = Path(__file__).with_name("test_pr_set.csv")
    df = pd.read_csv(test_file)
    columns = ["repo_name", "author", "created_at", "age_days", "pr_url"]
    min_age_days = 7
    max_age_days = 21

    result = filter_prs(df, columns, min_age_days, max_age_days)
    assert len(result) == 13
    assert all(result["age_days"] >= min_age_days)
    assert all(result["age_days"] <= max_age_days)
    assert set(result.columns) == set(columns)
