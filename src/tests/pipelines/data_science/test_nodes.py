# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
import pandas as pd
import numpy as np
import pytest


from europython_2021_demo.constants import (
    DEFAULT_USER_COL,
    DEFAULT_ITEM_COL,
    DEFAULT_RATING_COL,
    DEFAULT_TIMESTAMP_COL,
)

from europython_2021_demo.pipelines.data_science.nodes import python_stratified_split


@pytest.fixture(scope="module")
def test_specs():
    return {
        "number_of_rows": 1000,
        "seed": 123,
        "ratio": 0.6,
        "ratios": [0.2, 0.3, 0.5],
        "split_numbers": [2, 3, 5],
        "tolerance": 0.01,
        "number_of_items": 50,
        "number_of_users": 20,
        "fluctuation": 0.02,
    }


@pytest.fixture(scope="module")
def python_dataset(test_specs):
    def random_date_generator(start_date, range_in_days):
        """Helper function to generate random timestamps.
        Reference: https://stackoverflow.com/questions/41006182/generate-random-dates-within-a-range-in-numpy
        """
        days_to_add = np.arange(0, range_in_days)
        random_dates = []
        for i in range(range_in_days):
            random_date = np.datetime64(start_date) + np.random.choice(days_to_add)
            random_dates.append(random_date)

        return random_dates

    np.random.seed(test_specs["seed"])

    rating = pd.DataFrame(
        {
            DEFAULT_USER_COL: np.random.randint(1, 5, test_specs["number_of_rows"]),
            DEFAULT_ITEM_COL: np.random.randint(1, 15, test_specs["number_of_rows"]),
            DEFAULT_RATING_COL: np.random.randint(1, 6, test_specs["number_of_rows"]),
            DEFAULT_TIMESTAMP_COL: random_date_generator(
                "2018-01-01", test_specs["number_of_rows"]
            ),
        }
    )
    return rating


def test_stratified_splitter(test_specs, python_dataset):
    splits = python_stratified_split(
        python_dataset, ratio=test_specs["ratio"], min_rating=10, filter_by="user"
    )

    assert len(splits[0]) / test_specs["number_of_rows"] == pytest.approx(
        test_specs["ratio"], test_specs["tolerance"]
    )
    assert len(splits[1]) / test_specs["number_of_rows"] == pytest.approx(
        1 - test_specs["ratio"], test_specs["tolerance"]
    )

    for split in splits:
        assert set(split.columns) == set(python_dataset.columns)

    # Test if both contains the same user list. This is because stratified split is stratified.
    users_train = splits[0][DEFAULT_USER_COL].unique()
    users_test = splits[1][DEFAULT_USER_COL].unique()

    assert set(users_train) == set(users_test)

    splits = python_stratified_split(
        python_dataset, ratio=test_specs["ratios"], min_rating=10, filter_by="user"
    )

    assert len(splits) == 3
    assert len(splits[0]) / test_specs["number_of_rows"] == pytest.approx(
        test_specs["ratios"][0], test_specs["tolerance"]
    )
    assert len(splits[1]) / test_specs["number_of_rows"] == pytest.approx(
        test_specs["ratios"][1], test_specs["tolerance"]
    )
    assert len(splits[2]) / test_specs["number_of_rows"] == pytest.approx(
        test_specs["ratios"][2], test_specs["tolerance"]
    )

    for split in splits:
        assert set(split.columns) == set(python_dataset.columns)
