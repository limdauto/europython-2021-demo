# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
import pandas as pd

from europython_2021_demo.constants import (
    DEFAULT_ITEM_COL,
    DEFAULT_TIMESTAMP_COL,
    DEFAULT_USER_COL,
)

from .evaluation import map_at_k, ndcg_at_k, precision_at_k, recall_at_k
from .model import SARSingleNode
from .split import (
    min_rating_filter_pandas,
    process_split_ratio,
    split_pandas_data_with_ratios,
)


def _do_stratification(
    data,
    ratio=0.75,
    min_rating=1,
    filter_by="user",
    is_random=True,
    seed=42,
    col_user=DEFAULT_USER_COL,
    col_item=DEFAULT_ITEM_COL,
    col_timestamp=DEFAULT_TIMESTAMP_COL,
):
    # A few preliminary checks.
    if not (filter_by == "user" or filter_by == "item"):
        raise ValueError("filter_by should be either 'user' or 'item'.")

    if min_rating < 1:
        raise ValueError("min_rating should be integer and larger than or equal to 1.")

    if col_user not in data.columns:
        raise ValueError("Schema of data not valid. Missing User Col")

    if col_item not in data.columns:
        raise ValueError("Schema of data not valid. Missing Item Col")

    if not is_random:
        if col_timestamp not in data.columns:
            raise ValueError("Schema of data not valid. Missing Timestamp Col")

    multi_split, ratio = process_split_ratio(ratio)

    split_by_column = col_user if filter_by == "user" else col_item

    ratio = ratio if multi_split else [ratio, 1 - ratio]

    if min_rating > 1:
        data = min_rating_filter_pandas(
            data,
            min_rating=min_rating,
            filter_by=filter_by,
            col_user=col_user,
            col_item=col_item,
        )

    # Split by each group and aggregate splits together.
    splits = []

    # If it is for chronological splitting, the split will be performed in a random way.
    df_grouped = (
        data.sort_values(col_timestamp).groupby(split_by_column)
        if is_random is False
        else data.groupby(split_by_column)
    )

    for _, group in df_grouped:
        group_splits = split_pandas_data_with_ratios(
            group, ratio, shuffle=is_random, seed=seed
        )

        # Concatenate the list of split dataframes.
        concat_group_splits = pd.concat(group_splits)

        splits.append(concat_group_splits)

    # Concatenate splits for all the groups together.
    splits_all = pd.concat(splits)

    # Take split by split_index
    splits_list = [
        splits_all[splits_all["split_index"] == x].drop("split_index", axis=1)
        for x in range(len(ratio))
    ]

    return splits_list


def python_stratified_split(
    data,
    ratio=0.75,
    min_rating=1,
    filter_by="user",
    seed=42,
    col_user=DEFAULT_USER_COL,
    col_item=DEFAULT_ITEM_COL,
):
    """Pandas stratified splitter.

    For each user / item, the split function takes proportions of ratings which is
    specified by the split ratio(s). The split is stratified.

    Args:
        data (pandas.DataFrame): Pandas DataFrame to be split.
        ratio (float or list): Ratio for splitting data. If it is a single float number
            it splits data into two halves and the ratio argument indicates the ratio of
            training data set; if it is a list of float numbers, the splitter splits
            data into several portions corresponding to the split ratios. If a list is
            provided and the ratios are not summed to 1, they will be normalized.
        seed (int): Seed.
        min_rating (int): minimum number of ratings for user or item.
        filter_by (str): either "user" or "item", depending on which of the two is to
            filter with min_rating.
        col_user (str): column name of user IDs.
        col_item (str): column name of item IDs.

    Returns:
        list: Splits of the input data as pandas.DataFrame.
    """
    return _do_stratification(
        data,
        ratio=ratio,
        min_rating=min_rating,
        filter_by=filter_by,
        col_user=col_user,
        col_item=col_item,
        is_random=True,
        seed=seed,
    )


def train_model(
    train_set: pd.DataFrame,
    col_user,
    col_item,
    col_rating,
    col_timestamp,
    col_prediction,
    similarity_type,
    time_decay_coefficient,
    time_now,
    timedecay_formula,
):
    model = SARSingleNode(
        col_user=col_user,
        col_item=col_item,
        col_rating=col_rating,
        col_timestamp=col_timestamp,
        col_prediction=col_prediction,
        similarity_type=similarity_type,
        time_decay_coefficient=int(time_decay_coefficient),
        time_now=time_now,
        timedecay_formula=timedecay_formula,
    )
    model.fit(train_set)
    return model


def evaluate_model(
    model: SARSingleNode,
    test_set: pd.DataFrame,
    col_user: str,
    col_item: str,
    col_prediction: str,
    relevancy_method: str,
    k: int,
    threshold: int,
):
    top_k_recommended = model.recommend_k_items(test_set, remove_seen=True)
    args = [test_set, top_k_recommended]
    kwargs = dict(
        col_user=col_user,
        col_item=col_item,
        col_prediction=col_prediction,
        relevancy_method=relevancy_method,
        k=k,
        threshold=threshold,
    )
    model_metrics = pd.DataFrame(
        {
            "map_at_k": map_at_k(*args, **kwargs),
            "ndcg_at_k": ndcg_at_k(*args, **kwargs),
            "precision_at_k": precision_at_k(*args, **kwargs),
            "recall_at_k": recall_at_k(*args, **kwargs),
        },
        index=[0],
    )
    return [top_k_recommended, model_metrics]
