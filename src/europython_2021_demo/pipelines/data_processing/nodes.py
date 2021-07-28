""" Module contains data processing nodes for the pipeline.
"""
from datetime import datetime, timedelta

import numpy as np
import pandas as pd


def clean_ratings(data: pd.DataFrame) -> pd.DataFrame:
    """Clean ratings data

    Args:
        data: The original ratings DataFrame
    Return:
        The cleaned ratings DataFrame
    """
    data.loc[:, "Rating"] = data["Rating"].astype(np.float32)
    return data.dropna()


def clean_movies(data: pd.DataFrame) -> pd.DataFrame:
    """Clean movies data by retaining only important columns to the project

    Args:
        data: The original movies DataFrame
    Return:
        The cleaned moviesDataFrame
    """
    cleaned_movies = data[["MovieId", "MovieTitle"]]
    cleaned_movies.loc[len(cleaned_movies)] = [None, "This movie doesn't exist"]
    # return cleaned_movies.dropna()
    return cleaned_movies


def create_model_input_table(
    ratings: pd.DataFrame, movies: pd.DataFrame
) -> pd.DataFrame:
    """Create the input table for model training"""
    return movies.merge(ratings, left_on="MovieId", right_on="MovieId").dropna()
