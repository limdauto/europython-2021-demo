from kedro.pipeline import Pipeline, node
from .nodes import clean_movies, clean_ratings, create_model_input_table


def create_pipeline() -> Pipeline:
    return Pipeline(
        [
            node(
                clean_ratings,
                inputs="raw_ratings",
                outputs="cleaned_ratings",
                tags="data_cleaning",
            ),
            node(
                clean_movies,
                inputs="raw_movies",
                outputs="cleaned_movies",
                tags="data_cleaning",
            ),
            node(
                create_model_input_table,
                inputs=["cleaned_ratings", "cleaned_movies"],
                outputs="rated_movies",
            ),
        ]
    )
