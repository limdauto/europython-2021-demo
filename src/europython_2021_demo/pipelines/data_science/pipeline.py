from kedro.pipeline import Pipeline, node
from .nodes import python_stratified_split, train_model, evaluate_model


def create_pipeline() -> Pipeline:
    return Pipeline(
        [
            node(
                python_stratified_split,
                inputs=[
                    "rated_movies",
                    "params:sar.split.ratio",
                    "params:sar.split.min_rating",
                    "params:sar.split.filter_by",
                    "params:sar.split.seed",
                    "params:col_user",
                    "params:col_item",
                ],
                outputs=["train_set", "test_set"],
                name="split_data",
                tags="data_splitting",
            ),
            node(
                train_model,
                inputs=[
                    "train_set",
                    "params:col_user",
                    "params:col_item",
                    "params:col_rating",
                    "params:col_timestamp",
                    "params:col_prediction",
                    "params:sar.hyper_parameters.similarity_type",
                    "params:sar.hyper_parameters.time_decay_coefficient",
                    "params:sar.hyper_parameters.time_now",
                    "params:sar.hyper_parameters.timedecay_formula",
                ],
                outputs="model",
                name="train_model",
                tags="model_training",
            ),
            node(
                evaluate_model,
                inputs=[
                    "model",
                    "test_set",
                    "params:col_user",
                    "params:col_item",
                    "params:col_prediction",
                    "params:sar.evaluation.relevancy_method",
                    "params:sar.evaluation.k",
                    "params:sar.evaluation.threshold_items_per_user",
                ],
                outputs=["recommended_movies", "model_metrics"],
                name="evaluate_model",
                tags="model_evaluation",
            ),
        ]
    )
