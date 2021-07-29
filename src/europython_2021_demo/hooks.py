# Copyright 2021 QuantumBlack Visual Analytics Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
# NONINFRINGEMENT. IN NO EVENT WILL THE LICENSOR OR OTHER CONTRIBUTORS
# BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# The QuantumBlack Visual Analytics Limited ("QuantumBlack") name and logo
# (either separately or in combination, "QuantumBlack Trademarks") are
# trademarks of QuantumBlack. The License does not grant you any right or
# license to the QuantumBlack Trademarks. You may not use the QuantumBlack
# Trademarks or any confusingly similar mark as a trademark for your product,
# or use the QuantumBlack Trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# See the License for the specific language governing permissions and
# limitations under the License.

"""Project hooks."""
import logging
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from kedro.config import ConfigLoader
from kedro.framework.hooks import hook_impl
from kedro.io import DataCatalog
from kedro.versioning import Journal
from great_expectations.data_context import DataContext
from great_expectations.exceptions import DataContextError


logger = logging.getLogger(__name__)


class DataValidationHooks:
    def __init__(self):
        project_root = Path.cwd()
        ge_context_root_dir = project_root / "conf" / "base" / "great_expectations"
        self._ge_data_context: DataContext = DataContext(
            context_root_dir=str(ge_context_root_dir)
        )

    @hook_impl
    def before_dataset_saved(self, dataset_name: str, data: Any):
        try:
            self._ge_data_context.get_expectation_suite(dataset_name)
        except DataContextError:
            return

        result = self._ge_data_context.run_checkpoint(
            dataset_name,
            validations=[
                {
                    "batch_request": {
                        "datasource_name": "kedro",
                        "data_connector_name": "default_runtime_data_connector_name",
                        "data_asset_name": dataset_name,
                        "runtime_parameters": {"batch_data": data},
                        "batch_identifiers": {"default_identifier_name": dataset_name},
                    },
                    "expectation_suite_name": dataset_name,
                }
            ],
        )

        if not result["success"]:
            logger.error("Validation result: %s", result)
            raise ValueError(
                f"Data Validation failed for {dataset_name}. Please check data docs for more information."
            )


class ProjectHooks:
    @hook_impl
    def register_config_loader(
        self,
        conf_paths: Iterable[str],
        env: str,
        extra_params: Dict[str, Any],
    ) -> ConfigLoader:
        return ConfigLoader(conf_paths)

    @hook_impl
    def register_catalog(
        self,
        catalog: Optional[Dict[str, Dict[str, Any]]],
        credentials: Dict[str, Dict[str, Any]],
        load_versions: Dict[str, str],
        save_version: str,
        journal: Journal,
    ) -> DataCatalog:
        return DataCatalog.from_config(
            catalog, credentials, load_versions, save_version, journal
        )
