import json

from dagster import (
    AssetExecutionContext,
    AssetsDefinition,
    Definitions,
    asset,
    define_asset_job,
)


class RequestedResultConfig:
    endpoint: str
    params: dict


class RawDataConfig:
    source_gcs_path: str
    target_gcs_path: str


class BqConfig:
    gcs_path: str
    project_name: str
    dataset_name: str
    table_name: str


# _raw_data_asset_factoryのみ子クラスでオーバーライドする
# そのためにabstractmethodにする
class EndpointJobFactory:
    def __init__(self, endpoint_name: str, config_path: str):
        # リクエストのためのJSONファイルを読み込む
        self._endpoint_name = endpoint_name
        self._config = json.load(open(config_path))

        self._api_response_asset = self._api_response_asset_factory()
        self._raw_data_asset = self._raw_data_asset_factory()
        self._bq_asset = self._bq_asset_factory()

    @property
    def assets(self):
        return [self._api_response_asset, self._raw_data_asset, self._bq_asset]

    def create_job(self):
        asset_job = define_asset_job(
            name=self._endpoint_name,
            selection=self.assets,
            config=self._config["op"],
            description="base description",
        )

    def _api_response_asset_factory(self) -> AssetsDefinition:
        @asset
        def _api_response_asset(
            context: AssetExecutionContext, config: RequestedResultConfig
        ) -> None:
            # Cloud Functionsを叩く
            # リクエストの結果のJSONを表す
            return

        return _api_response_asset

    def _raw_data_asset_factory(self) -> AssetsDefinition:
        @asset(deps=[self._api_response_asset])
        def _raw_data_asset(
            context: AssetExecutionContext, config: RawDataConfig
        ) -> None:
            # リクエストの結果のJSONから加工したJSONを表す
            return

        return _raw_data_asset

    def _bq_asset_factory(self) -> AssetsDefinition:
        @asset(deps=[self._raw_data_asset])
        def _bq_asset(context: AssetExecutionContext, config: BqConfig) -> None:
            # BigQuery Tableを表す
            return

        return _bq_asset
