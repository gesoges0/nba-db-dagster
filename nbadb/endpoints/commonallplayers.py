"""
endpoint: commonallplayer
daily
  params:
      season: 今季のシーズン
init
  params:
      season: 1990年から一年ごと
"""

import requests
from dagster import (
    AssetExecutionContext,
    AssetsDefinition,
    Nothing,
    asset,
    define_asset_job,
)

from endpoints.base import EndpointJobFactory
from endpoints.utils import save_result, load_result, save_as_jsonl


class DailyJobFactory:
    pass


class InitCommonAllPlayersJobFactory:
    def __init__(self):
        self._endpoint_name = "commonallplayers"
        self._save_name = f"{self._endpoint_name}_initial_result"

        # assets
        self._api_response_asset = self._api_response_asset_factory()
        self._raw_data_asset = self._raw_data_asset_factory()
        self._bq_table_asset = self._bq_table_asset_factory()

    @property
    def assets(self) -> list[AssetsDefinition]:
        return [self._api_response_asset, self._raw_data_asset, self._bq_table_asset]

    def create_job(self):
        return define_asset_job(
            name=self._endpoint_name,
            selection=self.assets,
            config={},
            description="all common players を過去分すべて取得",
        )

    def _api_response_asset_factory(self) -> AssetsDefinition:
        @asset
        def _api_response_asset(context: AssetExecutionContext) -> Nothing:
            # Cloud Functionsを叩く
            url = url = "http://localhost:8080"
            response = requests.get(
                url=url,
                json={"endpoint": self._endpoint_name, "params": {"season": "202021"}},
            )
            # 結果
            print(response.json())
            # レスポンスを保存
            save_result(
                result=response.json(),
                save_name=self._save_name,
            )
            return

        return _api_response_asset

    def _raw_data_asset_factory(self) -> AssetsDefinition:
        @asset(deps=[self._api_response_asset])
        def _raw_data_asset(context: AssetExecutionContext) -> Nothing:
            """APIの結果をJSONL形式に変換"""
            # レスポンスを取得
            d = load_result(self._save_name)
            # 辞書をJSONL形式に変換
            save_as_jsonl(list_of_dict=d["CommonAllPlayers"], save_name=self._save_name)
            return

        return _raw_data_asset

    def _bq_table_asset_factory(self) -> AssetsDefinition:
        @asset(deps=[self._raw_data_asset])
        def _bq_table_asset(context: AssetExecutionContext) -> Nothing:
            # bq tableにデータを保存
            return

        return _bq_table_asset
