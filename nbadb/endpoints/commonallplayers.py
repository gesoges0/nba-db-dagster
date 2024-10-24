"""
endpoint: commonallplayer
daily
  params:
      season: 今季のシーズン
init
  params:
      season: 1990年から一年ごと
"""

from datetime import datetime

import requests
from dagster import (
    AssetExecutionContext,
    AssetsDefinition,
    Nothing,
    TimeWindowPartitionsDefinition,
    asset,
    define_asset_job,
)

from endpoints.base import EndpointJobFactory
from endpoints.utils import load_result, save_as_jsonl, save_result


class DailyJobFactory:
    pass


class InitCommonAllPlayersJobFactory:
    def __init__(self):
        self._endpoint_name = "commonallplayers"
        self._save_name = f"{self._endpoint_name}_initial_result"

        self._partition_def = TimeWindowPartitionsDefinition(
            start=datetime(1970, 1, 1), fmt="%Y", cron_schedule="0 0 1 1 *"
        )

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
        @asset(partitions_def=self._partition_def)
        def _api_response_asset(context: AssetExecutionContext) -> Nothing:
            """nba_apiを叩いたレスポンス"""
            # 現時点の年を取得
            this_year: int = context.partition_time_window.start.year + 1
            season_id: str = f"{this_year}{str(this_year + 1)[2:]}"

            # Cloud Functionsを叩く
            url = url = "http://localhost:8080"
            response = requests.get(
                url=url,
                json={"endpoint": self._endpoint_name, "params": {"season": season_id}},
            )
            # レスポンスを保存
            save_result(
                result=response.json(),
                save_name=f"{self._save_name}/{season_id}",
            )
            return

        return _api_response_asset

    def _raw_data_asset_factory(self) -> AssetsDefinition:
        @asset(deps=[self._api_response_asset], partitions_def=self._partition_def)
        def _raw_data_asset(context: AssetExecutionContext) -> Nothing:
            """BQテーブルに保存するための生データ"""
            # 現時点の年を取得
            this_year: int = context.partition_time_window.start.year + 1
            season_id: str = f"{this_year}{str(this_year + 1)[2:]}"
            # レスポンスを取得
            d = load_result(f"{self._save_name}/{season_id}")
            # 辞書をJSONL形式に変換
            save_as_jsonl(
                list_of_dict=d["CommonAllPlayers"],
                save_name=f"{self._save_name}/{season_id}",
            )
            return

        return _raw_data_asset

    def _bq_table_asset_factory(self) -> AssetsDefinition:
        @asset(deps=[self._raw_data_asset], partitions_def=self._partition_def)
        def _bq_table_asset(context: AssetExecutionContext) -> Nothing:
            """BQテーブル"""
            # bq tableにデータを保存
            return

        return _bq_table_asset
