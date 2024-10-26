"""
endpoint: commonallplayer
daily
  params:
      season: 
つねにテーブルを更新する
"""

import json
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
from dagster_gcp import BigQueryResource

from endpoints.base import EndpointJobFactory
from endpoints.utils import get_jsonl, load_result, save_as_jsonl, save_result
from helpers import bq


class DailyJobFactory:
    pass


class CommonAllPlayersJobFactory:
    def __init__(self):
        self._endpoint_name = "commonallplayers"
        self._save_name = f"{self._endpoint_name}_initial_result"
        self._table_name = "commonallplayers"

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
            """nba_apiを叩いたレスポンス"""
            # Cloud Functionsを叩く
            url = url = "http://localhost:8080"
            response = requests.get(
                url=url,
                json={"endpoint": self._endpoint_name},
            )
            # レスポンスを保存
            save_result(
                result=response.json(),
                save_name=f"{self._save_name}",
            )
            return

        return _api_response_asset

    def _raw_data_asset_factory(self) -> AssetsDefinition:
        @asset(deps=[self._api_response_asset])
        def _raw_data_asset(context: AssetExecutionContext) -> Nothing:
            """BQテーブルに保存するための生データ"""
            # レスポンスを取得
            d = load_result(f"{self._save_name}")
            # 辞書をJSONL形式に変換
            save_as_jsonl(
                list_of_dict=d["CommonAllPlayers"],
                save_name=f"{self._save_name}",
            )
            return

        return _raw_data_asset

    def _bq_table_asset_factory(self) -> AssetsDefinition:
        @asset(deps=[self._raw_data_asset])
        def _bq_table_asset(
            context: AssetExecutionContext, bigquery: BigQueryResource
        ) -> Nothing:
            """BQテーブル"""
            list_of_dict: list[dict] = get_jsonl(self._save_name)

            # infra/terraform/bigquery/commonallplayers.json からスキーマを取得
            # FIXME: configで指定できるようにする
            with open(
                "../infra/terraform/bigquery/commonallplayers.json", mode="r"
            ) as f:
                schema = json.load(f)

            # BQ側にある選手の重複を排除して挿入する
            with bigquery.get_client() as client:
                job = client.load_table_from_json(
                    json_rows=list_of_dict,
                    destination=f"{bigquery.project}.nba.{self._table_name}",
                    job_config=bq.build_load_job_config(schema),
                )

        return _bq_table_asset
