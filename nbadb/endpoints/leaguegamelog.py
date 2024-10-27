import json
import time
from datetime import datetime

import requests
from dagster import AssetExecutionContext, AssetsDefinition, Nothing, asset, define_asset_job
from dagster_gcp import BigQueryResource
from endpoints.utils import (
    PLAYER_OR_TEAM_ABBREVIATIONS,
    SEASON_TYPES,
    STATS_BEGINNING_YEAR,
    build_season_name,
    get_jsonl,
    is_exists,
    load_result,
    save_as_jsonl,
    save_result,
)
from helpers import bq


class LeagueGameLogJobFactory:
    """dailyで叩くjobの生成"""

    def __init__(self):
        self._endpoint_name = "leaguegamelog"
        self._save_name = f"{self._endpoint_name}_initial_result"
        self._table_name = "leaguegamelog"

        # assets
        self._api_response_asset = self._api_response_asset_factory()
        self._raw_data_asset = self._raw_data_asset_factory()
        self._bq_table_asset = self._bq_table_asset_factory()

    @property
    def assets(self):
        pass

    def create_job(self):
        pass

    def _api_response_asset_factory(self) -> AssetsDefinition:
        @asset
        def _api_response_asset(context: AssetExecutionContext) -> Nothing:
            pass

        return _api_response_asset

    def _raw_data_asset_factory(self) -> AssetsDefinition:
        @asset(deps=[self._api_response_asset])
        def _raw_data_asset(context: AssetExecutionContext) -> Nothing:
            pass

        return _raw_data_asset

    def _bq_table_asset_factory(self) -> AssetsDefinition:
        @asset(deps=[self._raw_data_asset])
        def _bq_table_asset(context: AssetExecutionContext, bigquery: BigQueryResource) -> Nothing:
            pass

        return _bq_table_asset


class InitLeagueGameLogFactory:
    """昨日分までのデータを取得するjobの生成(1回のみの実行)"""

    def __init__(self):
        self._endpoint_name = "leaguegamelog"
        self._save_name = f"{self._endpoint_name}"

        self._result_save_name = None

        # assets
        self._api_response_asset = self._api_response_asset_factory()
        self._raw_data_asset = self._raw_data_asset_factory()
        self._bq_table_asset = self._bq_table_asset_factory()

    @property
    def assets(self):
        return [self._api_response_asset, self._raw_data_asset, self._bq_table_asset]

    def create_job(self):
        return define_asset_job(
            name=f"{self._endpoint_name}_init",
            selection=self.assets,
            config={},
            description="leaguegamelog を昨日分まですべて取得",
        )

    def _api_response_asset_factory(self) -> AssetsDefinition:
        @asset(
            name=f"{self._endpoint_name}_api_response_init",
        )
        def _api_response_asset(context: AssetExecutionContext) -> Nothing:

            # 今シーズンを取得
            # FIXME: nba_apiからシーズンを取得したい
            this_year = datetime.today().year

            # 各シーズンに対してリクエストを送る
            url = "http://localhost:8080"
            # STATS_BEGINNING_YEAR
            for year in range(STATS_BEGINNING_YEAR, this_year + 1):
                season = build_season_name(year)
                for t_or_p in PLAYER_OR_TEAM_ABBREVIATIONS:
                    for season_type_key, season_type_value in SEASON_TYPES.items():
                        save_name = f"{self._save_name}/season={season}/player_or_team_abbreviation={t_or_p}/season_type={season_type_key}/data"
                        if is_exists(save_name, "result"):
                            continue

                        context.log.info(f"save_name: {save_name}")
                        response = requests.get(
                            url=url,
                            json={
                                "endpoint": self._endpoint_name,
                                "params": {
                                    "season": season,
                                    "player_or_team_abbreviation": t_or_p,
                                    "season_type_all_star": season_type_value,
                                },
                            },
                        )
                        save_result(
                            result=response.json(),
                            save_name=save_name,
                        )
                        time.sleep(20)
            return

        return _api_response_asset

    def _raw_data_asset_factory(self) -> AssetsDefinition:
        @asset(name=f"{self._endpoint_name}_raw_data_init", deps=[self._api_response_asset])
        def _raw_data_asset(context: AssetExecutionContext) -> Nothing:

            # 再帰的に取得
            this_year = datetime.today().year
            for year in range(STATS_BEGINNING_YEAR, this_year + 1):
                season = build_season_name(year)
                for t_or_p in PLAYER_OR_TEAM_ABBREVIATIONS:

                    jsonl_name = f"{self._save_name}/season={season}/player_or_team_abbreviation={t_or_p}/data"
                    if is_exists(jsonl_name, "raw"):
                        continue

                    list_of_dict = []
                    for season_type_key, season_type_value in SEASON_TYPES.items():
                        json_name = f"{self._save_name}/season={season}/player_or_team_abbreviation={t_or_p}/season_type={season_type_key}/data"
                        context.log.info(f"save_name: {json_name}")
                        d = load_result(json_name)
                        # すべての要素（辞書）の末尾にseason_typeを追加する
                        # FIXME: pandas, spark に変更
                        for _d in d["LeagueGameLog"]:
                            _d["_SEASON_TYPE"] = season_type_key
                        list_of_dict.extend(d["LeagueGameLog"])

                    save_as_jsonl(
                        list_of_dict=list_of_dict,
                        save_name=jsonl_name,
                    )

            return

        return _raw_data_asset

    def _bq_table_asset_factory(self) -> AssetsDefinition:
        @asset(name=f"{self._endpoint_name}_bq_table_init", deps=[self._raw_data_asset])
        def _bq_table_asset(context: AssetExecutionContext, bigquery: BigQueryResource) -> Nothing:

            # スキーマを取得
            schemas = dict()
            with open("../infra/terraform/bigquery/leaguegamelog_team.json", mode="r") as f:
                schemas["T"] = json.load(f)
            with open("../infra/terraform/bigquery/leaguegamelog_player.json", mode="r") as f:
                schemas["P"] = json.load(f)

            # 再帰的に取得
            this_year = datetime.today().year
            for year in range(STATS_BEGINNING_YEAR, this_year + 1):
                season = build_season_name(year)
                for t_or_p in PLAYER_OR_TEAM_ABBREVIATIONS:
                    save_name = f"{self._save_name}/season={season}/player_or_team_abbreviation={t_or_p}/data"
                    list_of_dict = get_jsonl(save_name)

                    # BQに保存
                    context.log.info(f"save_name: {save_name}")
                    with bigquery.get_client() as client:
                        client.load_table_from_json(
                            json_rows=list_of_dict,
                            destination=(
                                f"{bigquery.project}.nba.{self._endpoint_name}"
                                + "_"
                                + ("team" if t_or_p == "T" else "player")
                            ),
                            job_config=bq.build_load_job_config(schemas[t_or_p], write_disposition="append"),
                        )

        return _bq_table_asset
