import json
import time
from datetime import datetime

import requests
from dagster import AssetExecutionContext, AssetsDefinition, asset, define_asset_job
from dagster_gcp import BigQueryResource

from endpoints.utils import (
    CF_EMU_URL,
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
from helpers.nba import get_nba_season


class LeagueGameLogJobFactory2:
    pass


class InitLeagueGameLogJobFactory2:

    def __init__(self):
        self._endpoint_name = "leaguegamelog"

        self._result_save_name = None

        # ----------------------------------------------------------------
        # initialize assets
        self._api_response_assets = []
        self._raw_data_assets = []
        self._bq_table_asset = None

        for year in range(STATS_BEGINNING_YEAR, datetime.now().year + 1):
            for p_or_t in PLAYER_OR_TEAM_ABBREVIATIONS.keys():
                season_assets = []
                for season_type in SEASON_TYPES.keys():
                    season = build_season_name(year)
                    api_response_asset = self._api_response_asset_factory(season, p_or_t, season_type)
                    season_assets.append(api_response_asset)
                    self._api_response_assets.append(api_response_asset)
                raw_data_asset = self._raw_data_asset_factory(season, p_or_t, season_assets)
                self._raw_data_assets.append(raw_data_asset)

        self._bq_table_asset = self._bq_table_asset_factory(self._raw_data_assets)
        # ----------------------------------------------------------------

    @property
    def assets(self) -> list[AssetsDefinition]:
        return self._api_response_assets + self._raw_data_assets + [self._bq_table_asset]

    def create_job(self):
        return define_asset_job(
            name=f"{self._endpoint_name}_init2",
            selection=self.assets,
            description="all league game logs を取得",
            config={},
        )

    def _api_response_asset_factory(self, season: str, p_or_t: str, season_type: str) -> AssetsDefinition:
        @asset(
            name=f"{self._endpoint_name}_{season}_{p_or_t}_{season_type}_api_response2",
            op_tags={"dagster/concurrency_key": "nba_api"},
        )
        def _api_response_asset(context: AssetExecutionContext) -> None:
            """nba_apiを叩いたレスポンス"""
            # Cloud Functionsを叩く
            params = {"season": season, "player_or_team_abbreviation": p_or_t, "season_type_all_star": season_type}
            response = requests.get(
                url=CF_EMU_URL,
                json={"endpoint": self._endpoint_name, "params": params},
            )
            # 保存名
            save_name = f"{self._endpoint_name}2/season={season}/player_or_team_abbreviation={p_or_t}/season_type={season_type}/data"
            # レスポンスを保存
            save_result(
                result=response.json(),
                save_name=save_name,
            )
            return

        return _api_response_asset

    def _raw_data_asset_factory(
        self, season: str, player_or_team_abbreviation: str, season_assets: list[AssetsDefinition]
    ) -> AssetsDefinition:
        @asset(
            name=f"{self._endpoint_name}_{season}_{player_or_team_abbreviation}_raw_data2",
            deps=season_assets,
        )
        def _raw_data_asset(context: AssetExecutionContext) -> None:
            """BQテーブルに保存するための生データ"""
            list_of_dict = []
            for season_type in SEASON_TYPES.keys():
                d = load_result(
                    f"{self._endpoint_name}2/season={season}/player_or_team_abbreviation={player_or_team_abbreviation}/season_type_all_star={season_type}/data"
                )
                list_of_dict.extend(d["LeagueGameLog"])

            # 辞書をJSONL形式に変換
            save_name = (
                f"{self._endpoint_name}2/season={season}/player_or_team_abbreviation={player_or_team_abbreviation}/data"
            )
            save_as_jsonl(
                list_of_dict=list_of_dict,
                save_name=save_name,
            )
            return

        return _raw_data_asset

    def _bq_table_asset_factory(self, raw_data_assets: list[AssetsDefinition]) -> AssetsDefinition:
        @asset(
            name=f"{self._endpoint_name}_bq_table2",
            deps=raw_data_assets,
        )
        def _bq_table_asset(context: AssetExecutionContext, bigquery: BigQueryResource) -> None:
            """BQテーブルに保存"""
            for p_or_t, suffix in PLAYER_OR_TEAM_ABBREVIATIONS.items():
                table_name = f"{self._endpoint_name}2_{suffix}"
                with open(f"../schemas/{self._endpoint_name}.json", "r") as f:
                    schema = json.load(f)

                    for year in range(STATS_BEGINNING_YEAR, datetime.now().year + 1):
                        jsonl = f"{self._endpoint_name}2/season={year}/player_or_team_abbreviation={p_or_t}/data"
                        list_of_dict = get_jsonl(jsonl)
                        with bigquery.get_client() as client:
                            client.load_table_from_json(
                                json_rows=list_of_dict,
                                destination=f"{bigquery.project}.nba.{table_name}",
                                job_config=bq.build_load_job_config(schema, "append"),
                            )

            return

        return _bq_table_asset
