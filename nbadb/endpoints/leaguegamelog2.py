import json
import time
from datetime import datetime

import requests
from dagster import (
    AssetExecutionContext,
    AssetsDefinition,
    DailyPartitionsDefinition,
    RetryPolicy,
    asset,
    define_asset_job,
)
from dagster_gcp import BigQueryResource
from google.cloud.exceptions import GoogleCloudError

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

    def __init__(self):
        self._endpoint_name = "leaguegamelog"

        self._result_save_name = None
        self._partitions_def = DailyPartitionsDefinition(start_date="2024-10-28", timezone="Asia/Tokyo")

        # ----------------------------------------------------------------
        # initialize assets
        self._endpoint_name = "leaguegamelog"
        self._api_response_assets = {"T": [], "P": []}
        self._raw_data_assets = dict()
        self._bq_table_assets = dict()

        for p_or_t in PLAYER_OR_TEAM_ABBREVIATIONS.keys():
            for season_type_key in SEASON_TYPES.keys():
                self._api_response_assets[p_or_t].append(self._api_response_asset_factory(p_or_t, season_type_key))
            self._raw_data_assets[p_or_t] = self._raw_data_asset_factory(p_or_t, self._api_response_assets[p_or_t])
            self._bq_table_assets[p_or_t] = self._bq_table_asset_factory(p_or_t, self._raw_data_assets[p_or_t])

    @property
    def assets(self) -> list[AssetsDefinition]:
        return (
            self._api_response_assets["T"]
            + self._api_response_assets["P"]
            + [self._raw_data_assets["T"], self._raw_data_assets["P"]]
            + [self._bq_table_assets["T"], self._bq_table_assets["P"]]
        )

    def create_job(self):
        return define_asset_job(
            name=f"{self._endpoint_name}2",
            selection=self.assets,
            description="all league game logs を取得",
            config={"execution": {"config": {"multiprocess": {"max_concurrent": 2}}}},  # 同時実行数を2に制限
        )

    def _api_response_asset_factory(
        self,
        p_or_t: str,
        season_type_key: str,
    ) -> AssetsDefinition:
        @asset(
            name=f"{self._endpoint_name}_{p_or_t}_{season_type_key}_api_response2",
            retry_policy=RetryPolicy(max_retries=3, delay=30),
            partitions_def=self._partitions_def,
        )
        def _api_response_asset(context: AssetExecutionContext) -> None:
            """nba_apiを叩いたレスポンス"""
            partition_start_date = context.partition_time_window.start.date().isoformat()
            season = get_nba_season(partition_start_date)
            params = {
                "season": season,
                "player_or_team_abbreviation": p_or_t,
                "season_type_all_star": SEASON_TYPES[season_type_key],
            }
            response = requests.get(
                url=CF_EMU_URL,
                json={"endpoint": self._endpoint_name, "params": params},
            )
            save_name = f"{self._endpoint_name}2/season={season}/player_or_team_abbreviation={p_or_t}/season_type={season_type_key}/data"
            save_result(
                result=response.json(),
                save_name=save_name,
            )
            time.sleep(30)
            return

        return _api_response_asset

    def _raw_data_asset_factory(self, p_or_t: str, season_assets: list[AssetsDefinition]) -> AssetsDefinition:
        @asset(
            name=f"{self._endpoint_name}_{p_or_t}_raw_data2",
            deps=season_assets,
            partitions_def=self._partitions_def,
        )
        def _raw_data_asset(context: AssetExecutionContext) -> None:
            """BQテーブルに保存するための生データ"""
            partition_start_date = context.partition_time_window.start.date().isoformat()
            season = get_nba_season(partition_start_date)

            list_of_dict = []
            for season_type, season_type_value in SEASON_TYPES.items():
                d = load_result(
                    f"{self._endpoint_name}2/season={season}/player_or_team_abbreviation={p_or_t}/season_type={season_type}/data"
                )
                for _d in d["LeagueGameLog"]:
                    _d["_SEASON_TYPE"] = season_type_value

                list_of_dict.extend(d["LeagueGameLog"])

            save_name = f"{self._endpoint_name}2/season={season}/player_or_team_abbreviation={p_or_t}/data"
            save_as_jsonl(
                list_of_dict=list_of_dict,
                save_name=save_name,
            )
            return

        return _raw_data_asset

    def _bq_table_asset_factory(self, p_or_t: str, raw_data_asset: AssetsDefinition) -> AssetsDefinition:
        @asset(
            name=f"{self._endpoint_name}_{p_or_t}_bq_table2",
            deps=[raw_data_asset],
            partitions_def=self._partitions_def,
        )
        def _bq_table_asset(context: AssetExecutionContext, bigquery: BigQueryResource) -> None:
            """BQテーブルに保存"""
            partition_start_date = context.partition_time_window.start.date().isoformat()
            season = get_nba_season(partition_start_date)
            suffix = PLAYER_OR_TEAM_ABBREVIATIONS[p_or_t]
            table_name = (
                f"{self._endpoint_name}2_{suffix}"
                + ("_B2000" if season[0] == "1" else "_A2000")
                + f"${partition_start_date.replace('-', '')}"
            )
            with open(f"../infra/terraform/bigquery/leaguegamelog2_{suffix}.json", mode="r") as f:
                schema = json.load(f)

                jsonl = f"{self._endpoint_name}2/season={season}/player_or_team_abbreviation={p_or_t}/data"
                list_of_dict = [d for d in get_jsonl(jsonl) if d["GAME_DATE"] == partition_start_date]
                context.log.info(f"{len(list_of_dict)=}")
                with bigquery.get_client() as client:
                    client.load_table_from_json(
                        json_rows=list_of_dict,
                        destination=f"{bigquery.project}.nba.{table_name}",
                        job_config=bq.build_load_job_config(schema),
                    )

            return

        return _bq_table_asset


class InitLeagueGameLogJobFactory2:

    def __init__(self):
        self._endpoint_name = "leaguegamelog"

        self._result_save_name = None

        # ----------------------------------------------------------------
        # initialize assets
        self._api_response_assets = []
        self._raw_data_assets = {"T": [], "P": []}
        self._bq_table_assets = dict()

        for year in range(STATS_BEGINNING_YEAR, datetime.now().year + 1):
            for p_or_t in PLAYER_OR_TEAM_ABBREVIATIONS.keys():
                season_assets = []
                for season_type_key, season_type_value in SEASON_TYPES.items():
                    season = build_season_name(year)
                    api_response_asset = self._api_response_asset_factory(
                        season, p_or_t, season_type_key, season_type_value
                    )
                    season_assets.append(api_response_asset)
                    self._api_response_assets.append(api_response_asset)
                raw_data_asset = self._raw_data_asset_factory(season, p_or_t, season_assets)
                self._raw_data_assets[p_or_t].append(raw_data_asset)

        for p_or_t in PLAYER_OR_TEAM_ABBREVIATIONS.keys():
            self._bq_table_assets[p_or_t] = self._bq_table_asset_factory(p_or_t, self._raw_data_assets[p_or_t])
        # ----------------------------------------------------------------

    @property
    def assets(self) -> list[AssetsDefinition]:
        return (
            self._api_response_assets
            + self._raw_data_assets["T"]
            + self._raw_data_assets["P"]
            + [self._bq_table_assets["T"], self._bq_table_assets["P"]]
        )

    def create_job(self):
        return define_asset_job(
            name=f"{self._endpoint_name}2_init",
            selection=self.assets,
            description="all league game logs を取得",
            config={"execution": {"config": {"multiprocess": {"max_concurrent": 2}}}},  # 同時実行数を2に制限
        )

    def _api_response_asset_factory(
        self, season: str, p_or_t: str, season_type_key: str, season_type_value: str
    ) -> AssetsDefinition:
        @asset(
            name=f"{self._endpoint_name}_{season}_{p_or_t}_{season_type_key}_api_response_init2",
            retry_policy=RetryPolicy(max_retries=3, delay=30),
        )
        def _api_response_asset(context: AssetExecutionContext) -> None:
            """nba_apiを叩いたレスポンス"""
            # Cloud Functionsを叩く
            params = {"season": season, "player_or_team_abbreviation": p_or_t, "season_type_all_star": season_type_value}
            response = requests.get(
                url=CF_EMU_URL,
                json={"endpoint": self._endpoint_name, "params": params},
            )
            # 保存名
            save_name = f"{self._endpoint_name}2/season={season}/player_or_team_abbreviation={p_or_t}/season_type={season_type_key}/data"
            # レスポンスを保存
            save_result(
                result=response.json(),
                save_name=save_name,
            )
            time.sleep(30)
            return

        return _api_response_asset

    def _raw_data_asset_factory(
        self, season: str, player_or_team_abbreviation: str, season_assets: list[AssetsDefinition]
    ) -> AssetsDefinition:
        @asset(
            name=f"{self._endpoint_name}_{season}_{player_or_team_abbreviation}_raw_data_init2",
            deps=season_assets,
        )
        def _raw_data_asset(context: AssetExecutionContext) -> None:
            """BQテーブルに保存するための生データ"""
            list_of_dict = []
            for season_type, season_type_value in SEASON_TYPES.items():
                d = load_result(
                    f"{self._endpoint_name}2/season={season}/player_or_team_abbreviation={player_or_team_abbreviation}/season_type={season_type}/data"
                )
                for _d in d["LeagueGameLog"]:
                    _d["_SEASON_TYPE"] = season_type_value

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

    def _bq_table_asset_factory(self, p_or_t: str, raw_data_assets: list[AssetsDefinition]) -> AssetsDefinition:
        @asset(
            name=f"{self._endpoint_name}_{p_or_t}_bq_table_init2",
            deps=raw_data_assets,
        )
        def _bq_table_asset(context: AssetExecutionContext, bigquery: BigQueryResource) -> None:
            """BQテーブルに保存"""
            suffix = PLAYER_OR_TEAM_ABBREVIATIONS[p_or_t]

            with open(f"../infra/terraform/bigquery/leaguegamelog_{suffix}_B2000.json", mode="r") as f:
                schema = json.load(f)

                for year in range(STATS_BEGINNING_YEAR, datetime.now().year + 1):
                    season = build_season_name(year)
                    jsonl = f"{self._endpoint_name}2/season={season}/player_or_team_abbreviation={p_or_t}/data"
                    list_of_dict = get_jsonl(jsonl)
                    table_name = f"{self._endpoint_name}2_{suffix}" + ("_B2000" if year < 2000 else "_A2000")
                    context.log.info(f"{p_or_t=}, {season=}, {jsonl=}, {len(list_of_dict)=}")
                    with bigquery.get_client() as client:
                        try:
                            load_job = client.load_table_from_json(
                                json_rows=list_of_dict,
                                destination=f"{bigquery.project}.nba.{table_name}",
                                job_config=bq.build_load_job_config(schema, write_disposition="append"),
                            )
                            load_job.result()
                        except GoogleCloudError as e:
                            context.log.error(e)

            return

        return _bq_table_asset
