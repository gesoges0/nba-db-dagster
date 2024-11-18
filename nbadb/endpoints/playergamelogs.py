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
    save_memo,
    load_memo,
)
from helpers import bq
from helpers.nba import get_nba_season
from functools import lru_cache
from google.cloud import bigquery
import pandas as pd


@lru_cache
def iterate_active_players(year: int) -> list[str]:
    """BQのsample-437713.nba.leaguegamelog2からgame_idとdateを取得"""
    with bigquery.Client() as client:
        query = (
            f'SELECT PERSON_ID FROM `sample-437713.nba.commonallplayers` WHERE TO_YEAR = "{year}" ORDER BY TO_YEAR DESC'
        )
        query_job = client.query(query)
        return list(query_job.result())


@lru_cache
def iterate_all_players() -> list[str]:
    """BQのsample-437713.nba.leaguegamelog2からgame_idとdateを取得"""
    with bigquery.Client() as client:
        query = "SELECT PERSON_ID FROM `sample-437713.nba.commonallplayers` ORDER BY TO_YEAR DESC"
        query_job = client.query(query)
        return list(query_job.result())


@lru_cache
def iterable_season_year_and_game_cnt_from_playergamelogs() -> list[tuple[str, int]]:
    with bigquery.Client() as client:
        query = "SELECT GAME_DATE, COUNT(*) AS cnt FROM `sample-437713.nba.playergamelogs` GROUP BY SEASON_YEAR ORDER BY SEASON_YEAR"
        query_job = client.query(query)
        return list(query_job.result())


def game_date_and_cnt_from_leaguegamelog2_player() -> dict[datetime, int]:
    with bigquery.Client() as client:
        query = "SELECT GAME_DATE, COUNT(*) AS cnt FROM `sample-437713.nba.leaguegamelog2_player` GROUP BY 1 ORDER BY 1"
        query_job = client.query(query)
        return {row["GAME_DATE"]: row["cnt"] for row in query_job.result()}


def game_date_and_cnt_from_playergamelogs() -> dict[datetime, int]:
    with bigquery.Client() as client:
        query = "SELECT _GAME_DATE, COUNT(*) AS cnt FROM `sample-437713.nba.playergamelogs` GROUP BY 1 ORDER BY 1"
        query_job = client.query(query)
        return {row["_GAME_DATE"]: row["cnt"] for row in query_job.result()}


class PlayerGameLogsFactory:
    def __init__(self):
        self._endpoint_name = "playergamelogs"

        # assets
        self._api_response_assets = {
            "regular": self._api_response_asset_factory("regular"),
            "playoffs": self._api_response_asset_factory("playoffs"),
        }
        self._raw_data_asset = self._raw_data_asset_factory()
        self._bq_table_assets = self._bq_table_asset_factory()

    def _get_result_name(self, season: str, season_type: str):
        return f"{self._endpoint_name}/season={season}/season_type={season_type}/data"

    def _get_raw_name(self, season: str):
        return f"{self._endpoint_name}/season={season}/data"

    @property
    def assets(self) -> list[AssetsDefinition]:
        return [
            self._api_response_assets["regular"],
            self._api_response_assets["playoffs"],
            self._raw_data_asset,
            self._bq_table_assets,
        ]

    def create_job(self):
        return define_asset_job(
            name=f"{self._endpoint_name}",
            selection=self.assets,
            description="選手のplayergamelogs",
            config={"execution": {"config": {"multiprocess": {"max_concurrent": 2}}}},  # 同時実行数を2に制限
        )

    def _api_response_asset_factory(self, season_type) -> AssetsDefinition:
        @asset(
            name=f"{self._endpoint_name}_{season_type}_api_response",
            retry_policy=RetryPolicy(max_retries=3, delay=30),
        )
        def _api_response_asset(context: AssetExecutionContext) -> None:
            """nba_apiを叩いたレスポンス"""
            this_year = datetime.now().year

            for year in range(STATS_BEGINNING_YEAR, this_year + 1):
                season = build_season_name(year)
                save_name = self._get_result_name(season=season, season_type=season_type)
                if year != this_year and is_exists(save_name, "result"):
                    continue
                context.log.info(f"api_response, season: {season}")
                params = {"season_nullable": season, "season_type_nullable": SEASON_TYPES[season_type]}
                response = requests.get(
                    url=CF_EMU_URL,
                    json={
                        "endpoint": self._endpoint_name,
                        "params": params,
                    },
                )
                save_result(
                    result=response.json(),
                    save_name=save_name,
                )
                time.sleep(30)

        return _api_response_asset

    def _raw_data_asset_factory(self):
        @asset(
            name=f"{self._endpoint_name}_raw_data",
            deps=[self._api_response_assets["regular"], self._api_response_assets["playoffs"]],
            retry_policy=RetryPolicy(max_retries=3, delay=30),
        )
        def _raw_data_asset(context: AssetExecutionContext) -> None:
            """BQテーブルに保存するための生データ"""
            this_year = datetime.now().year
            for year in range(STATS_BEGINNING_YEAR, this_year + 1):
                season = build_season_name(year)
                result_names = {
                    "regular": self._get_result_name(season=season, season_type="regular"),
                    "playoffs": self._get_result_name(season=season, season_type="playoffs"),
                }
                if (
                    year != this_year
                    and is_exists(result_names["regular"], "result")
                    and is_exists(result_names["playoffs"], "result")
                    and is_exists(self._get_raw_name(season=season), "raw")
                ):
                    continue
                context.log.info(f"raw_data, season: {season}")
                save_name = self._get_raw_name(season=season)
                list_of_dict = []
                for season_type in SEASON_TYPES:
                    context.log.info(f"season_type: {season_type}")
                    d = load_result(result_names[season_type])["PlayerGameLogs"]
                    if len(d) == 0:
                        continue
                    # df = pd.DataFrame(d)
                    # df = df.where(pd.notnull(df), None)
                    # df["_SEASON_TYPE"] = SEASON_TYPES[season_type]
                    # df["_GAME_DATE"] = pd.to_datetime(df["GAME_DATE"]).dt.strftime("%Y-%m-%d")
                    # list_of_dict += df.to_dict(orient="records")
                    for _ in d:
                        _["_SEASON_TYPE"] = SEASON_TYPES[season_type]
                        _["_GAME_DATE"] = _["GAME_DATE"].split("T")[0]
                    list_of_dict.extend(d)
                save_as_jsonl(
                    list_of_dict=list_of_dict,
                    save_name=save_name,
                )
            return

        return _raw_data_asset

    def _bq_table_asset_factory(self):

        @asset(
            name=f"{self._endpoint_name}_bq_table",
            deps=[self._raw_data_asset],
            retry_policy=RetryPolicy(max_retries=3, delay=30),
        )
        def _bq_table_asset(context: AssetExecutionContext, bigquery: BigQueryResource) -> None:
            """BQテーブルに保存"""
            # # 初回
            # memo_name = f"{self._endpoint_name}/memo"
            # if not is_exists(memo_name, "memo"):
            #     memo = {"years": []}
            #     save_memo(memo, memo_name)
            # else:
            #     memo = load_memo(memo_name)
            # for year in range(STATS_BEGINNING_YEAR, datetime.now().year + 1):
            #     if year in memo["years"]:
            #         continue
            #     season = build_season_name(year)
            #     save_name = self._get_raw_name(season=season)
            #     list_of_dict = get_jsonl(save_name)
            #     context.log.info(f"bq_table, season: {season}, len(jsonl): {len(list_of_dict)}")
            #     if season[0] == "1":
            #         suffix = "B2000"
            #     else:
            #         suffix = "A2000"
            #     with open(f"../infra/terraform/bigquery/playergamelogs_{suffix}.json", mode="r") as f:
            #         schema = json.load(f)
            #     with bigquery.get_client() as client:
            #         client.load_table_from_json(
            #             json_rows=list_of_dict,
            #             destination=f"nba.playergamelogs_{suffix}",
            #             job_config=bq.build_load_job_config(schema, write_disposition="append"),
            #         ).result()
            #         memo["years"].append(year)
            #         save_memo(memo, memo_name)
            #         time.sleep(30)
            # return

            # 次回
            cnt_by_game_date_in_playergamelogs = game_date_and_cnt_from_playergamelogs()
            cnt_by_game_date_in_gamleog2_player = game_date_and_cnt_from_leaguegamelog2_player()

            for game_date, cnt in cnt_by_game_date_in_gamleog2_player.items():
                # 無かったら または 件数が違ったら テーブルに挿入
                if (
                    game_date not in cnt_by_game_date_in_playergamelogs
                    or cnt_by_game_date_in_playergamelogs[game_date] != cnt
                ):
                    context.log.info(
                        f"skip: bq_table, game_date: {game_date}, {cnt=}, {cnt_by_game_date_in_playergamelogs.get(game_date)=}"
                    )
                    # seasonごとにrawデータがまとまっているため, seasonを作成する
                    if game_date.month <= 9:
                        season = game_date.year - 1
                    else:
                        season = game_date.year
                    raw_name = self._get_raw_name(build_season_name(season))
                    list_of_dict = get_jsonl(raw_name)
                    filtered_list_of_dict = [
                        d for d in list_of_dict if d["_GAME_DATE"] == game_date.strftime("%Y-%m-%d")
                    ]

                    # rawのデータ個数とplayergamelogsの個数が同じだったらロードしても意味ない
                    if len(filtered_list_of_dict) == cnt_by_game_date_in_playergamelogs.get(game_date):
                        context.log.info(
                            f"skip: len(filtered_list_of_dict) == cnt_by_game_date_in_playergamelogs.get(game_date), {len(filtered_list_of_dict)=}, {cnt_by_game_date_in_playergamelogs.get(game_date)=}"
                        )
                        continue

                    # 新たにrawからデータをロードする
                    if str(game_date.year)[0] == "1":
                        suffix = "B2000"
                    else:
                        suffix = "A2000"
                    with open(f"../infra/terraform/bigquery/playergamelogs_{suffix}.json", mode="r") as f:
                        schema = json.load(f)
                    context.log.info(
                        f"load {game_date} playergamelogs_{suffix}: {cnt_by_game_date_in_playergamelogs.get(game_date)=} -> {len(filtered_list_of_dict)}"
                    )
                    with bigquery.get_client() as client:
                        load_job = client.load_table_from_json(
                            json_rows=filtered_list_of_dict,  # 日にち単位のjsonl,
                            destination=f"{bigquery.project}.nba.playergamelogs_{suffix}${game_date.strftime('%Y%m%d')}",
                            job_config=bq.build_load_job_config(schema),
                        )
                        load_job.result()
            return

        return _bq_table_asset
