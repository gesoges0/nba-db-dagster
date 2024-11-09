import json
import time
from datetime import date, datetime
from functools import lru_cache

import requests
from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetExecutionContext,
    AssetsDefinition,
    DailyPartitionsDefinition,
    RetryPolicy,
    asset,
    asset_check,
    define_asset_job,
)
from dagster_gcp import BigQueryResource
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

from endpoints.utils import (
    CF_EMU_URL,
    PLAYER_OR_TEAM_ABBREVIATIONS,
    SEASON_TYPES,
    STATS_BEGINNING_YEAR,
    build_season_name,
    file_glob_iterator,
    get_jsonl,
    get_jsonl_from_path,
    get_len_jsonl_in_raw_dir,
    is_exists,
    load_result,
    save_as_jsonl,
    save_result,
)
from helpers import bq
from helpers.nba import get_nba_season


@lru_cache
def iterate_game_id_and_date() -> list[tuple[str, datetime]]:
    """BQのsample-437713.nba.leaguegamelog2からgame_idとdateを取得"""
    with bigquery.Client() as client:
        query = "SELECT GAME_ID, GAME_DATE FROM `sample-437713.nba.leaguegamelog2_team` ORDER BY GAME_DATE"
        query_job = client.query(query)
        return list(query_job.result())


def iterate_game_date_and_cnt_from_boxscoreadvancedv2(player_or_team: str) -> list[tuple[datetime, int]]:
    with bigquery.Client() as client:
        query = f"SELECT _GAME_DATE, COUNT(*) AS cnt FROM `sample-437713.nba.boxscoreadvancedv2_{player_or_team}` GROUP BY _GAME_DATE"
        query_job = client.query(query)
        return list(query_job.result())


def delete_records_in_boxscoreadvancedv2(game_date: str, player_or_team: str, AB2000: str) -> None:
    with bigquery.Client() as client:
        query = f"DELETE FROM `sample-437713.nba.boxscoreadvancedv2_{player_or_team}_{AB2000}` WHERE _GAME_DATE = '{game_date}'"
        client.query(query).result()
    return


class InitBoxScoreAdvancedV2JobFactory:
    def __init__(self):
        self._endpoint_name = "boxscoreadvancedv2"
        self._save_name = f"{self._endpoint_name}"
        self._result_name = None
        self._raw_name = None

        # assets
        self._api_response_asset = self._api_response_asset_factory()
        self._api_response_asset_check = self._api_response_asset_check_factory()
        self._raw_data_assets = dict()
        self._raw_data_assets["player"] = self._raw_data_asset_factory("player")
        self._raw_data_assets["team"] = self._raw_data_asset_factory("team")
        self._bq_table_assets = dict()
        self._bq_table_assets["player"] = self._bq_table_asset_factory("player")
        self._bq_table_assets["team"] = self._bq_table_asset_factory("team")

    def get_result_name(self, game_id, game_date, season):
        return f"{self._save_name}/season={season}/game_date={game_date}/game_id={game_id}/data"

    def get_raw_name(self, game_id, game_date, season, player_or_team):
        return f"{self._save_name}/season={season}/game_date={game_date}/game_id={game_id}/player_or_team={player_or_team}/data"

    @property
    def assets(self) -> list[AssetsDefinition]:
        return [self._api_response_asset] + list(self._raw_data_assets.values()) + list(self._bq_table_assets.values())

    @property
    def asset_checks(self):
        return [self._api_response_asset_check]

    def create_job(self):
        return define_asset_job(
            name=f"{self._endpoint_name}_init",
            selection=self.assets,
            config={},
            description="boxscoreadvancedv2を取得",
        )

    def _api_response_asset_factory(self) -> AssetsDefinition:
        @asset(name=f"{self._endpoint_name}_api_response_init")
        def _api_response_asset(context: AssetExecutionContext) -> None:
            """nba_apiを叩いたレスポンス"""
            for game_id, _game_date in iterate_game_id_and_date():
                game_date = _game_date.strftime("%Y-%m-%d")
                season = build_season_name(_game_date.year)

                _result_name = self.get_result_name(game_id, game_date, season)

                if is_exists(_result_name, "result"):
                    continue

                context.log.info(f"game_id: {game_id}, game_date: {game_date}, season: {season}")
                try:
                    response = requests.get(
                        url=CF_EMU_URL,
                        json={"endpoint": self._endpoint_name, "params": {"game_id": game_id}},
                    )
                    save_result(
                        result=response.json(),
                        save_name=_result_name,
                    )
                    # time.sleep(2)
                except Exception as e:
                    print(e)
                    context.log.info(f"game_id: {game_id}, game_date: {game_date}, season: {season}, error: {e}")

            return

        return _api_response_asset

    def _api_response_asset_check_factory(self):
        @asset_check(asset=self._api_response_asset, blocking=True)
        def _api_response_asset_check(context: AssetCheckExecutionContext) -> AssetCheckResult:
            for game_id, _game_date in iterate_game_id_and_date():
                game_date = _game_date.strftime("%Y-%m-%d")
                season = build_season_name(_game_date.year)

                _result_name = self.get_result_name(game_id, game_date, season)

                if not is_exists(_result_name, "result"):
                    return AssetCheckResult(passed=False, metadata={"game_id": game_id, "game_date": game_date})

            return AssetCheckResult(passed=True)

        return _api_response_asset_check

    def _raw_data_asset_factory(self, player_or_team: str) -> AssetsDefinition:
        @asset(name=f"{self._endpoint_name}_raw_data_init_{player_or_team}", deps=[self._api_response_asset])
        def _raw_data_asset(context: AssetExecutionContext) -> None:
            """BQテーブルに保存するための生データ"""
            for game_id, _game_date in iterate_game_id_and_date():
                game_date = _game_date.strftime("%Y-%m-%d")
                season = build_season_name(_game_date.year)

                if _game_date < date(1996, 10, 1):
                    continue

                _result_name = self.get_result_name(game_id, game_date, season)
                _raw_name = self.get_raw_name(game_id, game_date, season, player_or_team)

                if is_exists(_raw_name, "raw"):
                    continue

                context.log.info(f"game_id: {game_id}, game_date: {game_date}, season: {season}")
                list_of_dict = load_result(_result_name)[f"{player_or_team.capitalize()}Stats"]
                if not list_of_dict:
                    continue
                for _d in list_of_dict:
                    _d["_GAME_DATE"] = game_date
                save_as_jsonl(
                    list_of_dict=list_of_dict,
                    save_name=_raw_name,
                )
            return

        return _raw_data_asset

    def _bq_table_asset_factory(self, player_or_team: str) -> AssetsDefinition:
        @asset(
            name=f"{self._endpoint_name}_bq_table_init_{player_or_team}", deps=[self._raw_data_assets[player_or_team]]
        )
        def _bq_table_asset(context: AssetExecutionContext, bigquery: BigQueryResource) -> None:
            """BQテーブルに保存する"""

            # schema
            with open(f"../infra/terraform/bigquery/boxscoreadvancedv2_{player_or_team}_A2000.json") as f:
                schema = json.load(f)

            # boxscoreadvancedv2の(game_date, cnt)のリストを取得
            cnt_by_game_date = {
                _game_date.strftime("%Y-%m-%d"): cnt
                for _game_date, cnt in iterate_game_date_and_cnt_from_boxscoreadvancedv2(player_or_team)
            }
            # leaguegamelog2のgame_dateのリストを取得
            game_dates = sorted(list({game_date for _, game_date in iterate_game_id_and_date()}))

            for _game_date in game_dates:
                game_date = _game_date.strftime("%Y-%m-%d")
                season = build_season_name(_game_date.year)
                B2000_or_A2000 = "B2000" if _game_date.year < 2000 else "A2000"

                # この日付のデータがローカルに何行分あるかを確認する
                local_cnt = get_len_jsonl_in_raw_dir(
                    f"{self._endpoint_name}/season={season}/game_date={game_date}/*/player_or_team={player_or_team}/data.jsonl"
                )

                # 既にboxscoreadvancedv2にデータがあり, ローカルのデータ行数とBQのデータ行数が一致している場合はスキップ
                if game_date in cnt_by_game_date and cnt_by_game_date[game_date] == local_cnt:
                    continue

                # ローカルのデータ行数が0の場合はスキップ
                if local_cnt == 0:
                    continue

                table_name = f"boxscoreadvancedv2_{player_or_team}_{B2000_or_A2000}${game_date.replace('-', '')}"

                # パーティションテーブルが存在しない場合は作成する
                if game_date not in cnt_by_game_date:
                    context.log.info(f"create partition table: {table_name}, 0 -> {local_cnt}")

                # ローカルのデータ行数とBQののデータ行数が一致していない場合は, BQのパーティションテーブルを置き換える
                else:
                    delete_records_in_boxscoreadvancedv2(game_date, player_or_team, B2000_or_A2000)
                    context.log.info(
                        f"replace partition table: {table_name}, cnt: {cnt_by_game_date[game_date]} -> {local_cnt}"
                    )

                # daily単位でまとめる
                json_rows = []
                for raw_path in file_glob_iterator(
                    glob_pattern=f"{self._endpoint_name}/season={season}/game_date={game_date}/game_id=*/player_or_team={player_or_team}/data.jsonl",
                    file_type="raw",
                ):
                    json_rows.extend(get_jsonl_from_path(raw_path))

                # 対象日のパーティションテーブルに対してデータを挿入
                with bigquery.get_client() as client:
                    # daily単位で挿入する
                    client.load_table_from_json(
                        json_rows=json_rows,
                        destination=f"{bigquery.project}.nba.{table_name}",
                        job_config=bq.build_load_job_config(schema, write_disposition="append"),
                    ).result()

            return

        return _bq_table_asset
