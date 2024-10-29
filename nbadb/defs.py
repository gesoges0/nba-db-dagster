from dagster import Definitions
from dagster_gcp import BigQueryResource
from endpoints.commonallplayers import CommonAllPlayersJobFactory
from endpoints.leaguegamelog import InitLeagueGameLogFactory, LeagueGameLogJobFactory
from endpoints.leaguegamelog2 import InitLeagueGameLogJobFactory2, LeagueGameLogJobFactory2

# commonallplayers
common_all_players_job_factory = CommonAllPlayersJobFactory()

# leaguegamelog
init_league_game_log_factory = InitLeagueGameLogFactory()
league_game_log_factory = LeagueGameLogJobFactory()

# leaguegamelog2
init_league_game_log_factory2 = InitLeagueGameLogJobFactory2()

definitions = Definitions(
    assets=common_all_players_job_factory.assets
    + init_league_game_log_factory.assets
    + league_game_log_factory.assets
    + init_league_game_log_factory2.assets,
    jobs=[
        common_all_players_job_factory.create_job(),
        init_league_game_log_factory.create_job(),
        league_game_log_factory.create_job(),
        init_league_game_log_factory2.create_job(),
    ],
    resources={
        "bigquery": BigQueryResource(
            project="sample-437713",
        )
    },
)
