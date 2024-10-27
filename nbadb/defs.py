from dagster import Definitions
from dagster_gcp import BigQueryResource
from endpoints.commonallplayers import CommonAllPlayersJobFactory
from endpoints.leaguegamelog import InitLeagueGameLogFactory, LeagueGameLogJobFactory

# commonallplayers
common_all_players_job_factory = CommonAllPlayersJobFactory()

# leaguegamelog
init_league_game_log_factory = InitLeagueGameLogFactory()

definitions = Definitions(
    assets=common_all_players_job_factory.assets + init_league_game_log_factory.assets,
    jobs=[common_all_players_job_factory.create_job(), init_league_game_log_factory.create_job()],
    resources={
        "bigquery": BigQueryResource(
            project="sample-437713",
        )
    },
)
