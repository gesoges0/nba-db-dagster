from dagster import Definitions
from dagster_gcp import BigQueryResource

from endpoints.boxscoreadvancedv2 import InitBoxScoreAdvancedV2JobFactory
from endpoints.commonallplayers import CommonAllPlayersJobFactory
from endpoints.leaguegamelog import InitLeagueGameLogFactory, LeagueGameLogJobFactory
from endpoints.leaguegamelog2 import InitLeagueGameLogJobFactory2, LeagueGameLogJobFactory2
from endpoints.playergamelogs import PlayerGameLogsFactory

# commonallplayers
common_all_players_job_factory = CommonAllPlayersJobFactory()

# leaguegamelog
# init_league_game_log_factory = InitLeagueGameLogFactory()
# league_game_log_factory = LeagueGameLogJobFactory()

# leaguegamelog2
init_league_game_log_factory2 = InitLeagueGameLogJobFactory2()
league_game_log_factory2 = LeagueGameLogJobFactory2()

# boxscoreadvancedv2
init_box_score_advanced_v2_factory = InitBoxScoreAdvancedV2JobFactory()

# playergamelogs
init_player_gamelogs_factory = PlayerGameLogsFactory()


definitions = Definitions(
    assets=common_all_players_job_factory.assets
    # + init_league_game_log_factory.assets
    # + league_game_log_factory.assets
    + init_league_game_log_factory2.assets
    + league_game_log_factory2.assets
    + init_box_score_advanced_v2_factory.assets
    + init_player_gamelogs_factory.assets,
    asset_checks=init_box_score_advanced_v2_factory.asset_checks,
    jobs=[
        common_all_players_job_factory.create_job(),
        # init_league_game_log_factory.create_job(),
        # league_game_log_factory.create_job(),
        init_league_game_log_factory2.create_job(),
        league_game_log_factory2.create_job(),
        init_box_score_advanced_v2_factory.create_job(),
        init_player_gamelogs_factory.create_job(),
    ],
    resources={
        "bigquery": BigQueryResource(
            project="sample-437713",
        )
    },
)
