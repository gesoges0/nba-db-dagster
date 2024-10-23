from dagster import Definitions

from endpoints.commonallplayers import \
    InitCommonAllPlayersJobFactory as InitCommonAllPlayersJobFactory

init_common_all_players_job_factory = InitCommonAllPlayersJobFactory()


definitions = Definitions(
    assets=init_common_all_players_job_factory.assets,
    jobs=[init_common_all_players_job_factory.create_job()],
)
