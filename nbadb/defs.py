from dagster import Definitions
from dagster_gcp import BigQueryResource

from endpoints.commonallplayers import CommonAllPlayersJobFactory

common_all_players_job_factory = CommonAllPlayersJobFactory()


definitions = Definitions(
    assets=common_all_players_job_factory.assets,
    jobs=[common_all_players_job_factory.create_job()],
    resources={
        "bigquery": BigQueryResource(
            project="sample-437713",
        )
    },
)
