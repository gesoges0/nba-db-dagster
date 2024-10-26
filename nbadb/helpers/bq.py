from google.cloud.bigquery import LoadJobConfig, SourceFormat, WriteDisposition


def build_load_job_config(schema: list[dict[str, str]]) -> LoadJobConfig:
    return LoadJobConfig(
        source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=schema,
        write_disposition=WriteDisposition.WRITE_TRUNCATE,
    )
