from google.cloud.bigquery import LoadJobConfig, SourceFormat, WriteDisposition


def build_load_job_config(schema: list[dict[str, str]], write_disposition=None) -> LoadJobConfig:
    if write_disposition == "append":
        write_disposition = WriteDisposition.WRITE_APPEND
    else:
        write_disposition = WriteDisposition.WRITE_TRUNCATE

    return LoadJobConfig(
        source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=schema,
        write_disposition=write_disposition,
    )
