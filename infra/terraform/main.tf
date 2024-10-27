# -----------------------------------------------------------------------------------------
# terraform import google_bigquery_dataset.dataset "projects/sample-437713/datasets/nba"

# provider "google" {
#   project = "sample-437713"
#   region  = "US"
# }

# locals {
#   dataset_id = "nba"
#   table_id   = "commonallplayers"
#   region     = "US"
# }

# # BigQuery dataset の作成
# resource "google_bigquery_dataset" "dataset" {
#   dataset_id = local.dataset_id
#   location   = local.region
# }

# # JSONファイルを読み込んでテーブルスキーマとして使用
# data "local_file" "schema" {
#   filename = "./bigquery/commonallplayers.json"
# }

# # BigQuery テーブルの作成
# resource "google_bigquery_table" "table" {
#   dataset_id = google_bigquery_dataset.dataset.dataset_id
#   table_id   = local.table_id

#   # schema_json に JSON スキーマを設定
#   schema = data.local_file.schema.content
# }
# -----------------------------------------------------------------------------------------
provider "google" {
  project = "sample-437713"
  region  = "US"
}

resource "google_bigquery_dataset" "dataset" {
  dataset_id = "nba"
  location   = "US"
}

locals {
  # テーブル設定ファイル(.config.json)を読み込む
  table_configs = {
    for file in fileset("${path.module}/bigquery", "*.config.json") :
    trimsuffix(basename(file), ".config.json") => merge(
      jsondecode(file("${path.module}/bigquery/${file}")),
      { table_id = trimsuffix(basename(file), ".config.json") }
    )
  }

  # スキーマファイル(.json)を読み込む
  table_schemas = {
    for file in fileset("${path.module}/bigquery", "*.json") :
    trimsuffix(basename(file), ".json") => jsondecode(file("${path.module}/bigquery/${file}"))
    if !endswith(file, ".config.json")
  }
}

resource "google_bigquery_table" "tables" {
  for_each = local.table_configs

  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = each.key

  deletion_protection = false

  schema = jsonencode(local.table_schemas[each.key])

  # パーティション設定が存在し、かつnullでない場合のみパーティションを設定
  dynamic "time_partitioning" {
    for_each = (
      try(each.value.partition_type, null) != null &&
      try(each.value.partition_column, null) != null
    ) ? [1] : []

    content {
      type  = each.value.partition_type
      field = each.value.partition_column
    }
  }

  description = each.value.description
}
