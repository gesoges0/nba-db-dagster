{% macro generate_total_query(stats_name) %}
{{ config(materialized='table') }}

SELECT
    RANK() OVER (ORDER BY SUM({{stats_name}}) DESC) AS rank,
    PLAYER_ID AS id,
    MAX(PLAYER_NAME) AS name,
    SUM({{stats_name}}) AS total,
FROM
    `sample-437713.nba.leaguegamelog2_player`
WHERE
    _SEASON_TYPE = "Regular Season"
GROUP BY
    PLAYER_ID
ORDER BY
    total DESC
{% endmacro %}