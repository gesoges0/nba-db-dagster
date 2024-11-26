SELECT
  CONCAT( SUBSTR(SEASON_ID, 2), '-', LPAD(CAST(CAST(SUBSTR(SEASON_ID, 4) AS INT64) + 1 AS STRING), 2, '0') ) AS season,
  MAX(PLAYER_NAME) AS name,
  SUM(FGA) AS total_fga,
  SUM(CASE WHEN MIN > 0 THEN 1 ELSE 0 END) AS games
FROM
  `sample-437713.nba_dbt.final_stats_player`
GROUP BY
  PLAYER_ID,
  SEASON_ID
HAVING
  total_fga >= 150
ORDER BY
  total_fga DESC