-- https://x.com/statmuse/status/1803058583803982125
{{ config(materialized='table') }}
SELECT
  SEASON_ID,
  PLAYER_ID,
  MAX(PLAYER_NAME) AS player_name,
  SUM(PTS) AS total_points,
  SUM(REB) AS total_rebounds,
  SUM(AST) AS total_assists,
  SUM(STL) AS total_steals
FROM
  `sample-437713.nba.leaguegamelog2_player`
WHERE
  _SEASON_TYPE = "Playoffs"
GROUP BY
  SEASON_ID,
  PLAYER_ID
HAVING
  SUM(PTS) >= 600
  AND SUM(REB) >= 200
  AND SUM(AST) >= 175
  AND SUM(STL) >= 40