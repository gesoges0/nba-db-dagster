WITH total_by_season AS (
  SELECT
    PLAYER_NAME,
    SUM(PTS) AS total
  FROM
    `sample-437713.nba.leaguegamelog2_player`
  WHERE
    _SEASON_TYPE = "Regular Season"
  GROUP BY
    PLAYER_NAME,
    SEASON_ID
)
SELECT
  PLAYER_NAME,
  COUNT(CASE WHEN total >= 2000 THEN 1 END) AS total_2000_cnt
FROM
  total_by_season
GROUP BY
  PLAYER_NAME
ORDER BY
  2 DESC
LIMIT 
  20
