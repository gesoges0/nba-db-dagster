SELECT
  PLAYER_NAME,
  SUM(CASE WHEN PTS >= 25 AND FG_PCT >= 0.5 THEN 1 ELSE 0 END) AS games
FROM
  `nba.leaguegamelog2_player`
WHERE
  _SEASON_TYPE ="Playoffs"
GROUP BY
  PLAYER_NAME
ORDER BY
  games DESC
LIMIT 20