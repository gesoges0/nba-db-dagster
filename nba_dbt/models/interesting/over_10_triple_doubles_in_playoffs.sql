SELECT
  PLAYER_NAME AS name,
  COUNT(CASE WHEN PTS >= 10 AND AST >= 10 AND REB >= 10 THEN 1 END) AS triple_double,
  COUNT(CASE WHEN MIN > 0 THEN 1 END) AS games,
FROM
  `nba.leaguegamelog2_player`
WHERE
  _SEASON_TYPE = "Playoffs"
GROUP BY
  PLAYER_NAME
HAVING
  triple_double >= 10
ORDER BY
  triple_double DESC