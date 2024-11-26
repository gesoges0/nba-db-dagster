SELECT
  PLAYER_ID AS player_id,
  MAX(PLAYER_NAME) AS name,
  SUM(CASE WHEN FG3M >= 5 THEN 1 ELSE 0 END) AS total_fg3m_over_5_time_games
FROM
  `sample-437713.nba.leaguegamelog2_player`
WHERE
  _SEASON_TYPE = "Regular Season" OR _SEASON_TYPE = "Playoffs"
GROUP BY
  1
ORDER BY
  3 DESC
