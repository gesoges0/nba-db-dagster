SELECT
  MAX(PLAYER_NAME) AS player_name,
  SUM(FG3M) AS total_fg3m,
  SUM(CASE
      WHEN MIN=0 OR MIN IS NULL THEN 0
      ELSE 1
  END
    ) AS total_games_played,
  SUM(MIN) AS total_minutes_played,
FROM
  `sample-437713.nba.leaguegamelog2_player`
WHERE
  _SEASON_TYPE = "Regular Season"
GROUP BY
  PLAYER_ID
ORDER BY
  SUM(FG3M) DESC
LIMIT 20