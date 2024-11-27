-- シーズンのファイナルの試合日を取得する
SELECT
  SEASON_ID,
  MAX(GAME_DATE) AS final_game_date
FROM
  `sample-437713.nba.leaguegamelog2_team`
WHERE
  _SEASON_TYPE = "Playoffs"
GROUP BY
  SEASON_ID
ORDER BY
  SEASON_ID