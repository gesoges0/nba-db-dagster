-- ファイナルのマッチアップを取得
-- 最後の日で勝ったチームがシリーズを制したチーム
SELECT
  MAX(SEASON_ID) AS season_id,
  MAX(CASE WHEN WL = "W" THEN TEAM_ABBREVIATION ELSE "" END) AS winner,
  MAX(CASE WHEN WL = "L" THEN TEAM_ABBREVIATION ELSE "" END) AS loser
FROM
  `sample-437713.nba.leaguegamelog2_team` AS lglt
WHERE
  GAME_DATE IN (
    SELECT final_game_date FROM `sample-437713.nba_dbt.final_game_date`
  )
GROUP BY GAME_DATE
ORDER BY 1