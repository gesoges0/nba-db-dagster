-- 1試合で20点以上のtriple doubleを2人がした試合を特定
WITH triple_double_two_men AS (
  SELECT
    GAME_ID,
    TEAM_ID,
    MAX(GAME_DATE) AS game_date
  FROM
    `sample-437713.nba.leaguegamelog2_player`
  GROUP BY
    GAME_ID, TEAM_ID
  HAVING
    SUM(CASE WHEN PTS>=20 AND AST>=10 AND REB>=10 THEN 1 ELSE 0 END) >= 2
)
-- 特定した試合と全試合レコードをLEFT JOINして, 特定した試合の各選手のスタッツを見る
SELECT l.GAME_DATE, _SEASON_TYPE, MATCHUP, TEAM_ABBREVIATION, PLAYER_NAME, PTS, AST, REB, BLK, STL, `MIN` FROM triple_double_two_men AS t LEFT JOIN `sample-437713.nba.leaguegamelog2_player` AS l
ON t.GAME_ID = l.GAME_ID AND t.TEAM_ID = l.TEAM_ID AND t.game_date = l.GAME_DATE
WHERE l.PTS >= 20 AND l.AST >= 10 AND l.REB >= 10
ORDER BY l.GAME_DATE