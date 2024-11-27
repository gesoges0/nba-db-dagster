-- ファイナルでのスタッツ (チーム単位)
WITH playoff_games AS (
  SELECT * FROM `sample-437713.nba.leaguegamelog2_team` WHERE _SEASON_TYPE = "Playoffs"
)
SELECT
  pg.*
FROM
  {{ ref("final_matchup") }} AS fm
LEFT JOIN
  playoff_games AS pg 
ON
  fm.season_id = pg.SEASON_ID AND
  pg.MATCHUP IN (fm.winner || ' vs. ' || fm.loser, fm.winner || ' @ ' || fm.loser, fm.loser || ' vs. ' || fm.winner, fm.loser || ' @ ' || fm.winner)
ORDER BY
  fm.season_id DESC