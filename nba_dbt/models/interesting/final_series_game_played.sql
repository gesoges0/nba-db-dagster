WITH 
final_games AS (
  SELECT
    MAX(PLAYER_NAME) AS name,
    SUM(CASE WHEN MIN > 0 THEN 1 ELSE 0 END) AS games
  FROM
    `sample-437713.nba_dbt.final_stats_player`
  GROUP BY
    PLAYER_ID
  HAVING
    games >= 10
  ORDER BY
    games DESC
),
active_player AS (
  SELECT 
    * 
  FROM 
    `sample-437713.nba.leaguegamelog2_player` 
  WHERE 
    SEASON_ID = "22024"
)
SELECT
  distinct fg.name, fg.games
FROM
  final_games AS fg
LEFT JOIN
   active_player AS ap
ON 
  fg.name = ap.PLAYER_NAME
WHERE
  ap.PLAYER_ID IS NOT NULL
ORDER BY
  2 DESC