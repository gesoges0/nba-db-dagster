-- BOSのPlayoffs Finalで最も3-Pointを決めた選手
SELECT
  PLAYER_ID,
  MAX(PLAYER_NAME) AS player_name,
  SUM(FG3M) AS total_fg3m
FROM
  {{ ref("final_stats_player") }}
WHERE
  TEAM_ABBREVIATION = "BOS"
GROUP BY
  1
ORDER BY
  3 DESC
