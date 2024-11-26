WITH
  pts_by_season AS (
  SELECT
    PLAYER_ID AS player_id,
    SEASON_ID AS season_id,
    MAX(PLAYER_NAME) AS player_name,
    SUM(PTS) AS total,
    SUM(CASE WHEN MIN > 0 THEN 1 ELSE 0 END) AS game
  FROM
    `sample-437713.nba.leaguegamelog2_player`
  WHERE
    _SEASON_TYPE = "Playoffs"
    AND SEASON_ID >= "42000"
  GROUP BY
    PLAYER_ID,
    SEASON_ID ),
  rookie_season AS (
  SELECT
    PLAYER_ID AS player_id,
    MIN(SEASON_ID) AS season_id
  FROM
    `sample-437713.nba.leaguegamelog2_player`
  WHERE
    _SEASON_TYPE = "Regular Season"
  GROUP BY
    PLAYER_ID )
SELECT
  CONCAT(SUBSTR(p.season_id, 2), '-', LPAD(CAST(CAST(SUBSTR(p.season_id, -2) AS INT64) + 1 AS STRING), 2, '0')) AS season,
  p.player_name, p.total, p.game
FROM
  rookie_season AS r
LEFT JOIN
  pts_by_season AS p
ON
  r.player_id = p.player_id AND
  CONCAT('4', SUBSTR(r.season_id, 2)) = p.season_id
WHERE
  p.total >= 85 -- p.total IS NOT NULL の意も含んでいる
ORDER BY
  p.season_id