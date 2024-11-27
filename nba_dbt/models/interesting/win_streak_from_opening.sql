WITH GameResults AS (
  SELECT
    SEASON_ID,
    TEAM_ID,
    TEAM_NAME,
    GAME_DATE,
    WL,
    ROW_NUMBER() OVER (PARTITION BY SEASON_ID, TEAM_ID ORDER BY GAME_DATE) AS game_order
  FROM
    `sample-437713.nba.leaguegamelog2_team`
  WHERE
    _SEASON_TYPE = "Regular Season"
),
WinStreaks AS (
  SELECT
    SEASON_ID,
    TEAM_ID,
    TEAM_NAME,
    GAME_DATE,
    game_order,
    WL,
    CASE
      WHEN WL = 'L' THEN 0
      ELSE 1
    END AS win_flag,
    SUM(CASE WHEN WL = 'L' THEN 1 ELSE 0 END) OVER (
      PARTITION BY SEASON_ID, TEAM_ID ORDER BY game_order ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS loss_count
  FROM
    GameResults
),
FilteredStreaks AS (
  SELECT
    SEASON_ID,
    TEAM_ID,
    TEAM_NAME,
    game_order,
    win_flag,
    loss_count
  FROM
    WinStreaks
  WHERE
    loss_count = 0
),
FinalResult AS (
  SELECT
    SEASON_ID,
    TEAM_ID,
    TEAM_NAME,
    COUNT(*) AS opening_win_streak
  FROM
    FilteredStreaks
  GROUP BY
    SEASON_ID, TEAM_ID, TEAM_NAME
)
SELECT
  CONCAT( SUBSTR(SEASON_ID, 2), '-', LPAD(CAST(CAST(SUBSTR(SEASON_ID, 4) AS INT64) + 1 AS STRING), 2, '0') ) AS season,
  TEAM_ID,
  TEAM_NAME,
  opening_win_streak
FROM
  FinalResult
ORDER BY
  opening_win_streak DESC,
  SEASON_ID