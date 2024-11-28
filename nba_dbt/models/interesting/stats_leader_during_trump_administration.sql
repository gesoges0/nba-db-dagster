WITH RankedStats AS (
  SELECT
    PLAYER_NAME,
    SUM(PTS) AS total_pts,
    SUM(AST) AS total_ast,
    SUM(REB) AS total_reb,
    SUM(BLK) AS total_blk,
    SUM(STL) AS total_stl,
    ROW_NUMBER() OVER (ORDER BY SUM(PTS) DESC) AS rank_pts,
    ROW_NUMBER() OVER (ORDER BY SUM(AST) DESC) AS rank_ast,
    ROW_NUMBER() OVER (ORDER BY SUM(REB) DESC) AS rank_reb,
    ROW_NUMBER() OVER (ORDER BY SUM(BLK) DESC) AS rank_blk,
    ROW_NUMBER() OVER (ORDER BY SUM(STL) DESC) AS rank_stl
  FROM
    `sample-437713.nba.leaguegamelog2_player`
  WHERE
    _SEASON_TYPE = "Regular Season" AND
    DATE(GAME_DATE) BETWEEN '2021-01-20' AND '2025-01-20'
  GROUP BY
    PLAYER_NAME
)
SELECT
  -- PTS
  MAX(CASE WHEN rank_pts = 1 THEN PLAYER_NAME END) AS top_scorer,
  MAX(total_pts) AS total_pts,
  MAX(CASE WHEN rank_pts = 2 THEN PLAYER_NAME END) AS top_scorer2,
  MAX(CASE WHEN rank_pts = 2 THEN total_pts END) AS total_pts2,
  MAX(CASE WHEN rank_pts = 3 THEN PLAYER_NAME END) AS top_scorer3,
  MAX(CASE WHEN rank_pts = 3 THEN total_pts END) AS total_pts3,
  -- AST
  MAX(CASE WHEN rank_ast = 1 THEN PLAYER_NAME END) AS top_assist,
  MAX(total_ast) AS total_ast,
  MAX(CASE WHEN rank_ast = 2 THEN PLAYER_NAME END) AS top_assist2,
  MAX(CASE WHEN rank_ast = 2 THEN total_ast END) AS total_ast2,
  MAX(CASE WHEN rank_ast = 3 THEN PLAYER_NAME END) AS top_assist3,
  MAX(CASE WHEN rank_ast = 3 THEN total_ast END) AS total_ast3,
  -- REB
  MAX(CASE WHEN rank_reb = 1 THEN PLAYER_NAME END) AS top_rebound,
  MAX(total_reb) AS total_reb,
  MAX(CASE WHEN rank_reb = 2 THEN PLAYER_NAME END) AS top_rebound2,
  MAX(CASE WHEN rank_reb = 2 THEN total_reb END) AS total_reb2,
  MAX(CASE WHEN rank_reb = 3 THEN PLAYER_NAME END) AS top_rebound3,
  MAX(CASE WHEN rank_reb = 3 THEN total_reb END) AS total_reb3,
  -- BLK
  MAX(CASE WHEN rank_blk = 1 THEN PLAYER_NAME END) AS top_block,
  MAX(total_blk) AS total_blk,
  MAX(CASE WHEN rank_blk = 2 THEN PLAYER_NAME END) AS top_block2,
  MAX(CASE WHEN rank_blk = 2 THEN total_blk END) AS total_blk2,
  MAX(CASE WHEN rank_blk = 3 THEN PLAYER_NAME END) AS top_block3,
  MAX(CASE WHEN rank_blk = 3 THEN total_blk END) AS total_blk3,
  -- STL
  MAX(CASE WHEN rank_stl = 1 THEN PLAYER_NAME END) AS top_steal,
  MAX(total_stl) AS total_stl,
  MAX(CASE WHEN rank_stl = 2 THEN PLAYER_NAME END) AS top_steal2,
  MAX(CASE WHEN rank_stl = 2 THEN total_stl END) AS total_stl2,
  MAX(CASE WHEN rank_stl = 3 THEN PLAYER_NAME END) AS top_steal3,
  MAX(CASE WHEN rank_stl = 3 THEN total_stl END) AS total_stl3,
FROM
  RankedStats