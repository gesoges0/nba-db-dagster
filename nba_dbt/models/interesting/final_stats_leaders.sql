WITH final_ranking AS (
  SELECT
      PLAYER_NAME,
      SUM(PTS) AS pts,
      SUM(AST) AS ast,
      SUM(REB) AS reb,
      SUM(BLK) AS blk,
      SUM(STL) AS stl,
      SUM(FGM) AS fgm,
      SUM(FG3M) AS fg3m,
      SUM(FTM) AS ftm,
      ROW_NUMBER() OVER (ORDER BY SUM(PTS) DESC) AS rank_pts,
      ROW_NUMBER() OVER (ORDER BY SUM(AST) DESC) AS rank_ast,
      ROW_NUMBER() OVER (ORDER BY SUM(REB) DESC) AS rank_reb,
      ROW_NUMBER() OVER (ORDER BY SUM(BLK) DESC) AS rank_blk,
      ROW_NUMBER() OVER (ORDER BY SUM(STL) DESC) AS rank_stl,
      ROW_NUMBER() OVER (ORDER BY SUM(FGM) DESC) AS rank_fgm,
      ROW_NUMBER() OVER (ORDER BY SUM(FG3M) DESC) AS rank_fg3m,
      ROW_NUMBER() OVER (ORDER BY SUM(FTM) DESC) AS rank_ftm,
  FROM
    `nba_dbt.final_stats_player`
  GROUP BY
    PLAYER_NAME
)
SELECT
  -- pts
  MAX(CASE WHEN rank_pts = 1 THEN PLAYER_NAME ELSE "" END) AS top1_pts,
  MAX(CASE WHEN rank_pts = 1 THEN pts ELSE 0 END) AS pts1,
  MAX(CASE WHEN rank_pts = 2 THEN PLAYER_NAME ELSE "" END) AS top2_pts,
  MAX(CASE WHEN rank_pts = 2 THEN pts ELSE 0 END) AS pts2,
  MAX(CASE WHEN rank_pts = 3 THEN PLAYER_NAME ELSE "" END) AS top3_pts,
  MAX(CASE WHEN rank_pts = 3 THEN pts ELSE 0 END) AS pts3,
  -- ast
  MAX(CASE WHEN rank_ast = 1 THEN PLAYER_NAME ELSE "" END) AS top1_ast,
  MAX(CASE WHEN rank_ast = 1 THEN ast ELSE 0 END) AS ast1,
  MAX(CASE WHEN rank_ast = 2 THEN PLAYER_NAME ELSE "" END) AS top2_ast,  
  MAX(CASE WHEN rank_ast = 2 THEN ast ELSE 0 END) AS ast2,
  MAX(CASE WHEN rank_ast = 3 THEN PLAYER_NAME ELSE "" END) AS top3_ast,
  MAX(CASE WHEN rank_ast = 3 THEN ast ELSE 0 END) AS ast3,
  -- reb
  MAX(CASE WHEN rank_reb = 1 THEN PLAYER_NAME ELSE "" END) AS top1_reb,
  MAX(CASE WHEN rank_reb = 1 THEN reb ELSE 0 END) AS reb1,
  MAX(CASE WHEN rank_reb = 2 THEN PLAYER_NAME ELSE "" END) AS top2_reb,  
  MAX(CASE WHEN rank_reb = 2 THEN reb ELSE 0 END) AS reb2,
  MAX(CASE WHEN rank_reb = 3 THEN PLAYER_NAME ELSE "" END) AS top3_reb,
  MAX(CASE WHEN rank_reb = 3 THEN reb ELSE 0 END) AS reb3,
  -- blk
  MAX(CASE WHEN rank_blk = 1 THEN PLAYER_NAME ELSE "" END) AS top1_blk,
  MAX(CASE WHEN rank_blk = 1 THEN blk ELSE 0 END) AS blk1,
  MAX(CASE WHEN rank_blk = 2 THEN PLAYER_NAME ELSE "" END) AS top2_blk,  
  MAX(CASE WHEN rank_blk = 2 THEN blk ELSE 0 END) AS blk2,
  MAX(CASE WHEN rank_blk = 3 THEN PLAYER_NAME ELSE "" END) AS top3_blk,
  MAX(CASE WHEN rank_blk = 3 THEN blk ELSE 0 END) AS blk3,
  -- stl
  MAX(CASE WHEN rank_stl = 1 THEN PLAYER_NAME ELSE "" END) AS top1_stl,
  MAX(CASE WHEN rank_stl = 1 THEN stl ELSE 0 END) AS stl1,
  MAX(CASE WHEN rank_stl = 2 THEN PLAYER_NAME ELSE "" END) AS top2_stl,  
  MAX(CASE WHEN rank_stl = 2 THEN stl ELSE 0 END) AS stl2,
  MAX(CASE WHEN rank_stl = 3 THEN PLAYER_NAME ELSE "" END) AS top3_stl,
  MAX(CASE WHEN rank_stl = 3 THEN stl ELSE 0 END) AS stl3,
  -- fgm
  MAX(CASE WHEN rank_fgm = 1 THEN PLAYER_NAME ELSE "" END) AS top1_fgm,
  MAX(CASE WHEN rank_fgm = 1 THEN fgm ELSE 0 END) AS fgm1,
  MAX(CASE WHEN rank_fgm = 2 THEN PLAYER_NAME ELSE "" END) AS top2_fgm,  
  MAX(CASE WHEN rank_fgm = 2 THEN fgm ELSE 0 END) AS fgm2,
  MAX(CASE WHEN rank_fgm = 3 THEN PLAYER_NAME ELSE "" END) AS top3_fgm,
  MAX(CASE WHEN rank_fgm = 3 THEN fgm ELSE 0 END) AS fgm3,
  -- fg3m
  MAX(CASE WHEN rank_fg3m = 1 THEN PLAYER_NAME ELSE "" END) AS top1_fg3m,
  MAX(CASE WHEN rank_fg3m = 1 THEN fg3m ELSE 0 END) AS fg3m1,
  MAX(CASE WHEN rank_fg3m = 2 THEN PLAYER_NAME ELSE "" END) AS top2_fg3m,  
  MAX(CASE WHEN rank_fg3m = 2 THEN fg3m ELSE 0 END) AS fg3m2,
  MAX(CASE WHEN rank_fg3m = 3 THEN PLAYER_NAME ELSE "" END) AS top3_fg3m,
  MAX(CASE WHEN rank_fg3m = 3 THEN fg3m ELSE 0 END) AS fg3m3,
  -- ftm
  MAX(CASE WHEN rank_ftm = 1 THEN PLAYER_NAME ELSE "" END) AS top1_ftm,
  MAX(CASE WHEN rank_ftm = 1 THEN ftm ELSE 0 END) AS ftm1,
  MAX(CASE WHEN rank_ftm = 2 THEN PLAYER_NAME ELSE "" END) AS top2_ftm,  
  MAX(CASE WHEN rank_ftm = 2 THEN ftm ELSE 0 END) AS ftm2,
  MAX(CASE WHEN rank_ftm = 3 THEN PLAYER_NAME ELSE "" END) AS top3_ftm,
  MAX(CASE WHEN rank_ftm = 3 THEN ftm ELSE 0 END) AS ftm3,
FROM
  final_ranking