{{ config(materialized='table') }}
SELECT 
    RANK() OVER (ORDER BY COUNT(*) DESC) AS rank,
    PLAYER_ID AS id,
    MAX(PLAYER_NAME) AS name,
    COUNT(*) AS triple_double_count
FROM 
    `sample-437713.nba.leaguegamelog2_player`
WHERE
    _SEASON_TYPE = "Regular Season" AND
    PTS >= 10 AND 
    REB >= 10 AND 
    AST >= 10
GROUP BY 
    PLAYER_ID
ORDER BY 
    triple_double_count DESC