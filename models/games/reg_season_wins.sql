/*
Wins and performance based on opponent rank 

*/

SELECT 
    season 
    , teamid 
    , COALESCE(SUM(CASE WHEN opp_rank <= 10 THEN win END), 0) as t_10_wins 
    , COALESCE(SUM(CASE WHEN opp_rank BETWEEN 11 AND 20 THEN win END), 0) as t_20_wins
    , COALESCE(COUNT(CASE WHEN opp_rank <= 10 THEN opponent_id END), 0) AS t_10_games 
    , COALESCE(COUNT(CASE WHEN opp_rank BETWEEN 11 AND 20 THEN opponent_id END), 0) AS t_20_games
    , AVG(opp_rank) AS avg_opp_rank 
FROM {{ ref('reg_season_matchups') }}
GROUP BY 
    season
    , teamid 