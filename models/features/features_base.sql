/*
Base template for the games, similar to prediction
Union of NCAA tournament games and Conference Tournament Games
*/

(SELECT 
    game_Id
    , season
    , team_a 
    , team_b 
    , team_a_seed 
    , team_b_seed 
    , team_a_win 
    , 'ncaa' AS tourney_type
FROM {{ ref('base_tourney_games') }}
)
UNION ALL 
(SELECT 
    game_Id
    , season
    , team_a 
    , team_b 
    -- seeds not included for conf tournaments
    , NULL AS team_a_seed
    , NULL As team_b_seed 
    , team_a_win 
    , 'conference' AS tourney_type
FROM {{ ref('base_conf_tourney_games') }}
)