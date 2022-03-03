/*
Base template for the games, similar to prediction
*/

SELECT 
    game_Id
    , season
    , team_a 
    , team_b 
    , team_a_seed 
    , team_b_seed 
    , team_a_win 
FROM {{ ref('base_tourney_games') }}