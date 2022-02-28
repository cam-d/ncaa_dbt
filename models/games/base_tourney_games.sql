/*
Format games the same way the prediction submission will be
year_lower teamid_higher teamid

team a = teamid of lower team
team b = teamid of higher team
team a win = if team a won
*/
with games as (
    SELECT 
        CONCAT(season,'_', LEAST(wteamid, lteamid), '_', GREATEST(wteamid, lteamid)) AS game_id
        , season
        , daynum
        , LEAST(wteamid, lteamid) as team_a
        , GREATEST(wteamid, lteamid) as team_b
        , CAST(LEAST(wteamid, lteamid) = WTeamID AS INT64) AS team_a_win
        , CASE WHEN LEAST(wteamid, lteamid) = WTeamID THEN wscore ELSE lscore END AS team_a_score
        , CASE WHEN GREATEST(wteamid, lteamid) = WTeamID THEN wscore ELSE lscore END AS team_b_score
        , wloc 
        , numot
    FROM {{ source('ncaa', 'mncaatourneycompactresults')}}
)

, add_seed as (
    select 
        games.*
        , s.seed AS team_a_seed 
        , s1.seed AS team_b_seed 
    FROM games 
    left join {{ source('ncaa', 'mncaatourneyseeds') }} as s on s.season = games.season and s.teamid = games.team_a
    left join {{ source('ncaa', 'mncaatourneyseeds') }} as s1 on s1.season = games.season and s1.teamid = games.team_b
)

SELECT * FROM add_seed