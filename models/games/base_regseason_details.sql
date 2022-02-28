WITH games AS (
    (select
    season, daynum, wteamid as teamid,lteamid as opponent_id, wscore as score 
    , wfgm as fgm , wfga as fga, wfgm3 as fgm3, wftm as ftm, wfta as fta, wor as oreb, wdr as dreb, wast as ast
    , wto as tos, wstl as stl, wblk as blk, wpf as pfouls
    , 1 as win
    from {{ source('ncaa', 'mregularseasondetailedresults') }} 
    )
    UNION ALL
    (select
    season, daynum, lteamid as teamid, WTeamID as opponent_id, lscore as score 
    , lfgm as fmg, lfga as fga, lfgm3 as fgm3, lftm as ftm, lfta as fta, lor as ored, ldr as dreb, last as ast
    , lto as tos, lstl as stl, lblk as blk, lpf as pfouls
    , 0 as win
    from {{ source('ncaa', 'mregularseasondetailedresults') }}
    )
)

SELECT * FROM games
order by teamid, season, daynum