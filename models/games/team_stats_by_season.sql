select
    season
    , teamid
    , count(*) as games 
    , sum(win) as wins 
    , avg(win) as win_pct
    , avg(score) as avg_score 
    , avg(fgm) as avg_fgm 
    , avg(fga) as avg_fga 
    , avg(fgm/fga) as avg_fg_pct
    , avg(fgm3) as avg_fgm3 
    , avg(ftm) as avg_ftm 
    , avg(fta) as avg_fta
    , avg(oreb) as avg_oreb
    , avg(dreb) as avg_dreb
    , avg(ast) as avg_ast
    , avg(tos) as avg_tos
    , avg(stl) as avg_stl
    , avg(blk) as avg_blk
    , avg(pfouls) as avg_fouls
from {{ ref('base_regseason_details') }} 
group by season, teamid
order by teamid, season