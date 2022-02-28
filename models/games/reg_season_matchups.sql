select 
    g.*
    ,  r.avg_rank AS opp_rank
    , r.max_season AS max_opp_rank
from {{ ref('base_regseason_details') }} as g 
left join {{ ref('team_rankings_by_day') }} as r on r.teamid = g.opponent_id
    and r.season = g.season 
    and r.daynum <= g.daynum
where 1=1 --win = 1
QUALIFY ROW_NUMBER() OVER(PARTITION BY g.season, g.daynum, g.teamid ORDER BY r.daynum DESC) = 1
order by g.season, g.daynum, r.daynum DESC