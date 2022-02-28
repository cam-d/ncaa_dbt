select 
    season 
    , teamid 
    , count(distinct case when ordinalrank <=10 THEN RankingDayNum END ) days_in_t10
    , count(distinct case when ordinalrank BETWEEN 10 AND 20 THEN RankingDayNum END ) days_in_t20
    , max(ordinalrank) as worst_rank
    , min(ordinalrank) as best_rank
    , max(RankingDayNum) as last_day_ranked
    , avg(Case when RankingDayNum > 100 then ordinalrank end) as recent_rank
from {{ source('ncaa', 'mmasseyordinals') }} 
group by season, teamid