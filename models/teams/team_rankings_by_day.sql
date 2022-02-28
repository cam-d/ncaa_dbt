/*
POM - pomeroy 
AP - assc. press 
USA - USA coaches poll
EBP - ESPN BPI
NET - NCAA
MAS - Massey
TRP - teamrankings pred
https://masseyratings.com/cb/compare.htm
*/



select 
    season 
    , teamid
    , RankingDayNum AS daynum
    , MAX(case when systemname = 'AP' THEN ordinalrank END) AS ap 
    , MAX(case when systemname = 'POM' THEN ordinalrank END) AS pom 
    , MAX(case when systemname = 'USA' THEN ordinalrank END) AS usa 
    , MAX(case when systemname = 'EBP' THEN ordinalrank END) AS espn
    , MAX(case when systemname = 'NET' THEN ordinalrank END) AS net 
    , MAX(case when systemname = 'MAS' THEN ordinalrank END) AS mas 
    , MAX(case when systemname = 'TRP' THEN ordinalrank END) AS trp
    , max(ordinalrank) as worst_rank
    , min(ordinalrank) as best_rank
    , STDDEV_SAMP(ordinalrank) AS st_dev 
    , AVG(ordinalrank) AS avg_rank
    , MAX(MAX(ordinalrank)) OVER (PARTITION BY season, teamid) AS max_season
from {{ source('ncaa', 'mmasseyordinals') }} 
group by
    season
    , teamid
    , daynum