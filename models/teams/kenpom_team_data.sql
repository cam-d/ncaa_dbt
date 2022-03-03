SELECT 
    tm.teamid 
    , k.year
    , k.rank
    , k.wins
    , k.losses
    , k.seed
    , k.pyth
    , k.adjusto
    , k.adjusto_rank
    , k.adjustd
    , k.adjustd_rank
    , k.adjustt
    , k.adjustt_rank
    , k.luck
    , k.luck_rank
    , k.sos_pyth
    , k.sos_pyth_rank
    , k.sos_oppo
    , k.sos_oppo_rank
    , k.sos_oppd
    , k.sos_oppd_rank
    , k.ncsos_pyth
    , k.ncsos_pyth_rank
FROM {{ ref('team_mappings') }} AS tm
JOIN {{ ref('kenpom_data') }} AS k ON TRIM(k.team) = TRIM(tm.kenpom_team)

