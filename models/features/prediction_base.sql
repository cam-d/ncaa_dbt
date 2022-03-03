SELECT 
    id as game_id
    , CAST(SPLIT(id, '_')[OFFSET(0)] AS INTEGER) AS season
    , CAST(SPLIT(id, '_')[OFFSET(1)] AS INTEGER) AS team_a   
    , CAST(SPLIT(id, '_')[OFFSET(2)] AS INTEGER) AS team_b
FROM {{ source('ncaa','msamplesubmissionstage1')}}