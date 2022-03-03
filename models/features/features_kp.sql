{% set base_cols = ["game_id", "season"] %}
WITH base AS (
    SELECT 
        game_id 
        , season
        , team_a
        , team_b
        , team_a_win
    FROM {{ ref('features_base') }} AS base 
)

{% set kp_cols = ["rank", "seed", "pyth", "adjusto", "adjusto_rank", "adjustd_rank", "adjustt_rank", "sos_pyth_rank", "sos_oppd_rank", "ncsos_pyth_rank"] %}

, a_stats AS (
    SELECT 
        teamid as a_teamid
        , year as a_season
        {% for col in kp_cols %}
        , {{ col_prefix(col, 'a') }}
        {% endfor %}
    FROM {{ ref('kenpom_team_data') }}
)

, b_stats AS (
    SELECT 
        teamid as b_teamid
        , year as b_season
        {% for col in kp_cols %}
        , {{ col_prefix(col, 'b') }}
        {% endfor %}
    FROM {{ ref('kenpom_team_data') }}
)

SELECT 
    base.*
    {% for col in kp_cols %}
    , {{ col_diff(col) }}
    {% endfor %}
FROM base 
LEFT JOIN a_stats ON a_stats.a_teamid = base.team_a AND a_stats.a_season = base.season 
LEFT JOIN b_stats ON b_stats.b_teamid = base.team_b AND b_stats.b_season = base.season