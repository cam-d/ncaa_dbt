{% set base_cols = ["game_id", "season"] %}
WITH base AS (
    SELECT 
        game_id 
        , season
        , team_a
        , team_b
        , team_a_win
        , tourney_type
    FROM {{ ref('features_base') }} AS base 
)

{% set stat_cols = ["avg_fgm", "avg_fga", "avg_fg_pct", "avg_fgm3", "avg_ftm", "avg_fta", "avg_oreb","avg_dreb","avg_ast"] %}

, a_stats AS (
    SELECT 
        teamid as a_teamid
        , season as a_season
        {% for col in stat_cols %}
        , {{ col_prefix(col, 'a') }}
        {% endfor %}
    FROM {{ ref('team_stats_by_season') }}
)

, b_stats AS (
    SELECT 
        teamid as b_teamid
        , season as b_season
        {% for col in stat_cols %}
        , {{ col_prefix(col, 'b') }}
        {% endfor %}
    FROM {{ ref('team_stats_by_season') }}
)

{% set win_cols = ["win_pct", "t_10_games", "t_10_wins",  "t_20_games", "t_20_wins", "avg_opp_rank"] %}

, a_wins AS (
     SELECT 
        teamid as aw_teamid
        , season as aw_season
        {% for col in win_cols %}
        , {{ col_prefix(col, 'aw') }}
        {% endfor %}
    FROM {{ ref('reg_season_wins') }}
)

, b_wins AS (
     SELECT 
        teamid as bw_teamid
        , season as bw_season
        {% for col in win_cols %}
        , {{ col_prefix(col, 'bw') }} 
        {% endfor %}
    FROM {{ ref('reg_season_wins') }}
)

{% set rank_cols = ["days_in_t10", "days_in_t20", "worst_rank", "best_rank", "recent_rank"] %}

, a_rank AS (
    SELECT 
        teamid as ar_teamid
        , season as ar_season
        {% for col in rank_cols %}
        , {{ col_prefix(col, 'ar') }} 
        {% endfor %}
    FROM {{ ref('team_rankings_by_season') }}
)

, b_rank AS (
    SELECT 
        teamid as br_teamid
        , season as br_season
        {% for col in rank_cols %}
        , {{ col_prefix(col, 'br') }} 
        {% endfor %}
    FROM {{ ref('team_rankings_by_season') }}
)


SELECT 
    base.*
   -- only using diffs
    {% for col in stat_cols %}
    , {{ col_diff(col) }}
    {% endfor %}
    
    {% for col in rank_cols %}
    , {{ col_diff(col, 'r') }}
    {% endfor %}

    {% for col in win_cols %}
    , {{ col_diff(col, 'w') }}
    {% endfor %}
    -- , a_stats.*
    -- , b_stats.*
    -- , a_wins.* 
    -- , b_wins.*
    -- , a_rank.*
    -- , b_rank.*
FROM base 
LEFT JOIN a_stats ON a_stats.a_teamid = base.team_a AND a_stats.a_season = base.season 
LEFT JOIN b_stats ON b_stats.b_teamid = base.team_b AND b_stats.b_season = base.season
LEFT JOIN a_wins ON a_wins.aw_teamid = base.team_a AND a_wins.aw_season = base.season 
LEFT JOIN b_wins ON b_wins.bw_teamid = base.team_b AND b_wins.bw_season = base.season 
LEFT JOIN a_rank ON a_rank.ar_teamid = base.team_a AND a_rank.ar_season = base.season 
LEFT JOIN b_rank ON b_rank.br_teamid = base.team_b AND b_rank.br_season = base.season 
