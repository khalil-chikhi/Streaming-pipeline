with base as (

    select * from {{ ref('stg_wiki_events') }}
    where event_type = 'edit'

)

select
    event_hour,
    event_date,
    wiki,
    server_name,

    count(*)                                    as total_edits,
    count(distinct editor)                      as unique_editors,

    count(*) filter (where editor_type = 'bot')     as bot_edits,
    count(*) filter (where editor_type = 'human')   as human_edits,

    round(
        count(*) filter (where editor_type = 'bot') * 100.0 / count(*),
        2
    )                                           as bot_pct,

    mode() within group (order by namespace)    as dominant_namespace

from base
group by 1, 2, 3, 4
order by event_hour desc, total_edits desc