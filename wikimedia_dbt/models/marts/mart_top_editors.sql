with base as (

    select * from {{ ref('stg_wiki_events') }}
    where event_type = 'edit'
      and editor_type = 'human'

)

select
    editor,
    wiki,
    event_date,

    count(*)                        as total_edits,
    count(distinct page_title)      as unique_pages_edited,
    min(event_time)                 as first_edit,
    max(event_time)                 as last_edit

from base
group by 1, 2, 3
order by total_edits desc