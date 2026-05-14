with source as (

    select * from delta_scan('C:/tmp/delta/wikimedia_events')

),

cleaned as (

    select
        cast(id as varchar)         as event_id,
        type                        as event_type,
        title                       as page_title,
        namespace,
        user                        as editor,
        is_bot,
        wiki,
        server_name,
        comment,
        event_time,
        processed_at,

        case
            when is_bot = true then 'bot'
            else 'human'
        end                         as editor_type,


        date_trunc('hour', event_time) as event_hour,
        date_trunc('day', event_time)  as event_date

    from source
    where event_time is not null
      and type in ('edit', 'new', 'categorize', 'log')
      and id is not null
)

select * from cleaned