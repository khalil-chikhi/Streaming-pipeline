with source as (

    select * from workspace.default.wikimedia_events

),

cleaned as (

    select
        event_id,
        event_type,
        page_title,
        namespace,
        editor,
        is_bot,
        editor_type,
        wiki,
        server_name,
        comment,
        event_time,
        processed_at,

        date_trunc('hour', event_time)  as event_hour,
        date_trunc('day', event_time)   as event_date

    from source
    where event_time is not null
      and event_type in ('edit', 'new', 'categorize', 'log')
      and event_id is not null

)

select * from cleaned