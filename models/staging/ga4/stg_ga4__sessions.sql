-- inventory of all sessions with dimensions and metrics such as:
-- dims: channel grouping, device, geo, starttime, endtime 
-- metrics: # of events, session event value, session purchase value

-- TODO - duplicate session_start events with the same session_id can fire with different pages, need a way to resolve ties

with session_start_dims as (
    select 
        concat(IFNULL(client_id, ''), IFNULL(cast(ga_session_id as STRING), '')) as session_key,
        ga_session_id,
        client_id,
        user_id,
        event_date_dt as session_date,
        traffic_source_campaign_name,
        traffic_source_source,
        traffic_source_medium,
        ga_session_number,
        page_location as landing_page,
        {{extract_hostname_from_url('page_location')}} as landing_page_hostname,
    from {{ref("stg_ga4__event_session_start")}}
)

select * from session_start_dims