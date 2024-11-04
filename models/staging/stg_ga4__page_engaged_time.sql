with pek_time as (
    select
        page_engagement_key,
        event_date_dt,
        sum(engagement_time_msec) as page_engagement_time
    from {{ ref('stg_ga4__events') }}
    group by 1,2
),
matched_pv as ( -- need to replace the pek with one that uses page_location to match back to correct page_view
    select
        to_base64(md5(concat(session_key, page_location))) as page_engagement_key, 
        event_date_dt
    from {{ ref('stg_ga4__events') }}
    where event_name = 'page_view'
),
denominator as ( 
    select
        page_engagement_key,
        event_date_dt,
        count(page_engagement_key) as page_engagement_denominator --for sessions with multiple hits to the same page
    from matched_pv
    group by 1,2
)
select
    denominator.page_engagement_key,
    denominator.event_date_dt,
    case
        when pek_time.page_engagement_time is null then null -- safe_divide in the numerator would return 0; we need null to prevent page views with no recorded engagement time from factoring in to later calculations
        else safe_divide(pek_time.page_engagement_time , denominator.page_engagement_denominator) 
    end as page_engagement_time_msec, --technically the average engagement time for that page in that session
    case 
        when pek_time.page_engagement_time is null then null -- remove page_views with no engagement time from the denominator
        else denominator.page_engagement_denominator 
    end as page_engagement_denominator
from denominator 
left join pek_time
    on denominator.event_date_dt = pek_time.event_date_dt
    and denominator.page_engagement_key = pek_time.page_engagement_key
