{{
    config(
        materialized='table'
    )
}}

with fhv_tripdata as (
    select 
    pickup_datetime,
    dropOff_datetime,
    CAST(pulocationid as INT64) as pulocationid,
    CAST(dolocationid AS INT64) as dolocationid,
    sr_flag,
    affiliated_base_number, 
        'fhv' as service_type
    from {{ ref('stg_fhv') }}
    where pulocationid is not null and dolocationid is not null --and pulocationid<>
), 
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select 
f.*
,pu.borough AS pickup_borough
,pu.zone as pickup_zone
,dr.zone as dropoff_zone
,dr.borough as dropoff_borough
from fhv_tripdata as f 
inner join dim_zones as pu on f.pulocationid=pu.locationid
inner join dim_zones as dr on f.dolocationid=dr.locationid