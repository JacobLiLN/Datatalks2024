{{
    config(
        materialized='view'
    )
}}

with tripdata as
(
    select dispatching_base_num,
    TIMESTAMP_MICROS(CAST(SUBSTRING(CAST(pickup_datetime AS STRING), 1, 16) AS INT64)) as pickup_datetime,
    TIMESTAMP_MICROS(CAST(SUBSTRING(CAST(dropOff_datetime AS STRING), 1, 16) AS INT64)) as dropOff_datetime,
    pulocationid,
    dolocationid,
    sr_flag,
    affiliated_base_number
    from {{ source('staging','fhv') }}
)

select *
from tripdata
WHERE EXTRACT(YEAR FROM pickup_datetime)=2019



