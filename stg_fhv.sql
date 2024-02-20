{{
    config(
        materialized='view'
    )
}}

with tripdata as 
(
  select *
  where vendorid is not null 
)

select *
from tripdata