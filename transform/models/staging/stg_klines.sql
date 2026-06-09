-- Binance returns prices as strings and times as epoch-ms. Cast + clean here.
-- Postgres: to_timestamp() takes epoch *seconds*, so divide ms by 1000.0.
with source as (
    select * from {{ source('binance_raw', 'klines') }}
)

select
    symbol,
    to_timestamp(open_time / 1000.0)        as open_time,
    to_timestamp(close_time / 1000.0)       as close_time,
    cast(open   as double precision)         as open,
    cast(high   as double precision)         as high,
    cast(low    as double precision)         as low,
    cast(close  as double precision)         as close,
    cast(volume as double precision)         as volume,
    trades
from source
