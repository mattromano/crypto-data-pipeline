-- Roll the (hourly) klines up to a daily OHLCV bar per symbol.
-- Postgres has no first()/last() aggregate, so we take the first/last element of
-- an ordered array_agg: [1] of an asc array is the open, [1] of a desc array is
-- the close.
with klines as (
    select * from {{ ref('stg_klines') }}
)

select
    symbol,
    date_trunc('day', open_time)                    as trade_date,
    (array_agg(open  order by open_time asc))[1]    as open,
    max(high)                                       as high,
    min(low)                                        as low,
    (array_agg(close order by open_time desc))[1]   as close,
    sum(volume)                                     as volume,
    sum(trades)                                     as trades
from klines
group by symbol, date_trunc('day', open_time)
order by symbol, trade_date
