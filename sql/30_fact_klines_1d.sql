-- Daily OHLCV derived from 1h candles

create table if not exists core.fact_klines_1d (
  symbol_id     int not null references core.dim_symbols(symbol_id),
  day_utc       date not null,
  open          numeric not null,
  high          numeric not null,
  low           numeric not null,
  close         numeric not null,
  volume        numeric,
  trades        bigint,
  quote_volume  numeric,
  primary key (symbol_id, day_utc)
);

with base as (
  select
    s.symbol_id,
    date_trunc('day', k.open_time_utc)::date as day_utc,
    k.open_time_utc, k.close_time_utc,
    k.open, k.high, k.low, k.close,
    k.volume, k.number_of_trades, k.quote_asset_volume
  from stg.klines_1h k
  join core.dim_symbols s on s.symbol = k.symbol
),
agg as (
  select
    symbol_id,
    day_utc,
    (array_agg(open  order by open_time_utc asc))[1] as open,
    (array_agg(close order by open_time_utc asc))[array_length(array_agg(close),1)] as close,
    max(high) as high,
    min(low)  as low,
    sum(volume)             as volume,
    sum(number_of_trades)   as trades,
    sum(quote_asset_volume) as quote_volume
  from base
  group by 1,2
)
insert into core.fact_klines_1d(symbol_id, day_utc, open, high, low, close, volume, trades, quote_volume)
select symbol_id, day_utc, open, high, low, close, volume, trades, quote_volume
from agg
on conflict (symbol_id, day_utc) do update
set open  = excluded.open,
    high  = excluded.high,
    low   = excluded.low,
    close = excluded.close,
    volume = excluded.volume,
    trades = excluded.trades,
    quote_volume = excluded.quote_volume;