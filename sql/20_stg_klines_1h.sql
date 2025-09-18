-- STAGING (clean + dedupe) for 1h klines
drop materialized view if exists stg.klines_1h;

create materialized view stg.klines_1h as
with ranked as (
  select
    k.symbol,

    -- ms → timestamp
    to_timestamp(k.open_time::numeric  / 1000.0) as open_time_utc,
    to_timestamp(k.close_time::numeric / 1000.0) as close_time_utc,

    -- numeric casts
    k.open::numeric  as open,
    k.high::numeric  as high,
    k.low::numeric   as low,
    k.close::numeric as close,

    k.volume::numeric                 as volume,
    k.number_of_trades::bigint        as number_of_trades,
    k.quote_asset_volume::numeric     as quote_asset_volume,
    k.taker_buy_base_volume::numeric  as taker_buy_base_volume,
    k.taker_buy_quote_volume::numeric as taker_buy_quote_volume,

    -- pick the latest duplicate deterministically by close_time & open
    row_number() over (
      partition by k.symbol, k.open_time
      order by k.close_time desc, k.open desc
    ) as rn

  from raw.binance_klines k
)
select
  symbol, open_time_utc, close_time_utc,
  open, high, low, close,
  volume, number_of_trades, quote_asset_volume,
  taker_buy_base_volume, taker_buy_quote_volume
from ranked
where rn = 1
with no data;

create index if not exists ix_stg_klines_1h__symbol_time
  on stg.klines_1h(symbol, open_time_utc);

REFRESH MATERIALIZED VIEW stg.klines_1h;