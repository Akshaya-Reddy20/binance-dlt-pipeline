-- Dimension for trading pairs (symbols)
create table if not exists core.dim_symbols (
  symbol_id serial primary key,
  symbol       text unique not null,
  base_asset   text,
  quote_asset  text,
  status       text,
  is_active    boolean generated always as (status = 'TRADING') stored
);

-- Upsert from raw view, not directly from public
insert into core.dim_symbols(symbol, base_asset, quote_asset, status)
select distinct
  s.symbol,
  s.base_asset,
  s.quote_asset,
  coalesce(s.status, 'UNKNOWN') as status
from raw.binance_symbols s
on conflict (symbol) do update
set base_asset  = excluded.base_asset,
    quote_asset = excluded.quote_asset,
    status      = excluded.status;