%%%-------------------------------------------------------------------
%%% @doc 插件监督树：N 个 PG 写入 worker（按 file_name 散列分派，保证同批次顺序）
%%% @end
%%%-------------------------------------------------------------------
-module(emqx_pg_ingest_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    Cfg = emqx_pg_ingest_config:load(),
    PoolSize = emqx_pg_ingest_config:get_int(Cfg, [<<"pg">>, <<"pool_size">>], 2),
    Workers = [
        #{
            id => {emqx_pg_ingest_pg, I},
            start => {emqx_pg_ingest_pg, start_link, [I, Cfg]},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [emqx_pg_ingest_pg]
        }
     || I <- lists:seq(1, max(1, PoolSize))
    ],
    {ok, {{one_for_one, 10, 10}, Workers}}.
