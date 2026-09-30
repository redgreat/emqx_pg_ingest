%%%-------------------------------------------------------------------
%%% @doc 插件 application：启动监督树并注册 EMQX 钩子
%%% @end
%%%-------------------------------------------------------------------
-module(emqx_pg_ingest_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_Type, _Args) ->
    {ok, Sup} = emqx_pg_ingest_sup:start_link(),
    ok = emqx_pg_ingest:load(),
    logger:notice("[emqx_pg_ingest] plugin started"),
    {ok, Sup}.

stop(_State) ->
    ok = emqx_pg_ingest:unload(),
    logger:notice("[emqx_pg_ingest] plugin stopped"),
    ok.
