%%%-------------------------------------------------------------------
%%% @doc PostgreSQL 写入 worker（每实例持有一条连接，串行执行，保证同批次顺序）
%%%
%%% SQL 全部来自配置（profiles.<name>），字段用「字段名列表 + $n 占位」映射：
%%%   imp_fields / imp            批次记录（imp_racebox 之类）
%%%   record_fields / record      明细记录（lc_racebox 之类），逐行执行于同一事务
%%%   complete_dedup              已完整轨迹段查询，命中即跳过整段重导入
%%%   dedup                       MQTT 批次查询，命中即跳过 QoS 重投
%%%   batch / finalize            记录批次并刷新轨迹段完整性
%%% 写入失败会记日志、回滚并断开连接，下一条消息自动重连。
%%% @end
%%%-------------------------------------------------------------------
-module(emqx_pg_ingest_pg).

-behaviour(gen_server).

-export([start_link/2, ingest/2, worker_name/1, select_profile/2, normalize_sql/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(STATS, emqx_pg_ingest_stats).

-record(st, {index, cfg, conn = undefined}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Index, Cfg) ->
    gen_server:start_link({local, worker_name(Index)}, ?MODULE, {Index, Cfg}, []).

worker_name(Index) ->
    list_to_atom("emqx_pg_ingest_pg_" ++ integer_to_list(Index)).

%% @doc 异步投递一条已解码报文（不阻塞调用方，即 EMQX 钩子）。
ingest(Pid, Item) ->
    gen_server:cast(Pid, {ingest, Item}).

%%%===================================================================
%%% gen_server
%%%===================================================================

init({Index, Cfg}) ->
    process_flag(trap_exit, true),
    {ok, #st{index = Index, cfg = Cfg}}.

handle_call({stats, Index}, _From, St) ->
    {reply, #{worker => Index, connected => is_connected(St)}, St};
handle_call(_Req, _From, St) ->
    {reply, ok, St}.

handle_cast({ingest, Item}, St) ->
    {noreply, safe_ingest(Item, St)};
handle_cast(_Msg, St) ->
    {noreply, St}.

handle_info(_Info, St) ->
    {noreply, St}.

terminate(_Reason, #st{conn = Conn}) ->
    close(Conn),
    ok.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

%%%===================================================================
%%% 入库流程
%%%===================================================================

safe_ingest(Item, St) ->
    case ensure_conn(St) of
        {error, Reason, St1} ->
            count(connect_failed),
            logger:error("[emqx_pg_ingest] PG 连接失败: ~p", [Reason]),
            St1;
        {ok, St1} ->
            try
                ingest_item(Item, St1)
            catch
                Class:Reason:Stack ->
                    count(write_failed),
                    logger:error(
                        "[emqx_pg_ingest] 入库失败 file_name=~ts: ~p:~p~n~p",
                        [file_name(Item), Class, Reason, Stack]
                    ),
                    close(St1#st.conn),
                    St1#st{conn = undefined}
            end
    end.

ingest_item(Item, #st{conn = Conn, cfg = Cfg, index = Index} = St) ->
    Decoded = maps:get(decoded, Item),
    RequestedProfile = maps:get(profile, Item, <<"racebox">>),
    ProfileName = select_profile(RequestedProfile, Decoded),
    Profile = emqx_pg_ingest_config:profile(Cfg, ProfileName),
    File = maps:get(file_name, Decoded),
    case map_size(Profile) of
        0 ->
            count(profile_missing),
            logger:error("[emqx_pg_ingest] 未找到 profile=~ts 的 SQL 配置，跳过", [ProfileName]),
            St;
        _ ->
            case completed(Profile, Decoded, Conn) orelse duplicated(Profile, Decoded, Conn) of
                true ->
                    count(duplicate);
                false ->
                    ImpStamp = emqx_pg_ingest_codec:uuid_v4(),
                    N = run_tx(Conn, fun(C) -> write_all(C, Profile, Decoded, ImpStamp, File) end),
                    count(written),
                    logger:info(
                        "[emqx_pg_ingest] 入库完成 worker=~p file_name=~ts records=~p",
                        [Index, File, N]
                    )
            end,
            St
    end.

%% RBX1/simlive_rb 没有 RBX2 才提供的 session_key/batch_key/record_index。
%% 它们与 RBX2 共用 auto 解码主题时，必须自动回退到按 file_name
%% 去重的旧 profile，否则会在组装 complete_dedup 参数时缺 session_key。
-spec select_profile(binary(), map()) -> binary().
select_profile(<<"racebox">>, #{kind := rbx1}) ->
    <<"racebox_legacy">>;
select_profile(<<"racebox">>, #{kind := simlive_rb}) ->
    <<"racebox_legacy">>;
select_profile(Profile, _Decoded) ->
    Profile.

run_tx(Conn, Fun) ->
    _ = code:ensure_loaded(epgsql),
    case erlang:function_exported(epgsql, with_transaction, 2) of
        true ->
            case epgsql:with_transaction(Conn, Fun) of
                {ok, Result} -> Result;
                {rollback, Reason} -> erlang:error({transaction_rollback, Reason})
            end;
        false ->
            {ok, [], []} = epgsql:equery(Conn, "BEGIN", []),
            try
                Result = Fun(Conn),
                {ok, [], []} = epgsql:equery(Conn, "COMMIT", []),
                Result
            catch
                Class:Reason ->
                    _ = epgsql:equery(Conn, "ROLLBACK", []),
                    erlang:raise(Class, Reason, [])
            end
    end.

write_all(C, Profile, Decoded, ImpStamp, File) ->
    Records = maps:get(records, Decoded),
    Base = maps:merge(Decoded, #{
        imp_stamp => ImpStamp,
        file_name => File,
        device_id => maps:get(device_id, Decoded, undefined),
        duration => maps:get(duration, Decoded, 0),
        record_count => length(Records)
    }),
    _ = maybe_exec(C, <<"imp">>, Profile, Base),
    lists:foreach(
        fun(Rec) -> exec_fields(C, <<"record">>, Profile, maps:merge(Rec, Base)) end,
        Records
    ),
    _ = maybe_exec(C, <<"batch">>, Profile, Base),
    _ = maybe_exec(C, <<"finalize">>, Profile, Base),
    length(Records).

completed(Profile, Decoded, C) ->
    query_exists(Profile, <<"complete_dedup">>, <<"complete_dedup_fields">>, Decoded, C).

duplicated(Profile, Decoded, C) ->
    query_exists(Profile, <<"dedup">>, <<"dedup_fields">>, Decoded, C).

query_exists(Profile, SqlKey, FieldsKey, Values, C) ->
    case sql_of(Profile, SqlKey) of
        undefined ->
            false;
        SQL ->
            Fields = maps:get(FieldsKey, Profile, [<<"file_name">>]),
            Params = params(Fields, Values),
            case epgsql:equery(C, SQL, Params) of
                {ok, _Cols, []} ->
                    false;
                {ok, _Cols, _Rows} ->
                    true;
                {error, Reason} ->
                    %% 去重查询失败不阻断写入（表上的唯一约束仍是最终防线）
                    logger:warning("[emqx_pg_ingest] 去重查询失败 key=~ts: ~p", [SqlKey, Reason]),
                    false
            end
    end.

maybe_exec(C, SqlKey, Profile, Values) ->
    case sql_of(Profile, SqlKey) of
        undefined -> skipped;
        _ -> exec_fields(C, SqlKey, Profile, Values)
    end.

exec_fields(C, SqlKey, Profile, Values) ->
    SQL = normalize_sql(SqlKey, sql_of(Profile, SqlKey)),
    Params = params(fields_of(Profile, SqlKey), Values),
    case exec(C, SqlKey, SQL, Params) of
        {ok, _Cols, _Rows} -> ok;
        {ok, _Count} -> ok;
        {error, Reason} -> erlang:error({sql_failed, SqlKey, Reason});
        Other -> erlang:error({sql_unexpected, SqlKey, Other})
    end.

%% 兼容已部署的旧 JSON：早期 racebox_legacy SQL 指定了
%% ON CONFLICT (file_name)，但历史库不适合对整列建唯一约束。
%% 去掉仲裁目标后，PostgreSQL 仍会使用 RBX1 部分唯一索引阻止重投。
-spec normalize_sql(binary(), binary()) -> binary().
normalize_sql(<<"imp">>, SQL) when is_binary(SQL) ->
    re:replace(
        SQL,
        <<"ON\\s+CONFLICT\\s*\\(\\s*file_name\\s*\\)">>,
        <<"ON CONFLICT">>,
        [global, caseless, {return, binary}]
    );
normalize_sql(_SqlKey, SQL) ->
    SQL.

sql_of(Profile, <<"imp">>) -> maps:get(<<"imp">>, Profile, undefined);
sql_of(Profile, <<"record">>) -> maps:get(<<"record">>, Profile, undefined);
sql_of(Profile, <<"dedup">>) -> maps:get(<<"dedup">>, Profile, undefined);
sql_of(Profile, <<"complete_dedup">>) -> maps:get(<<"complete_dedup">>, Profile, undefined);
sql_of(Profile, <<"batch">>) -> maps:get(<<"batch">>, Profile, undefined);
sql_of(Profile, <<"finalize">>) -> maps:get(<<"finalize">>, Profile, undefined);
sql_of(_Profile, _Key) -> undefined.

fields_of(Profile, <<"imp">>) -> maps:get(<<"imp_fields">>, Profile, []);
fields_of(Profile, <<"record">>) -> maps:get(<<"record_fields">>, Profile, []);
fields_of(Profile, <<"batch">>) -> maps:get(<<"batch_fields">>, Profile, []);
fields_of(Profile, <<"finalize">>) -> maps:get(<<"finalize_fields">>, Profile, []);
fields_of(_Profile, _Key) -> [].

params(Fields, Values) ->
    [
        case find_value(Field, Values) of
            {ok, V} -> V;
            error -> erlang:error({missing_field, Field, lists:sort(maps:keys(Values))})
        end
     || Field <- Fields
    ].

find_value(Field, Values) ->
    case maps:find(Field, Values) of
        {ok, _} = Found -> Found;
        error when is_binary(Field) ->
            try maps:find(binary_to_existing_atom(Field, utf8), Values)
            catch error:badarg -> error
            end;
        error -> error
    end.

%% prepared_query 可缓存同名语句；不可用时退回 equery
exec(C, Name, SQL, Params) ->
    _ = code:ensure_loaded(epgsql),
    case erlang:function_exported(epgsql, prepared_query, 4) of
        true -> epgsql:prepared_query(C, Name, SQL, Params);
        false -> epgsql:equery(C, SQL, Params)
    end.

%%%===================================================================
%%% 连接管理
%%%===================================================================

ensure_conn(#st{conn = Conn, cfg = Cfg} = St) ->
    case is_alive(Conn) of
        true ->
            {ok, St};
        false ->
            close(Conn),
            case connect(Cfg) of
                {ok, NewConn} -> {ok, St#st{conn = NewConn}};
                {error, Reason} -> {error, Reason, St#st{conn = undefined}}
            end
    end.

is_connected(#st{conn = Conn}) -> is_alive(Conn).

is_alive(undefined) -> false;
is_alive(Conn) ->
    try
        epgsql:is_alive(Conn)
    of
        true -> true;
        _ -> false
    catch
        _:_ -> false
    end.

close(undefined) -> ok;
close(Conn) ->
    _ = try
            epgsql:close(Conn)
        catch
            _:_ -> ok
        end,
    ok.

connect(Cfg) ->
    Opts = #{
        host => to_list(Cfg, [<<"pg">>, <<"host">>], <<"127.0.0.1">>),
        port => emqx_pg_ingest_config:get_int(Cfg, [<<"pg">>, <<"port">>], 5432),
        username => to_list(Cfg, [<<"pg">>, <<"username">>], <<"postgres">>),
        password => to_list(Cfg, [<<"pg">>, <<"password">>], <<>>),
        database => to_list(Cfg, [<<"pg">>, <<"database">>], <<"postgres">>),
        timeout => emqx_pg_ingest_config:get_int(Cfg, [<<"pg">>, <<"timeout_ms">>], 5000)
    },
    case epgsql:connect(Opts) of
        {ok, Conn} ->
            logger:notice("[emqx_pg_ingest] PG 已连接 ~s:~p/~s", [
                maps:get(host, Opts), maps:get(port, Opts), maps:get(database, Opts)
            ]),
            {ok, Conn};
        {error, Reason} ->
            {error, Reason}
    end.

to_list(Cfg, Path, Default) ->
    binary_to_list(emqx_pg_ingest_config:get_str(Cfg, Path, Default)).

file_name(Item) ->
    maps:get(file_name, maps:get(decoded, Item, #{}), <<"?">>).

count(Key) ->
    _ = ets:update_counter(?STATS, Key, 1, {Key, 0}),
    ok.

