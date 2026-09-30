%%%-------------------------------------------------------------------
%%% @doc EMQX 消费插件主模块：注册 `message.publish` 钩子 → 按主题配置解码 → 异步写库
%%%
%%% 设计要点：
%%%   * 钩子内只做纯解码与投递（快，不阻塞 broker），DB 写入交给 worker 异步执行；
%%%   * RBX2 同一 session_key（旧协议同一 file_name）固定散列到同一 worker，保证批次顺序与幂等；
%%%   * 未匹配的主题/解码失败只计数并记日志，绝不影响消息正常转发。
%%% @end
%%%-------------------------------------------------------------------
-module(emqx_pg_ingest).

-export([load/0, unload/0, reload/0, on_message_publish/1, stats/0, hook_spec/0]).

-define(CFG_KEY, {?MODULE, cfg}).
-define(STATS, emqx_pg_ingest_stats).

%% @doc 插件启动：加载配置、初始化计数器、注册钩子。
-spec load() -> ok.
load() ->
    Cfg = emqx_pg_ingest_config:load(),
    persistent_term:put(?CFG_KEY, Cfg),
    init_stats(),
    Topics = emqx_pg_ingest_config:get_list(Cfg, [<<"topics">>], []),
    case Topics of
        [] ->
            logger:warning("[emqx_pg_ingest] 未配置 topics，插件已启动但不消费任何数据");
        _ ->
            logger:notice("[emqx_pg_ingest] 消费主题: ~p", [
                [maps:get(<<"topic">>, T, <<>>) || T <- Topics, is_map(T)]
            ])
    end,
    {HookPoint, Callback, Priority} = hook_spec(),
    emqx_hooks:add(HookPoint, Callback, Priority).

%% EMQX 5.9 的 emqx_hooks:add/3 第三个参数必须是整数优先级，不能传插件名原子。
-spec hook_spec() -> {atom(), {module(), atom(), list()}, integer()}.
hook_spec() ->
    {'message.publish', {?MODULE, on_message_publish, []}, 0}.

%% @doc 插件停止：移除钩子。
-spec unload() -> ok.
unload() ->
    _ = try
            emqx_hooks:del('message.publish', {?MODULE, on_message_publish})
        catch
            _:_ -> ok
        end,
    ok.

%% @doc 热重载配置（修改配置文件后调用；EMQX 控制台重启插件亦可）。
-spec reload() -> ok.
reload() ->
    Cfg = emqx_pg_ingest_config:load(),
    persistent_term:put(?CFG_KEY, Cfg),
    logger:notice("[emqx_pg_ingest] 配置已热重载"),
    ok.

%% @doc 运行统计（排查用）：`emqx_pg_ingest:stats()`。
-spec stats() -> map().
stats() ->
    case ets:info(?STATS) of
        undefined -> #{};
        _ -> maps:from_list([{K, V} || {K, V} <- ets:tab2list(?STATS)])
    end.

%% @doc `message.publish` 钩子：必须原样返回消息，保证消息继续正常流转。
-spec on_message_publish(emqx_types:message()) -> emqx_types:message().
on_message_publish(Msg) ->
    try
        do_publish(Msg)
    catch
        Class:Reason:Stack ->
            count(dropped),
            logger:error("[emqx_pg_ingest] 处理异常: ~p:~p~n~p", [Class, Reason, Stack])
    end,
    Msg.

do_publish(Msg) ->
    Topic = emqx_message:topic(Msg),
    case payload_bin(emqx_message:payload(Msg)) of
        skip ->
            count(skip_no_binary_payload),
            ok;
        {ok, Bin} ->
            dispatch(Topic, Bin)
    end.

payload_bin(Bin) when is_binary(Bin) ->
    {ok, Bin};
payload_bin({binary, Bin}) when is_binary(Bin) ->
    {ok, Bin};
payload_bin(_) ->
    skip.

dispatch(Topic, Bin) ->
    Cfg = persistent_term:get(?CFG_KEY, #{}),
    case emqx_pg_ingest_config:match_topic(Cfg, Topic) of
        undefined ->
            count(skip_unmatched_topic),
            ok;
        TopicCfg ->
            CodecBin = emqx_pg_ingest_config:get_str(TopicCfg, [<<"codec">>], <<"auto">>),
            Codec = to_codec(CodecBin),
            case emqx_pg_ingest_codec:decode(Codec, Bin) of
                {ok, Decoded} ->
                    count(accepted),
                    Item = #{
                        decoded => enrich(Decoded, Topic, TopicCfg),
                        profile => emqx_pg_ingest_config:get_str(
                            TopicCfg, [<<"profile">>], <<"racebox">>
                        ),
                        topic => Topic
                    },
                    submit(Item);
                {error, Reason} ->
                    count(decode_failed),
                    logger:warning(
                        "[emqx_pg_ingest] 解码失败 topic=~ts bytes=~p reason=~p",
                        [Topic, byte_size(Bin), Reason]
                    )
            end
    end.

%% device_id 缺失时按配置从主题取（如 simlive/+/data 的第 2 段）
enrich(Decoded, Topic, TopicCfg) ->
    case maps:get(device_id, Decoded, undefined) of
        undefined ->
            case emqx_pg_ingest_config:get_int(TopicCfg, [<<"device_from_topic">>], 0) of
                0 -> Decoded;
                N -> Decoded#{device_id => emqx_pg_ingest_config:topic_segment(Topic, N)}
            end;
        _ ->
            Decoded
    end.

submit(Item) ->
    Decoded = maps:get(decoded, Item),
    File = maps:get(file_name, Decoded),
    %% RBX2 的 open/final 文件名会变化；稳定 session_key 必须固定到同一 worker，
    %% 才能保证同一轨迹段的批次和段尾事务严格有序。
    RouteKey = maps:get(session_key, Decoded, File),
    case worker(RouteKey) of
        undefined ->
            count(worker_down),
            logger:error("[emqx_pg_ingest] 无可用写入 worker，丢弃 file_name=~ts", [File]),
            ok;
        Pid ->
            emqx_pg_ingest_pg:ingest(Pid, Item)
    end.

worker(File) ->
    Cfg = persistent_term:get(?CFG_KEY, #{}),
    PoolSize = emqx_pg_ingest_config:get_int(Cfg, [<<"pg">>, <<"pool_size">>], 2),
    Index = erlang:phash2(File, max(1, PoolSize)) + 1,
    whereis(emqx_pg_ingest_pg:worker_name(Index)).

to_codec(<<"rbx1">>) -> rbx1;
to_codec(<<"rbx2">>) -> rbx2;
to_codec(<<"simlive_rb">>) -> simlive_rb;
to_codec(<<"simlive_bm">>) -> simlive_bm;
to_codec(_) -> auto.

init_stats() ->
    case ets:info(?STATS) of
        undefined ->
            _ = ets:new(?STATS, [named_table, public, set, {write_concurrency, true}]),
            ok;
        _ ->
            ok
    end.

count(Key) ->
    _ = ets:update_counter(?STATS, Key, 1, {Key, 0}),
    ok.
