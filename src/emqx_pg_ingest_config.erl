%%%-------------------------------------------------------------------
%%% @doc 插件配置加载（JSON 或 Erlang terms；支持 env 指定路径）
%%%
%%% 查找顺序（取第一个存在者）：
%%%   1) 环境变量 EMQX_PG_INGEST_CONF 指定的文件
%%%   2) $EMQX_ETC_DIR/emqx_pg_ingest.json（EMQX 默认 etc 目录）
%%%   3) 插件 priv 目录下的 emqx_pg_ingest.json
%%% 文件名以 .terms 结尾时按 Erlang terms（file:consult）解析，便于不依赖 JSON 库。
%%% @end
%%%-------------------------------------------------------------------
-module(emqx_pg_ingest_config).

-export([load/0, resolve_path/0, get_int/3, get_str/3, get_list/3, profile/2,
         match_topic/2, topic_segment/2, kv/2]).

-define(DEFAULT_FILE, "emqx_pg_ingest.json").

%% @doc 读取配置（失败时返回最小可用配置，避免插件启动崩溃）。
-spec load() -> map().
load() ->
    case resolve_path() of
        {error, Reason} ->
            logger:error("[emqx_pg_ingest] 未找到配置文件: ~p", [Reason]),
            #{};
        Path ->
            case read_file(Path) of
                {ok, Cfg} ->
                    logger:info("[emqx_pg_ingest] 配置已加载: ~ts", [Path]),
                    Cfg;
                {error, Reason} ->
                    logger:error("[emqx_pg_ingest] 配置解析失败 ~ts: ~p", [Path, Reason]),
                    #{}
            end
    end.

%% @doc 解析配置文件路径。
-spec resolve_path() -> file:filename_all() | {error, enoent}.
resolve_path() ->
    Env = os:getenv("EMQX_PG_INGEST_CONF"),
    Etc = filename:join(
        case os:getenv("EMQX_ETC_DIR") of
            false -> "/opt/emqx/etc";
            Dir -> Dir
        end,
        ?DEFAULT_FILE
    ),
    Priv =
        try filename:join(code:priv_dir(emqx_pg_ingest), ?DEFAULT_FILE) catch
            _:_ -> undefined
        end,
    Candidates = lists:filtermap(fun name/1, [Env, Etc, Priv, ?DEFAULT_FILE]),
    case [P || P <- Candidates, filelib:is_file(P)] of
        [Path | _] -> Path;
        [] -> {error, enoent}
    end.

name(false) -> false;
name(undefined) -> false;
name(P) -> {true, P}.

read_file(Path) ->
    case filename:extension(Path) of
        ".terms" ->
            file:consult(Path);
        _ ->
            case file:read_file(Path) of
                {ok, Bin} -> decode_json(Bin);
                {error, Reason} -> {error, Reason}
            end
    end.

decode_json(Bin) ->
    case code:ensure_loaded(emqx_utils_json) of
        {module, _} ->
            try
                {ok, emqx_utils_json:decode(Bin)}
            catch
                _:Reason -> {error, Reason}
            end;
        _ ->
            case code:ensure_loaded(jsx) of
                {module, _} -> {ok, jsx:decode(Bin, [return_maps])};
                _ -> {error, no_json_library_use_dot_terms_file}
            end
    end.

%% @doc 读取整数配置（支持二进制/整数，缺失或非法时用默认值）。
-spec get_int(map(), [binary()], integer()) -> integer().
get_int(Cfg, Path, Default) ->
    case kv(Cfg, Path) of
        undefined -> Default;
        V when is_integer(V) -> V;
        V when is_binary(V) -> binary_to_integer(V);
        V when is_list(V) -> list_to_integer(V);
        _ -> Default
    end.

%% @doc 读取字符串配置（统一返回 binary）。
-spec get_str(map(), [binary()], binary()) -> binary().
get_str(Cfg, Path, Default) ->
    case kv(Cfg, Path) of
        undefined -> Default;
        V when is_binary(V) -> V;
        V when is_list(V) -> unicode:characters_to_binary(V);
        V when is_integer(V) -> integer_to_binary(V);
        _ -> Default
    end.

%% @doc 读取数组配置；元素为 binary 时直接返回。
-spec get_list(map(), [binary()], list()) -> list().
get_list(Cfg, Path, Default) ->
    case kv(Cfg, Path) of
        L when is_list(L) -> L;
        _ -> Default
    end.

%% @doc 取某 SQL profile（racebox / bms 等）。
-spec profile(map(), binary()) -> map().
profile(Cfg, Name) ->
    case kv(Cfg, [<<"profiles">>, Name]) of
        M when is_map(M) -> M;
        _ -> #{}
    end.

%% @doc 按顺序找第一条匹配的主题配置；无匹配返回 undefined。
-spec match_topic(map(), binary()) -> map() | undefined.
match_topic(Cfg, Topic) ->
    Topics = get_list(Cfg, [<<"topics">>], []),
    case
        [
            T
         || T <- Topics,
            is_map(T),
            topic_match(Topic, get_str(T, [<<"topic">>], <<>>))
        ]
    of
        [T | _] -> T;
        [] -> undefined
    end.

%% @doc 主题通配匹配（自实现 `+`/`#`，不依赖 emqx_topic，便于本地单测）。
topic_match(_Topic, <<>>) -> false;
topic_match(Topic, Filter) ->
    match_segments(
        string:split(Topic, <<"/">>, all),
        string:split(Filter, <<"/">>, all)
    ).

match_segments([], []) ->
    true;
match_segments(_Rest, [<<"#">> | _]) ->
    true;
match_segments([_T | Ts], [<<"+">> | Fs]) ->
    match_segments(Ts, Fs);
match_segments([T | Ts], [F | Fs]) when T =:= F ->
    match_segments(Ts, Fs);
match_segments(_Topics, _Filter) ->
    false.

%% @doc 取主题中的第 N 段（1 为起始）；越界返回 undefined。
-spec topic_segment(binary(), pos_integer()) -> binary() | undefined.
topic_segment(Topic, N) ->
    case string:split(Topic, <<"/">>, all) of
        Segs when length(Segs) >= N -> lists:nth(N, Segs);
        _ -> undefined
    end.

%% @doc 按路径取嵌套值（键为 binary）。
-spec kv(map(), [binary()]) -> term().
kv(Cfg, Path) -> kv(Cfg, Path, fun(M, K) -> maps:get(K, M, undefined) end).

kv(Value, [], _Get) -> Value;
kv(Value, [_ | _], _Get) when not is_map(Value) -> undefined;
kv(Map, [K | Rest], Get) -> kv(Get(Map, K), Rest, Get).
