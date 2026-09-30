%%%-------------------------------------------------------------------
%%% @doc 解码器 eunit 测试：用合成报文验证三种协议解出的字段值 + CRC 校验逻辑。
%%% 运行：
%%%   erlc -o ebin src/emqx_pg_ingest_codec.erl test/emqx_pg_ingest_codec_tests.erl
%%%   erl -noshell -pa ebin -eval "eunit:test(emqx_pg_ingest_codec_tests, [verbose]), init:stop()."
%%% @end
%%%-------------------------------------------------------------------
-module(emqx_pg_ingest_codec_tests).

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% CRC / 工具
%%%===================================================================

crc16_ccitt_vector_test() ->
    ?assertEqual(16#29B1, emqx_pg_ingest_codec:crc16_ccitt(<<"123456789">>)).

crc32_matches_erlang_test() ->
    Bin = <<"racebox">>,
    ?assertEqual(erlang:crc32(Bin), emqx_pg_ingest_codec:crc32(Bin)).

uuid_v4_format_test() ->
    Uuid = emqx_pg_ingest_codec:uuid_v4(),
    ?assertMatch(
        {match, _},
        re:run(Uuid, "^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
    ).

magic_test() ->
    ?assertEqual(rbx1, emqx_pg_ingest_codec:magic(<<"RBX1", 0, 0, 0>>)),
    ?assertEqual(rbx2, emqx_pg_ingest_codec:magic(<<"RBX2", 0, 0, 0>>)),
    ?assertEqual(simlive_rb, emqx_pg_ingest_codec:magic(<<"RB", 1, 3, 0>>)),
    ?assertEqual(simlive_bm, emqx_pg_ingest_codec:magic(<<"BM", 1, 1, 0>>)),
    ?assertEqual(unknown, emqx_pg_ingest_codec:magic(<<"XX", 0>>)).

hook_registration_shape_test() ->
    {'message.publish', {emqx_pg_ingest, on_message_publish, []}, Priority} =
        emqx_pg_ingest:hook_spec(),
    ?assert(is_integer(Priority)).

protocol_profile_selection_test() ->
    ?assertEqual(
        <<"racebox_legacy">>,
        emqx_pg_ingest_pg:select_profile(<<"racebox">>, #{kind => rbx1})
    ),
    ?assertEqual(
        <<"racebox_legacy">>,
        emqx_pg_ingest_pg:select_profile(<<"racebox">>, #{kind => simlive_rb})
    ),
    ?assertEqual(
        <<"racebox">>,
        emqx_pg_ingest_pg:select_profile(<<"racebox">>, #{kind => rbx2})
    ),
    ?assertEqual(
        <<"custom">>,
        emqx_pg_ingest_pg:select_profile(<<"custom">>, #{kind => rbx1})
    ).

legacy_conflict_sql_normalization_test() ->
    Old = <<"INSERT INTO imp_racebox(file_name) VALUES ($1) ON CONFLICT (file_name) DO NOTHING">>,
    New = emqx_pg_ingest_pg:normalize_sql(<<"imp">>, Old),
    ?assertEqual(
        <<"INSERT INTO imp_racebox(file_name) VALUES ($1) ON CONFLICT DO NOTHING">>,
        New
    ),
    ?assertEqual(Old, emqx_pg_ingest_pg:normalize_sql(<<"record">>, Old)).

transaction_result_contract_test() ->
    ?assertEqual(720, emqx_pg_ingest_pg:transaction_result(720)),
    ?assertEqual({ok, custom_reply}, emqx_pg_ingest_pg:transaction_result({ok, custom_reply})),
    ?assertError(
        {transaction_rollback, database_error},
        emqx_pg_ingest_pg:transaction_result({rollback, database_error})
    ).

decode_unknown_codec_test() ->
    ?assertMatch({error, {unsupported_codec, foo}}, emqx_pg_ingest_codec:decode(foo, <<1, 2>>)),
    ?assertMatch({error, unknown_magic}, emqx_pg_ingest_codec:decode(auto, <<"nope">>)).

%%%===================================================================
%%% 主题通配（+ / #）
%%%===================================================================

topic_match_test() ->
    Cfg = #{
        <<"topics">> => [
            #{<<"topic">> => <<"deskwong/racebox/data">>},
            #{<<"topic">> => <<"simlive/+/data">>},
            #{<<"topic">> => <<"sensors/#">>}
        ]
    },
    ?assertEqual(
        #{<<"topic">> => <<"simlive/+/data">>},
        emqx_pg_ingest_config:match_topic(Cfg, <<"simlive/esp32-tracker-001/data">>)
    ),
    ?assertEqual(
        #{<<"topic">> => <<"deskwong/racebox/data">>},
        emqx_pg_ingest_config:match_topic(Cfg, <<"deskwong/racebox/data">>)
    ),
    ?assertEqual(
        #{<<"topic">> => <<"sensors/#">>},
        emqx_pg_ingest_config:match_topic(Cfg, <<"sensors/a/b/c">>)
    ),
    ?assertEqual(undefined, emqx_pg_ingest_config:match_topic(Cfg, <<"other/x">>)),

    %% 主题第 N 段提取（device_id 用）
    ?assertEqual(
        <<"esp32-tracker-001">>,
        emqx_pg_ingest_config:topic_segment(<<"simlive/esp32-tracker-001/data">>, 2)
    ),
    ?assertEqual(undefined, emqx_pg_ingest_config:topic_segment(<<"a/b">>, 5)).

%%%===================================================================
%%% RBX1（deskwong）
%%%===================================================================

rbx1_decode_test() ->
    ImportId = <<1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16>>,
    Device = pad48(<<"deskwong-01">>),
    Recs = [
        rbx1_record(1000, 121345678, 312345678, 10000),
        rbx1_record(2000, 121345679, 312345679, 20000)
    ],
    Payload = rbx1_payload(Recs, ImportId, Device, 0, 2),
    {ok, D} = emqx_pg_ingest_codec:decode(auto, Payload),
    ?assertEqual(rbx1, maps:get(kind, D)),
    ?assertEqual(2, maps:get(record_count, D)),
    ?assertEqual(2, maps:get(total, D)),
    ?assertEqual(<<"deskwong-01">>, maps:get(device_id, D)),
    ?assertEqual(<<"rbx1_01020304-0506-0708-090a-0b0c0d0e0f10_0">>, maps:get(file_name, D)),
    [R1, R2] = maps:get(records, D),
    ?assertEqual(1000, maps:get(itow, R1)),
    ?assertEqual(2026, maps:get(year, R1)),
    ?assertEqual(9, maps:get(month, R1)),
    ?assertEqual(26, maps:get(day, R1)),
    ?assertEqual(3, maps:get(fix_status, R1)),
    ?assertEqual(12, maps:get(numberof_svs, R1)),
    ?assertEqual(12.1345678, maps:get(longitude, R1)),
    ?assertEqual(31.2345678, maps:get(latitude, R1)),
    %% speed：rbx1 原始 10000（mm/s = 10 m/s）→ 统一换算 36.0 km/h
    ?assertEqual(36.0, maps:get(speed, R1)),
    ?assertEqual(90.0, maps:get(heading, R1)),
    ?assertEqual(0.01, maps:get(gforce_x, R1)),
    ?assertEqual(3.0, maps:get(rotation_rate_z, R1)),
    ?assertEqual(2000, maps:get(itow, R2)),
    %% 同 import_id、不同 offset → file_name 不同（多消息批次各自幂等）
    {ok, D2} = emqx_pg_ingest_codec:decode(rbx1, rbx1_payload(Recs, ImportId, Device, 2, 4)),
    ?assertNotEqual(maps:get(file_name, D), maps:get(file_name, D2)),
    %% RBX1 bit0 是最终同步批次，旧插件不得拒绝。
    {ok, _} = emqx_pg_ingest_codec:decode(rbx1, rbx1_payload_flags(Recs, ImportId, Device, 0, 2, 1)).

rbx2_decode_and_stable_session_test() ->
    SyncId = <<16#10, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16>>,
    Device = pad48(<<"RaceBox Mini S 2254300997">>),
    Recs = [
        rbx1_record(1000, 121345678, 312345678, 10000),
        rbx1_record(2000, 121345679, 312345679, 20000)
    ],
    Payload = rbx2_payload(Recs, SyncId, Device, 750, 196559, 7, 0, 2,
                           20260924221855, 20260924221925, 1000, 0, 3),
    {ok, D} = emqx_pg_ingest_codec:decode(auto, Payload),
    ?assertEqual(rbx2, maps:get(kind, D)),
    ?assertEqual(<<"20260924221855_20260924221925">>, maps:get(file_name, D)),
    ?assertEqual(<<"rbx2_RaceBox Mini S 2254300997_20260924221855_1000_0">>, maps:get(session_key, D)),
    ?assertEqual(<<"rbx2_RaceBox Mini S 2254300997_20260924221855_1000_0_0">>, maps:get(batch_key, D)),
    ?assertEqual(true, maps:get(sync_complete, D)),
    ?assertEqual(true, maps:get(session_complete, D)),
    ?assertEqual(2, maps:get(session_total, D)),
    [R1, R2] = maps:get(records, D),
    ?assertEqual(0, maps:get(record_index, R1)),
    ?assertEqual(1, maps:get(record_index, R2)),
    %% sync_id 改变不应改变跨同步稳定的 session_key/file_name。
    OtherSync = <<16#20, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16>>,
    {ok, D2} = emqx_pg_ingest_codec:decode(rbx2,
        rbx2_payload(Recs, OtherSync, Device, 750, 196559, 7, 0, 2,
                     20260924221855, 20260924221925, 1000, 0, 3)),
    ?assertEqual(maps:get(session_key, D), maps:get(session_key, D2)),
    ?assertEqual(maps:get(file_name, D), maps:get(file_name, D2)),
    {ok, Open} = emqx_pg_ingest_codec:decode(rbx2,
        rbx2_payload(Recs, SyncId, Device, 0, 196559, 7, 0, 0,
                     20260924221855, 0, 1000, 0, 0)),
    ?assertEqual(<<"20260924221855_open_1000_0">>, maps:get(file_name, Open)),
    ?assertEqual(false, maps:get(session_complete, Open)).

rbx1_length_error_test() ->
    Payload = rbx1_payload([rbx1_record(1, 1, 1, 1)], <<0:128>>, pad48(<<"dev">>), 0, 1),
    Broken = binary:part(Payload, 0, byte_size(Payload) - 1),
    ?assertMatch({error, {bad_length, _}}, emqx_pg_ingest_codec:decode(rbx1, Broken)).

rbx1_crc_error_test() ->
    Payload = rbx1_payload([rbx1_record(1, 1, 1, 1)], <<0:128>>, pad48(<<"dev">>), 0, 1),
    ?assertMatch(
        {error, {crc_mismatch, _}},
        emqx_pg_ingest_codec:decode(rbx1, corrupt_last_byte(Payload))
    ).

%%%===================================================================
%%% simlive RB（定位批量）
%%%===================================================================

simlive_rb_decode_test() ->
    Name = <<"20260926120000_20260926123000_p001">>,
    Recs = [simlive_rb_record(5000, 1134567890, 223456789, 3000)],
    Payload = simlive_rb_payload(Name, Recs, 1800),
    {ok, D} = emqx_pg_ingest_codec:decode(auto, Payload),
    ?assertEqual(simlive_rb, maps:get(kind, D)),
    ?assertEqual(Name, maps:get(file_name, D)),
    ?assertEqual(1800, maps:get(duration, D)),
    ?assertEqual(1, maps:get(record_count, D)),
    [R] = maps:get(records, D),
    ?assertEqual(5000, maps:get(itow, R)),
    ?assertEqual(113.4567890, maps:get(longitude, R)),
    ?assertEqual(22.3456789, maps:get(latitude, R)),
    ?assertEqual(3, maps:get(fix_status, R)),
    %% simlive speed 已是 km/h：原始 3000 → 3000/100*60 = 1800.0
    ?assertEqual(1800.0, maps:get(speed, R)),
    ?assertEqual(77, maps:get(battery, R)).

simlive_rb_bad_crc_test() ->
    Payload = simlive_rb_payload(<<"abc">>, [simlive_rb_record(1, 1, 1, 1)], 0),
    ?assertMatch(
        {error, {crc_mismatch, _}},
        emqx_pg_ingest_codec:decode(simlive_rb, corrupt_last_byte(Payload))
    ),
    %% 截断（少 2 字节 CRC）→ 明确报结构问题
    Truncated = binary:part(Payload, 0, byte_size(Payload) - 2),
    ?assertMatch({error, {bad_simlive_rb_payload, _}}, emqx_pg_ingest_codec:decode(simlive_rb, Truncated)).

%%%===================================================================
%%% simlive BM（BMS 快照）
%%%===================================================================

simlive_bm_decode_test() ->
    Payload = simlive_bm_payload(<<"BMS_36720000">>, 6790, 890, [3564, 3565, 3584], [31, 33], 35, 3),
    {ok, D} = emqx_pg_ingest_codec:decode(auto, Payload),
    ?assertEqual(simlive_bm, maps:get(kind, D)),
    ?assertEqual(<<"BMS_36720000">>, maps:get(file_name, D)),
    [S] = maps:get(records, D),
    ?assertEqual(67.90, maps:get(total_voltage, S)),
    ?assertEqual(8.90, maps:get(current, S)),
    ?assertEqual(19, maps:get(soc, S)),
    ?assertEqual(100, maps:get(soh, S)),
    ?assertEqual(3.584, maps:get(cell_max_v, S)),
    ?assertEqual(3.564, maps:get(cell_min_v, S)),
    ?assertEqual(3, maps:get(cell_count, S)),
    ?assertEqual([3.564, 3.565, 3.584], maps:get(cell_voltages, S)),
    ?assertEqual([31, 33], maps:get(temps_c, S)),
    ?assertEqual(35, maps:get(mos_temp_c, S)),
    ?assertEqual(true, maps:get(charging, S)),
    ?assertEqual(true, maps:get(chg_mos_on, S)),
    ?assertEqual(false, maps:get(dsg_mos_on, S)),
    ?assertEqual(false, maps:get(balancing, S)),
    ?assertEqual(0, maps:get(alarm_code, S)).

%%%===================================================================
%%% 报文构造工具（与协议文档字节布局一一对应）
%%%===================================================================

rbx1_record(Itow, LonMicro, LatMicro, SpeedMmS) ->
    <<Itow:32/little, 2026:16/little, 9:8, 26:8, 12:8, 0:8, 0:8, 0:8, 0:32/little,
        0:32/little-signed, 3:8, 0:16, 12:8, LonMicro:32/little-signed, LatMicro:32/little-signed,
        0:32/little-signed, 0:32/little-signed, 0:32/little, 0:32/little, SpeedMmS:32/little-signed,
        9000000:32/little-signed, 0:32/little, 0:32/little, 100:16/little, 0:16,
        10:16/little-signed, 20:16/little-signed, 30:16/little-signed, 100:16/little-signed,
        200:16/little-signed, 300:16/little-signed>>.

rbx1_payload(Recs, ImportId, Device48, Offset, Total) ->
    rbx1_payload_flags(Recs, ImportId, Device48, Offset, Total, 0).

rbx1_payload_flags(Recs, ImportId, Device48, Offset, Total, Flags) ->
    RecsBin = iolist_to_binary(Recs),
    DataCrc = erlang:crc32(RecsBin),
    Head92 = <<"RBX1", 1:8, Flags:8, 96:16/little, 80:16/little, (length(Recs)):16/little,
        Offset:32/little, Total:32/little, 20260926:32/little, ImportId:16/binary,
        Device48:48/binary, DataCrc:32/little>>,
    <<Head92/binary, (erlang:crc32(Head92)):32/little, RecsBin/binary>>.

rbx2_payload(Recs, SyncId, Device48, SyncOffset, SyncTotal, SessionIndex, SessionOffset,
             SessionTotal, StartUtc, EndUtc, StartItow, StartNano, Flags) ->
    RecsBin = iolist_to_binary(Recs),
    DataCrc = erlang:crc32(RecsBin),
    Head132 = <<"RBX2", 2:8, Flags:8, 136:16/little, 80:16/little, (length(Recs)):16/little,
        SyncOffset:32/little, SyncTotal:32/little, 20260926:32/little, SyncId:16/binary,
        Device48:48/binary, SessionIndex:32/little, SessionOffset:32/little,
        SessionTotal:32/little, StartUtc:64/little, EndUtc:64/little,
        StartItow:32/little, StartNano:32/little-signed, 0:32/little, DataCrc:32/little>>,
    <<Head132/binary, (erlang:crc32(Head132)):32/little, RecsBin/binary>>.

simlive_rb_record(Itow, Lon, Lat, Speed) ->
    <<Itow:32/little, 2026:16/little, 9:8, 26:8, 12:8, 0:8, 0:8, 1:8, 0:32/little,
        0:32/little-signed, 3:8, 0:8, 0:8, 12:8, Lon:32/little-signed, Lat:32/little-signed,
        0:32/little-signed, 0:32/little-signed, 0:32/little, 0:32/little, Speed:32/little-signed,
        9000000:32/little-signed, 0:32/little, 0:32/little, 100:16/little, 0:8, 77:8,
        10:16/little-signed, 20:16/little-signed, 30:16/little-signed, 100:16/little-signed,
        200:16/little-signed, 300:16/little-signed>>.

simlive_rb_payload(Name, Recs, Duration) ->
    RecsBin = iolist_to_binary(Recs),
    Body = <<"RB", 1:8, 3:8, (byte_size(Name)):8, Name/binary, Duration:32/little,
        (length(Recs)):16/little, RecsBin/binary>>,
    <<Body/binary, (emqx_pg_ingest_codec:crc16_ccitt(Body)):16/little>>.

simlive_bm_payload(Name, TotalV, Current, Cells, Temps, MosTemp, Status) ->
    CellBin = iolist_to_binary([<<C:16/little>> || C <- Cells]),
    TempBin = iolist_to_binary([<<T:16/little-signed>> || T <- Temps]),
    Fixed = <<TotalV:16/little, Current:32/little-signed, 604:32/little-signed, 19:8, 100:8,
        50000:32/little, 10300:32/little, 10126331:32/little, 10126:32/little, 36720000:32/little,
        3584:16/little, 3564:16/little, 20:16/little, (length(Cells)):8, (length(Temps)):8>>,
    Body = <<"BM", 1:8, 1:8, (byte_size(Name)):8, Name/binary, Fixed/binary, CellBin/binary,
        TempBin/binary, MosTemp:16/little-signed, Status:8, 0:16/little>>,
    <<Body/binary, (emqx_pg_ingest_codec:crc16_ccitt(Body)):16/little>>.

pad48(Bin) ->
    Pad = 48 - byte_size(Bin),
    <<Bin/binary, 0:(Pad * 8)>>.

corrupt_last_byte(Payload) ->
    Size = byte_size(Payload),
    <<Head:(Size - 1)/binary, _Last:8>> = Payload,
    <<Head/binary, 255:8>>.
