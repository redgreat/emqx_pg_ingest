%%%-------------------------------------------------------------------
%%% @doc EMQX PostgreSQL 消费插件 —— 二进制协议解码（纯函数，不依赖 EMQX）
%%%
%%% 支持四种报文（按魔数自动识别，也可按 topic 显式指定）：
%%%   rbx1        deskwong 固件：`deskwong/racebox/data`，`RBX1` 头 96B + N×80B（小端）
%%%   rbx2        deskwong 固件：轨迹段级幂等协议，`RBX2` 头 136B + N×80B（小端）
%%%   simlive_rb  simlive 固件：`simlive/{device}/data`，`RB` + file_name + N×80B + CRC16
%%%   simlive_bm  simlive 固件：`simlive/{device}/bms`，`BM` + file_name + 快照 + CRC16
%%%
%%% 参考：deskwong/doc/racebox-mqtt-binary.md、simlive/doc/binary_protocol.md
%%% 注意：两种定位协议的 speed 单位不同（rbx1 为 m/s、simlive 为 km/h），
%%%       本模块统一换算成 km/h，以对齐 lc_racebox.speed 列语义。
%%% @end
%%%-------------------------------------------------------------------
-module(emqx_pg_ingest_codec).

-export([decode/2, magic/1, crc16_ccitt/1, crc32/1, uuid_v4/0]).
-export([decode_rbx1/1, decode_rbx2/1, decode_simlive_rb/1, decode_simlive_bm/1]).

-define(RBX1_HEADER_SIZE, 96).
-define(RBX2_HEADER_SIZE, 136).
-define(RECORD_SIZE, 80).
-define(MPS_TO_KMH, 3.6).

-type codec() :: auto | rbx1 | rbx2 | simlive_rb | simlive_bm.
-type decoded() :: #{
    kind := atom(),
    file_name := binary(),
    device_id := binary() | undefined,
    duration := non_neg_integer(),
    record_count := non_neg_integer(),
    total := non_neg_integer() | undefined,
    records := [map()]
}.

-export_type([codec/0, decoded/0]).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc 按 codec 解码报文；auto 时依据魔数分派。
-spec decode(codec(), binary()) -> {ok, decoded()} | {error, term()}.
decode(auto, Payload) ->
    case magic(Payload) of
        unknown -> {error, unknown_magic};
        Codec -> decode(Codec, Payload)
    end;
decode(rbx1, Payload) ->
    decode_rbx1(Payload);
decode(rbx2, Payload) ->
    decode_rbx2(Payload);
decode(simlive_rb, Payload) ->
    decode_simlive_rb(Payload);
decode(simlive_bm, Payload) ->
    decode_simlive_bm(Payload);
decode(Codec, _Payload) ->
    {error, {unsupported_codec, Codec}}.

%% @doc 侦察报文类型（只看前 4 字节，不校验 CRC）。
-spec magic(binary()) -> rbx1 | rbx2 | simlive_rb | simlive_bm | unknown.
magic(<<"RBX1", _/binary>>) -> rbx1;
magic(<<"RBX2", _/binary>>) -> rbx2;
magic(<<"RB", _/binary>>) -> simlive_rb;
magic(<<"BM", _/binary>>) -> simlive_bm;
magic(_) -> unknown.

%% @doc CRC-32/IEEE（转发 erlang:crc32/1，便于测试对照）。
-spec crc32(binary()) -> non_neg_integer().
crc32(Bin) -> erlang:crc32(Bin).

%% @doc CRC-16/CCITT-FALSE：poly 0x1021、初值 0xFFFF、不反射、无 xorout。
%% 标准向量：crc16_ccitt(<<"123456789">>) =:= 16#29B1
-spec crc16_ccitt(binary()) -> non_neg_integer().
crc16_ccitt(Data) -> crc16_fold(Data, 16#FFFF).

crc16_fold(<<>>, Crc) ->
    Crc band 16#FFFF;
crc16_fold(<<Byte:8, Rest/binary>>, Crc) ->
    crc16_fold(Rest, crc16_bits(8, Crc bxor (Byte bsl 8))).

crc16_bits(0, Crc) ->
    Crc band 16#FFFF;
crc16_bits(N, Crc) ->
    Next =
        case Crc band 16#8000 of
            0 -> Crc bsl 1;
            _ -> (Crc bsl 1) bxor 16#1021
        end,
    crc16_bits(N - 1, Next band 16#FFFF).

%% @doc 生成 UUID v4 文本（用于 imp_stamp）。
-spec uuid_v4() -> binary().
uuid_v4() ->
    <<A:32, B:16, C0:16, D0:16, E:48>> = crypto:strong_rand_bytes(16),
    C = (C0 band 16#0FFF) bor 16#4000,
    D = (D0 band 16#3FFF) bor 16#8000,
    iolist_to_binary(
        io_lib:format("~8.16.0b-~4.16.0b-~4.16.0b-~4.16.0b-~12.16.0b", [A, B, C, D, E])
    ).

%%%===================================================================
%%% RBX1（deskwong 固件）
%%%===================================================================

%% @doc RBX1：96 字节消息头 + count×80 字节记录，头部/记录区各一个 CRC32。
%%
%% file_name 说明：RBX1 一次同步会被拆成多条 MQTT 消息（同 import_id、不同 offset），
%% 而 imp_racebox.file_name 是唯一键，因此这里合成 `rbx1_{import_id}_{offset}`，
%% 保证同一批的多条消息各自幂等（QoS1 重投即被去重）。
-spec decode_rbx1(binary()) -> {ok, decoded()} | {error, term()}.
decode_rbx1(
    <<"RBX1", Ver:8, Flags:8, HeaderSize:16/little, RecordSize:16/little, Count:16/little,
        Offset:32/little, Total:32/little, SyncDate:32/little, ImportIdRaw:16/binary,
        DeviceRaw:48/binary, DataCrc:32/little, HeaderCrc:32/little, RecordsBin/binary>> = Payload
) when Ver =:= 1, (Flags band 16#FE) =:= 0 ->
    Expected = Count * RecordSize,
    case {HeaderSize, RecordSize, byte_size(RecordsBin)} of
        {?RBX1_HEADER_SIZE, ?RECORD_SIZE, Expected} ->
            Header92 = binary:part(Payload, 0, 92),
            case {crc32(Header92), crc32(RecordsBin)} of
                {HeaderCrc, DataCrc} ->
                    ImportId = uuid_bin_to_str(ImportIdRaw),
                    DeviceName = cstring(DeviceRaw),
                    {ok, #{
                        kind => rbx1,
                        file_name =>
                            iolist_to_binary(["rbx1_", ImportId, "_", integer_to_binary(Offset)]),
                        device_id => null_if_empty(DeviceName),
                        duration => 0,
                        record_count => Count,
                        total => Total,
                        import_id => ImportId,
                        offset => Offset,
                        sync_date => SyncDate,
                        records => decode_records(RecordsBin, fun decode_rbx1_record/1)
                    }};
                {GotH, GotD} ->
                    {error,
                        {crc_mismatch, [
                            {header, #{expect => HeaderCrc, got => GotH}},
                            {data, #{expect => DataCrc, got => GotD}}
                        ]}}
            end;
        {_, _, Size} ->
            {error, {bad_length, #{count => Count, records_bytes => Size, expect => Expected}}}
    end;
decode_rbx1(<<"RBX1", _/binary>>) ->
    {error, unsupported_rbx1_version_or_flags};
decode_rbx1(_) ->
    {error, not_rbx1}.

%%%===================================================================
%%% RBX2（轨迹段级幂等）
%%%===================================================================

%% flags: bit0=本次同步最终批次，bit1=当前轨迹段最终批次。
%% session_key 由设备名、首条 UTC、iTOW、纳秒组成，重复下载时保持稳定；sync_id 仅用于一次传输。
-spec decode_rbx2(binary()) -> {ok, decoded()} | {error, term()}.
decode_rbx2(
    <<"RBX2", 2:8, Flags:8, HeaderSize:16/little, RecordSize:16/little, Count:16/little,
        SyncOffset:32/little, SyncTotal:32/little, SyncDate:32/little, SyncIdRaw:16/binary,
        DeviceRaw:48/binary, SessionIndex:32/little, SessionOffset:32/little,
        SessionTotal:32/little, StartUtc:64/little, EndUtc:64/little,
        StartItow:32/little, StartNano:32/little-signed, _Reserved:32/little,
        DataCrc:32/little, HeaderCrc:32/little, RecordsBin/binary>> = Payload
) when (Flags band 16#FC) =:= 0 ->
    Expected = Count * RecordSize,
    SessionFinal = (Flags band 2) =/= 0,
    SessionShapeOk = case SessionFinal of
        true -> SessionTotal > 0 andalso EndUtc >= StartUtc andalso
                SessionOffset + Count =:= SessionTotal;
        false -> (SessionTotal =:= 0 orelse SessionOffset + Count =< SessionTotal) andalso
                 (EndUtc =:= 0 orelse EndUtc >= StartUtc)
    end,
    case {HeaderSize, RecordSize, byte_size(RecordsBin), StartUtc > 0, SessionShapeOk} of
        {?RBX2_HEADER_SIZE, ?RECORD_SIZE, Expected, true, true} ->
            Header132 = binary:part(Payload, 0, 132),
            case {crc32(Header132), crc32(RecordsBin)} of
                {HeaderCrc, DataCrc} ->
                    SyncId = uuid_bin_to_str(SyncIdRaw),
                    DeviceName = cstring(DeviceRaw),
                    StartText = utc14(StartUtc),
                    FileName = case EndUtc of
                        0 -> iolist_to_binary([
                            StartText, "_open_", integer_to_binary(StartItow), "_",
                            integer_to_binary(StartNano)
                        ]);
                        _ -> <<StartText/binary, "_", (utc14(EndUtc))/binary>>
                    end,
                    SessionKey = iolist_to_binary([
                        "rbx2_", DeviceName, "_", StartText, "_",
                        integer_to_binary(StartItow), "_", integer_to_binary(StartNano)
                    ]),
                    BatchKey = iolist_to_binary([
                        SessionKey, "_", integer_to_binary(SessionOffset)
                    ]),
                    Records0 = decode_records(RecordsBin, fun decode_rbx1_record/1),
                    Records = add_record_indexes(Records0, SessionOffset),
                    {ok, #{
                        kind => rbx2,
                        file_name => FileName,
                        batch_key => BatchKey,
                        session_key => SessionKey,
                        device_id => null_if_empty(DeviceName),
                        duration => 0,
                        record_count => Count,
                        total => SyncTotal,
                        sync_id => SyncId,
                        sync_offset => SyncOffset,
                        sync_date => SyncDate,
                        sync_complete => (Flags band 1) =/= 0,
                        session_index => SessionIndex,
                        session_offset => SessionOffset,
                        session_total => SessionTotal,
                        session_complete => SessionFinal,
                        session_start_utc => StartUtc,
                        session_end_utc => EndUtc,
                        payload_crc32 => DataCrc,
                        records => Records
                    }};
                {GotH, GotD} ->
                    {error, {crc_mismatch, [
                        {header, #{expect => HeaderCrc, got => GotH}},
                        {data, #{expect => DataCrc, got => GotD}}
                    ]}}
            end;
        Other ->
            {error, {bad_rbx2_layout, Other}}
    end;
decode_rbx2(<<"RBX2", _/binary>>) ->
    {error, unsupported_rbx2_version_or_flags};
decode_rbx2(_) ->
    {error, not_rbx2}.

utc14(Value) ->
    list_to_binary(io_lib:format("~14..0B", [Value])).

add_record_indexes(Records, Offset) ->
    add_record_indexes(Records, Offset, []).

add_record_indexes([], _Index, Acc) ->
    lists:reverse(Acc);
add_record_indexes([Record | Rest], Index, Acc) ->
    add_record_indexes(Rest, Index + 1, [Record#{record_index => Index} | Acc]).

%% @doc RBX1 单条 80 字节记录（偏移见 deskwong/doc/racebox-mqtt-binary.md §2）。
-spec decode_rbx1_record(binary()) -> map().
decode_rbx1_record(
    <<Itow:32/little, Year:16/little, Month:8, Day:8, Hour:8, Minute:8, Second:8, _B11:8,
        TimeAccuracy:32/little, Nanoseconds:32/little-signed, FixStatus:8, _B21:16, Svs:8,
        Longitude:32/little-signed, Latitude:32/little-signed, WgsAlt:32/little-signed,
        MslAlt:32/little-signed, HAcc:32/little, VAcc:32/little, Speed:32/little-signed,
        Heading:32/little-signed, SpeedAcc:32/little, HeadingAcc:32/little, Pdop:16/little,
        _B66:16, Gx:16/little-signed, Gy:16/little-signed, Gz:16/little-signed,
        Rx:16/little-signed, Ry:16/little-signed, Rz:16/little-signed>>
) ->
    #{
        itow => Itow,
        year => Year,
        month => Month,
        day => Day,
        hour => Hour,
        minute => Minute,
        second => Second,
        time_accuracy => TimeAccuracy,
        nanoseconds => Nanoseconds,
        fix_status => FixStatus,
        numberof_svs => Svs,
        longitude => Longitude / 1.0e7,
        latitude => Latitude / 1.0e7,
        wgs_altitude => WgsAlt / 1000.0,
        msl_altitude => MslAlt / 1000.0,
        horizontal_accuracy => HAcc / 1000.0,
        vertical_accuracy => VAcc / 1000.0,
        %% RBX1 的 speed 是 m/s（文档 §2）→ 统一换算 km/h
        speed => Speed / 1000.0 * ?MPS_TO_KMH,
        heading => Heading / 1.0e5,
        speed_accuracy => SpeedAcc,
        heading_accuracy => HeadingAcc,
        pdop => Pdop,
        gforce_x => Gx / 1000.0,
        gforce_y => Gy / 1000.0,
        gforce_z => Gz / 1000.0,
        rotation_rate_x => Rx / 100.0,
        rotation_rate_y => Ry / 100.0,
        rotation_rate_z => Rz / 100.0
    }.

%%%===================================================================
%%% simlive（RB / BM）
%%%===================================================================

%% @doc simlive 定位批量：`RB` + file_name + N×80B 原始 UBX payload + CRC16。
%% file_name 即消费端去重主键（对应 imp_racebox.file_name 唯一约束）。
-spec decode_simlive_rb(binary()) -> {ok, decoded()} | {error, term()}.
decode_simlive_rb(
    <<"RB", Ver:8, _Flags:8, NameLen:8, Name:NameLen/binary, Duration:32/little, Count:16/little,
        RecordsBin:(Count * ?RECORD_SIZE)/binary, Crc:16/little>> = Payload
) when Ver =:= 1 ->
    Body = binary:part(Payload, 0, byte_size(Payload) - 2),
    case crc16_ccitt(Body) of
        Crc ->
            {ok, #{
                kind => simlive_rb,
                file_name => Name,
                device_id => undefined,
                duration => Duration,
                record_count => Count,
                total => Count,
                records => decode_records(RecordsBin, fun decode_simlive_rb_record/1)
            }};
        Got ->
            {error, {crc_mismatch, #{expect => Crc, got => Got}}}
    end;
decode_simlive_rb(<<"RB", _/binary>> = Payload) ->
    %% 魔数正确但结构/长度不符（截断等）→ 明确报长度问题，避免误判成版本不支持
    {error, {bad_simlive_rb_payload, byte_size(Payload)}};
decode_simlive_rb(_) ->
    {error, not_simlive_rb}.

%% @doc simlive 单条 80 字节记录（simlive/doc/binary_protocol.md §3）。
%% 与 rbx1 的差异：speed 已是 km/h（/100*60）、含 battery、pdop 后多两个标志字节。
-spec decode_simlive_rb_record(binary()) -> map().
decode_simlive_rb_record(
    <<Itow:32/little, Year:16/little, Month:8, Day:8, Hour:8, Minute:8, Second:8, _Validity:8,
        TimeAccuracy:32/little, Nanoseconds:32/little-signed, FixStatus:8, _FixFlags:8,
        _DateTimeFlags:8, Svs:8, Longitude:32/little-signed, Latitude:32/little-signed,
        WgsAlt:32/little-signed, MslAlt:32/little-signed, HAcc:32/little, VAcc:32/little,
        Speed:32/little-signed, Heading:32/little-signed, SpeedAcc:32/little,
        HeadingAcc:32/little, Pdop:16/little, _LatLonFlags:8, Battery:8, Gx:16/little-signed,
        Gy:16/little-signed, Gz:16/little-signed, Rx:16/little-signed, Ry:16/little-signed,
        Rz:16/little-signed>>
) ->
    #{
        itow => Itow,
        year => Year,
        month => Month,
        day => Day,
        hour => Hour,
        minute => Minute,
        second => Second,
        time_accuracy => TimeAccuracy,
        nanoseconds => Nanoseconds,
        fix_status => FixStatus,
        numberof_svs => Svs,
        longitude => Longitude / 1.0e7,
        latitude => Latitude / 1.0e7,
        wgs_altitude => WgsAlt / 1000.0,
        msl_altitude => MslAlt / 1000.0,
        horizontal_accuracy => HAcc / 1000.0,
        vertical_accuracy => VAcc / 1000.0,
        %% simlive 的 speed 已是 km/h
        speed => Speed / 100.0 * 60.0,
        heading => Heading / 1.0e5,
        speed_accuracy => SpeedAcc,
        heading_accuracy => HeadingAcc / 1.0e5,
        pdop => Pdop,
        battery => Battery,
        gforce_x => Gx / 1000.0,
        gforce_y => Gy / 1000.0,
        gforce_z => Gz / 1000.0,
        rotation_rate_x => Rx / 100.0,
        rotation_rate_y => Ry / 100.0,
        rotation_rate_z => Rz / 100.0
    }.

%% @doc simlive BMS 快照（`BM` + file_name + 40B 定长字段 + 单体电压/温度数组 + CRC16）。
%% 定长块按 simlive/doc/binary_protocol.md §3B 的 Python 参考实现（`<HiiBBIIIIIHHHBB`，40B）解析；
%% 注意文档表格标注为 45B，与参考实现不一致，首次联调请用一条真实报文核对。
-spec decode_simlive_bm(binary()) -> {ok, decoded()} | {error, term()}.
decode_simlive_bm(
    <<"BM", Ver:8, _Flags:8, NameLen:8, Name:NameLen/binary, Fixed:40/binary, Rest/binary>> =
        Payload
) when Ver =:= 1 ->
    <<TotalV:16/little, Current:32/little-signed, Power:32/little-signed, Soc:8, Soh:8,
        CapTotal:32/little, CapRemain:32/little, CycleAh:32/little, TotalCycles:32/little,
        Runtime:32/little, CellMax:16/little, CellMin:16/little, CellDelta:16/little,
        CellCount:8, TempCount:8>> = Fixed,
    Need = CellCount * 2 + TempCount * 2 + 2 + 1 + 2 + 2,
    case byte_size(Rest) =:= Need of
        false ->
            {error, {bad_bms_length, #{cells => CellCount, temps => TempCount, tail => byte_size(Rest)}}};
        true ->
            Body = binary:part(Payload, 0, byte_size(Payload) - 2),
            <<Crc:16/little>> = binary:part(Payload, byte_size(Payload) - 2, 2),
            case crc16_ccitt(Body) of
                Crc ->
                    <<CellsBin:(CellCount * 2)/binary, TempsBin:(TempCount * 2)/binary,
                        MosTemp:16/little-signed, Status:8, Alarm:16/little, _Crc:16/little>> =
                        Rest,
                    {ok, #{
                        kind => simlive_bm,
                        file_name => Name,
                        device_id => undefined,
                        duration => 0,
                        record_count => 1,
                        total => 1,
                        records => [
                            #{
                                total_voltage => TotalV / 100.0,
                                current => Current / 100.0,
                                power => Power,
                                soc => Soc,
                                soh => Soh,
                                capacity_total => CapTotal / 1000.0,
                                capacity_remain => CapRemain / 1000.0,
                                cycle_ah => CycleAh / 1000.0,
                                total_cycles => TotalCycles,
                                runtime_sec => Runtime,
                                cell_max_v => CellMax / 1000.0,
                                cell_min_v => CellMin / 1000.0,
                                cell_delta_v => CellDelta / 1000.0,
                                cell_count => CellCount,
                                cell_voltages => [V / 1000.0 || <<V:16/little>> <= CellsBin],
                                temp_count => TempCount,
                                temps_c => [T || <<T:16/little-signed>> <= TempsBin],
                                mos_temp_c => MosTemp,
                                charging => (Status band 16#01) =/= 0,
                                chg_mos_on => (Status band 16#02) =/= 0,
                                dsg_mos_on => (Status band 16#04) =/= 0,
                                balancing => (Status band 16#08) =/= 0,
                                alarm_code => Alarm
                            }
                        ]
                    }};
                Got ->
                    {error, {crc_mismatch, #{expect => Crc, got => Got}}}
            end
    end;
decode_simlive_bm(<<"BM", _/binary>> = Payload) ->
    {error, {bad_simlive_bm_payload, byte_size(Payload)}};
decode_simlive_bm(_) ->
    {error, not_simlive_bm}.

%%%===================================================================
%%% 内部工具
%%%===================================================================

decode_records(Bin, Fun) -> decode_records(Bin, Fun, []).

decode_records(<<>>, _Fun, Acc) ->
    lists:reverse(Acc);
decode_records(<<Chunk:?RECORD_SIZE/binary, Rest/binary>>, Fun, Acc) ->
    decode_records(Rest, Fun, [Fun(Chunk) | Acc]).

uuid_bin_to_str(<<A:32, B:16, C:16, D:16, E:48>>) ->
    iolist_to_binary(
        io_lib:format("~8.16.0b-~4.16.0b-~4.16.0b-~4.16.0b-~12.16.0b", [A, B, C, D, E])
    ).

cstring(Bin) ->
    case binary:split(Bin, <<0>>) of
        [Head | _] -> Head;
        [] -> <<>>
    end.

null_if_empty(<<>>) -> undefined;
null_if_empty(Bin) -> Bin.


