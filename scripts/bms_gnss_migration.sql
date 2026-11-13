-- simlive BMS 板载定位字段迁移 / 建表脚本
--
-- 用途：simlive 固件在 `BM` 消息主体 CRC16 之后追加 24B 板载定位数据 + 2B 尾段 CRC16
--       （flags bit1，见 simlive/doc/binary_protocol.md §3B.1）。
--       本插件解码后会把 gnss_* 字段写入 lc_bms，发布插件前必须先执行本脚本，
--       否则 SQL 引用的列不存在，整条 BMS 消息的事务会回滚。
--
-- 兼容：新库直接建表；已有 imp_bms/lc_bms 的库只执行下面的 ALTER（IF NOT EXISTS，可重复跑）。
--       线上生产库的时间列叫 createdat（本脚本建表也统一用 createdat），历史脚本里
--       出现过 inserttime，因此时间索引按实际存在的列动态创建，避免对老库直接报错。
-- 语义：旧固件 / 无定位消息 → gnss_* 全为 NULL；未定位 → gnss_valid=false、gnss_utc=NULL。
--
-- 执行：psql -h <host> -p <port> -U <user> -d <db> -f scripts/bms_gnss_migration.sql

create table if not exists imp_bms (
    id          bigserial primary key,
    file_name   varchar(200) not null,
    device_id   varchar(100),
    imp_stamp   uuid not null,
    createdat   timestamptz not null default current_timestamp
);
create unique index if not exists uni_imp_bms_file_name on imp_bms (file_name);

create table if not exists lc_bms (
    id                bigserial primary key,
    imp_stamp         uuid not null,
    file_name         varchar(200),
    device_id         varchar(100),
    total_voltage     numeric(8,3),
    current           numeric(8,3),
    power             integer,
    soc               smallint,
    soh               smallint,
    capacity_total    numeric(10,3),
    capacity_remain   numeric(10,3),
    cycle_ah          numeric(12,3),
    total_cycles      integer,
    runtime_sec       integer,
    cell_max_v        numeric(6,3),
    cell_min_v        numeric(6,3),
    cell_delta_v      numeric(6,3),
    cell_count        smallint,
    cell_voltages     numeric(6,3)[],
    temps_c           smallint[],
    mos_temp_c        smallint,
    charging          boolean,
    chg_mos_on        boolean,
    dsg_mos_on        boolean,
    balancing         boolean,
    alarm_code        integer,
    gnss_valid        boolean,
    gnss_latitude     numeric(10,7),
    gnss_longitude    numeric(10,7),
    gnss_altitude_m   numeric(8,1),
    gnss_speed_kmh    numeric(7,2),
    gnss_course_deg   numeric(6,1),
    gnss_satellites   smallint,
    gnss_utc          text,
    createdat         timestamptz not null default current_timestamp
);

-- 旧表补列（可重复执行；新表已含这些列时会被 IF NOT EXISTS 跳过）
alter table lc_bms
    add column if not exists gnss_valid        boolean,
    add column if not exists gnss_latitude    numeric(10,7),
    add column if not exists gnss_longitude   numeric(10,7),
    add column if not exists gnss_altitude_m  numeric(8,1),
    add column if not exists gnss_speed_kmh   numeric(7,2),
    add column if not exists gnss_course_deg  numeric(6,1),
    add column if not exists gnss_satellites  smallint,
    add column if not exists gnss_utc         text;

create index if not exists non_lc_bms_file_name on lc_bms (file_name);

-- 时间列在 prod 为 createdat、早期脚本为 inserttime，按实际存在的列建索引。
do $$
begin
    if exists (
        select 1 from information_schema.columns
        where table_schema = current_schema()
          and table_name = 'lc_bms' and column_name = 'createdat'
    ) then
        execute 'create index if not exists non_lc_bms_createdat on lc_bms (createdat desc)';
    elsif exists (
        select 1 from information_schema.columns
        where table_schema = current_schema()
          and table_name = 'lc_bms' and column_name = 'inserttime'
    ) then
        execute 'create index if not exists non_lc_bms_inserttime on lc_bms (inserttime desc)';
    end if;
end $$;

create index if not exists non_lc_bms_gnss_pos on lc_bms (gnss_latitude, gnss_longitude)
    where gnss_valid;

-- 校验：应能看到 8 个 gnss_* 列
-- select column_name, data_type from information_schema.columns
--  where table_name = 'lc_bms' and column_name like 'gnss_%' order by column_name;

-- 抽样查看带定位的 BMS 快照
-- select file_name, device_id, soc, gnss_valid, gnss_latitude, gnss_longitude, gnss_utc
--   from lc_bms where gnss_valid order by createdat desc limit 10;
