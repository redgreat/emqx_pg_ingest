# emqx_pg_ingest —— EMQX → PostgreSQL 消费插件（Erlang）

订阅 EMQX 的 `message.publish` 钩子，把**二进制**定位/BMS 报文在 broker 侧解码后
幂等写入 PostgreSQL，**所有 SQL 与字段映射都在 JSON 配置里，可自由自定义**。

## ⚠️ 先确认：你的 broker 能不能装自定义插件

| 场景 | 能否安装 | 说明 |
|------|---------|------|
| **自建 EMQX 5.x**（OSS / Enterprise，控制台左侧有「插件 → 安装插件」） | ✅ 可以 | 用本插件 |
| **EMQX Cloud**（Serverless / Dedicated，域名形如 `*.emqxsl.cn`） | ❌ 不可以 | 云版禁止加载自定义插件，改用下面的替代方案 |

**EMQX Cloud 的两个替代方案**：
1. 规则引擎 + PostgreSQL 数据桥接：把原始 payload 存成 `bytea` 列，再由小组件解码（多一步）；
2. **复用本插件的解码模块**：`emqx_pg_ingest_codec.erl` 是**纯 Erlang 函数、零 EMQX 依赖**，
   可以直接放进一个独立消费进程（Erlang 或通过 ports 调用），在你自己的服务器上解码入库。

## 功能

- 钩子内只做解码与投递（**不阻塞 broker**），写库由 worker 池异步执行
- RBX2 三级幂等：轨迹段 `session_key`、MQTT 批次 `batch_key`、明细 `(session_key, record_index)`；RBX1/simlive 继续按旧键兼容
- 同一 `file_name` 固定散列到同一 worker → 批次内顺序稳定
- CRC 不通过 / 长度不符 → 拒绝入库并记 warning，绝不写脏数据
- 断线自动重连；写失败回滚 + 断开连接，下一条消息恢复
- 运行统计：节点 shell 执行 `emqx_pg_ingest:stats().`
- 热重载配置：`emqx_pg_ingest:reload().`（或重启插件）

## 支持的协议

| codec | 报文魔数 | 典型主题 | 来源 |
|-------|---------|---------|------|
| `rbx2` | `RBX2`（头 136B + N×80B，双 CRC32） | `deskwong/racebox/data` | `deskwong/doc/racebox-mqtt-binary.md` |
| `rbx1` | `RBX1`（头 96B + N×80B，双 CRC32） | `deskwong/racebox/data` | `deskwong/doc/racebox-mqtt-binary.md` |
| `simlive_rb` | `RB` + file_name + N×80B + CRC16 | `simlive/{device}/data` | `simlive/doc/binary_protocol.md` §2–3 |
| `simlive_bm` | `BM` + file_name + 快照 + CRC16 | `simlive/{device}/bms` | 同上 §3B |
| `auto` | 按魔数自动识别 | 任意 | — |

**字段换算的两处关键决定**：

1. **速度统一写成 km/h**：`lc_racebox.speed` 列语义是 km/h，而 `rbx1/rbx2` 协议里是 m/s
   （×3.6 换算），`simlive` 协议里本来就是 km/h（/100×60）。两路数据可直接同表同列。
2. **RBX1 的 `file_name` 合成为 `rbx1_{import_id}_{offset}`**：RBX1 一次同步会拆成多条 MQTT
   消息（同 `import_id`、不同 `offset`），而 `imp_racebox.file_name` 是唯一键，
   按消息级幂等才能既去重（QoS1 重投）又不丢批内的后续消息。
3. **RBX2 按设备真实轨迹段幂等**：`sync_id` 只追踪一次传输，稳定 `session_key` 标识跨同步的同一段；
   最终 `file_name` 仍为旧 Python 消费者使用的 `首条UTC_末条UTC`。已存在的旧文件会跳过，长段流式上传产生的临时数据会在段尾事务内回收。

## 目录结构

```text
emqx_pg_ingest/
├── Makefile                         # 打包入口：make rel → _build/default/emqx_plugrel/*.tar.gz
├── Dockerfile                       # 方式 D：把插件预装进 EMQX 镜像（免 allow，启动即启用）
├── rebar.config                     # deps(epgsql/emqx_plugin_helper) + emqx_plugrel 打包插件 + relx release
├── scripts/ensure-rebar3.sh         # 按本机 OTP 版本下载 EMQX 定制版 rebar3
├── .tool-versions                   # 目标 OTP（erlang 27.2，对应 EMQX 5.9）
├── src/
│   ├── emqx_pg_ingest_codec.erl     解码器（纯函数，无 EMQX 依赖，可单测）
│   ├── emqx_pg_ingest_config.erl    配置加载（JSON / .terms）+ 主题通配匹配
│   ├── emqx_pg_ingest.erl           钩子注册 + 解码分派 + 统计计数
│   ├── emqx_pg_ingest_pg.erl        PG 写入 worker（连接、事务、自定义 SQL）
│   ├── emqx_pg_ingest_sup.erl       监督树（worker 池）
│   ├── emqx_pg_ingest_app.erl       application
│   └── emqx_pg_ingest.app.src
├── test/emqx_pg_ingest_codec_tests.erl   17 条 eunit
├── priv/emqx_pg_ingest.json               配置样例（含可直接用的 SQL）
└── README.md
```

## 构建（产出可安装的插件包）

**EMQX 5.x 的插件包是 `.tar.gz`（OTP release），不是 zip**（zip 只用于 EMQX 4.x）。
包内是 `emqx_pg_ingest` 应用 + `emqx_plugin_helper` + `manifest.json`，用官方 `emqx_plugrel` 生成。

### ⚠️ OTP 大版本必须与目标 EMQX 一致

编译出来的 beam 只能向前兼容：**用比 EMQX 更新的 OTP 编出来的包，EMQX 加载会失败**。
参考：EMQX 5.8 → OTP 26；EMQX 5.9 → **OTP 27**（本仓库 `.tool-versions` 已写 `erlang 27.2`）。

```bash
cd emqx_pg_ingest
make rel        # = scripts/ensure-rebar3.sh（下载 EMQX 定制版 rebar3）+ rebar3 emqx_plugrel tar
# 产物：_build/default/emqx_plugrel/emqx_pg_ingest-0.1.0.tar.gz
```

`make rel` 会自动按本机 OTP 版本挑选对应的 EMQX 定制版 rebar3（25/26/27 → 3.19/3.20/3.24）。
若本机 OTP 与目标 EMQX 不一致，用 Docker 编（把 `/your/path` 换成本仓库路径）：

```bash
docker run --rm -v /your/path/emqx_pg_ingest:/p -w /p -e BUILD_WITHOUT_QUIC=1 erlang:27 \
  bash -lc "apt-get update -qq && apt-get install -y -qq git make curl >/dev/null && make rel"
```

**CI 已自动打包**：打 `v*` 标签的 Release 里直接挂着 `emqx_pg_ingest-<版本>.tar.gz`（用 OTP 27 构建），
并发布预装插件的多架构镜像 `ghcr.io/redgreat/emqx_pg_ingest:<版本>` 与 `latest`。
公开仓库创建的 GHCR Package 关联本仓库并继承公开可见性。

发布前先提交并推送全部修改，然后任选一个脚本推送标签。无参数默认自动递增 patch：

```bash
./scripts/release.sh
# 指定版本：./scripts/release.sh v0.2.0
```

```powershell
.\scripts\release.ps1
# 指定版本：.\scripts\release.ps1 -Tag v0.2.0
```

`.github/workflows/ci.yaml` 会依次执行 EUnit、按标签写入插件版本、构建 `.tar.gz` 和 SHA256、
构建 `linux/amd64 + linux/arm64` 镜像、推送 GHCR，最后创建 GitHub Release。任一步失败都不会发布不完整 Release。
发布脚本只负责校验 Git 状态和推送标签，不安装 Docker、不在本机构建，也不执行本地部署。

**不装 EMQX 的裸机自测**（解码/匹配/配置三块是纯 Erlang，任何 OTP 都能跑）：

```bash
mkdir -p ebin
erlc -o ebin src/*.erl test/*.erl
erl -noshell -pa ebin -eval 'eunit:test(emqx_pg_ingest_codec_tests, [verbose]), init:stop().'
# → All 17 tests passed
```

## 安装（自建 EMQX 5.x）

升级到 RBX2 前先在 `agentwong` 仓库根目录执行数据库迁移：

```bash
python scripts/migrate.py --apply
```

必须先确认 `V0008__racebox_idempotent_sessions.sql` 已成功，再启用新版插件。否则新版固件发来的 RBX2 会因缺列/缺表而整笔事务回滚。

**方式 A：命令行（推荐）**

```bash
cp emqx_pg_ingest-0.1.0.tar.gz $EMQX_HOME/plugins/     # $EMQX_HOME 例如 /opt/emqx
emqx ctl plugins install emqx_pg_ingest-0.1.0          # 注意是“包名-版本”
emqx ctl plugins list
emqx ctl plugins start emqx_pg_ingest-0.1.0
```

**方式 B：控制台上传（Dashboard）** —— EMQX 出于安全考虑，需先在节点上放行：

```bash
emqx ctl plugins allow emqx_pg_ingest-0.1.0
```

然后 Dashboard → **管理 → 插件扩展 → 插件**（Cluster Settings → Extensions → Plugins）→
「安装插件」→ 选 `emqx_pg_ingest-0.1.0.tar.gz` → 安装 → 启用。

**方式 C：REST API**

```bash
emqx ctl plugins allow emqx_pg_ingest-0.1.0
curl -u $KEY:$SECRET -X POST http://$EMQX_HOST:18083/api/v5/plugins/install \
     -H "Content-Type: multipart/form-data" -F "plugin=@emqx_pg_ingest-0.1.0.tar.gz"
```

**方式 D：使用 CI 发布的预装镜像（推荐，免 allow、免手工安装）**

`Dockerfile` 仅由 GitHub Actions 使用：CI 将插件解压进固定的 EMQX 5.9.1 镜像，并在
`base.hocon` 注册 `plugins.states`。部署端使用 `docker-compose.yml` 拉取
`ghcr.io/redgreat/emqx_pg_ingest:latest`，仓库不再提供本地插件镜像构建流程。

> **不能挂载整个 `/opt/emqx/etc` 或 `/opt/emqx/plugins`。** Docker bind mount 会遮住镜像里
> 已追加到 `base.hocon` 的 `plugins.states` 和已解压插件目录，这正是“包已挂载但插件没有加载”的常见原因。
> 只挂载 `/opt/emqx/etc/emqx_pg_ingest.json` 这一个业务配置文件即可，仍不要恢复 etc/plugins 整目录挂载。

### 升级版本时还要不要重新 `allow`？

| 安装方式 | 是否需要 `allow` | 说明 |
|---|---|---|
| **CLI** `emqx ctl plugins install` | ❌ 不需要 | 本地管理员操作，直接装；集群加 `--cluster` |
| **Docker 预装**（方式 D） | ❌ 不需要 | 启动即加载 |
| **Dashboard 上传 / REST API** | ✅ **每次都要** | `allow` 按 **`插件名-版本`** 精确匹配，且官方说明「授权状态是临时的，安装完成后会自动失效」（源码里默认 TTL 5 分钟）→ **allow 完要立刻上传**；集群环境**每个节点都要** allow |

所以：升级到 `emqx_pg_ingest-0.1.8` 时，若走 Dashboard 就要
`emqx ctl plugins allow emqx_pg_ingest-0.1.8`（上一版的 allow 不会继承；名字是「包名-版本」，版本不能省）。

**命令行升级流程**（不需要 allow）：

```bash
cp emqx_pg_ingest-0.1.8.tar.gz $EMQX_HOME/plugins/
emqx ctl plugins stop      emqx_pg_ingest-0.1.7
emqx ctl plugins uninstall emqx_pg_ingest-0.1.7     # EMQX 不允许同插件多版本共存
emqx ctl plugins install   emqx_pg_ingest-0.1.8
emqx ctl plugins start     emqx_pg_ingest-0.1.8
emqx ctl plugins list
# 集群：install/start/stop 追加 --cluster（Dashboard 上传会自动分发到各节点）
```

> 插件版本号来自 git tag（CI 会把它写进包名），包名规则固定为 `<插件名>-<版本>`；
> 我们的配置放在插件自己的 JSON 里（不在 EMQX 插件配置中），升级不会影响它。

**装完还要放配置文件**（插件不自带 EMQX 配置界面，读自己的 JSON）：

1. 把 `priv/emqx_pg_ingest.json` 复制到 `$EMQX_HOME/etc/emqx_pg_ingest.json` 并改成你的 PG/主题；
   或用 `EMQX_PG_INGEST_CONF=/path/to/emqx_pg_ingest.json` 指定；
2. 重启插件使配置生效（或节点 shell 执行 `emqx_pg_ingest:reload().`）；
3. 日志确认：`[emqx_pg_ingest] 配置已加载` / `PG 已连接` / `消费主题: [...]`。

## 配置说明

```jsonc
{
  "pg": { "host": "...", "port": 5432, "username": "...", "password": "...",
          "database": "eadm", "pool_size": 2, "timeout_ms": 5000 },
  "topics": [
    { "topic": "simlive/+/data",        // 支持 MQTT 通配 + / #
      "codec": "simlive_rb",            // rbx1 | rbx2 | simlive_rb | simlive_bm | auto
      "profile": "racebox",             // 用下面哪个 SQL profile
      "device_from_topic": 2 }          // 报文里没有 device_id 时，取主题第 N 段（1 起）
  ],
  "profiles": {
    "racebox": {
      "dedup":         "SELECT 1 FROM imp_racebox WHERE file_name = $1",   // 返回行 = 已处理过
      "imp_fields":    ["file_name", "device_id", "imp_stamp", "duration", "record_count"],
      "imp":           "INSERT INTO imp_racebox(...) VALUES ($1,...,$5) ON CONFLICT (file_name) DO NOTHING",
      "record_fields": ["imp_stamp", "file_name", "device_id", "itow", "..."], // 顺序 = $1..$n
      "record":        "INSERT INTO lc_racebox(...) VALUES ($1,...,$31)"
    }
  }
}
```

**字段从哪来**：`record_fields` 里的名字 = 解码后的字段名（见协议文档的字段表），
加上插件注入的 `imp_stamp`（UUID v4）、`file_name`、`device_id`、`duration`、`record_count`。
`*_fields` 与 SQL 里的 `$n` **按位置一一对应**，改字段只改这两处即可，不用改代码。
字段名写错会报 `{missing_field, 字段名, [...]}` 并回滚，便于定位配置问题。

### BMS 表（当前仓库尚未建，需要时按此新建）

```sql
create table imp_bms (id bigserial primary key, file_name varchar(200) not null,
                      device_id varchar(100), imp_stamp uuid not null,
                      inserttime timestamptz not null default current_timestamp);
create unique index uni_imp_bms_file_name on imp_bms (file_name);

create table lc_bms (id bigserial primary key, imp_stamp uuid not null,
                     file_name varchar(200), device_id varchar(100),
                     total_voltage numeric(8,3), current numeric(8,3), power integer,
                     soc smallint, soh smallint, capacity_total numeric(10,3),
                     capacity_remain numeric(10,3), cycle_ah numeric(12,3),
                     total_cycles integer, runtime_sec integer,
                     cell_max_v numeric(6,3), cell_min_v numeric(6,3), cell_delta_v numeric(6,3),
                     cell_count smallint, cell_voltages numeric(6,3)[], temps_c smallint[],
                     mos_temp_c smallint, charging boolean, chg_mos_on boolean,
                     dsg_mos_on boolean, balancing boolean, alarm_code integer,
                     inserttime timestamptz not null default current_timestamp);
create index non_lc_bms_file_name on lc_bms (file_name);
create index non_lc_bms_inserttime on lc_bms (inserttime desc);
```

## 验证

```bash
# 1) 节点 shell 看统计（EMQX 控制台 → 诊断工具 → 节点 shell）
emqx_pg_ingest:stats().
%% #{accepted => 12, written => 12, duplicate => 2, decode_failed => 0, ...}

# 2) 日志
emqx ctl log tail            # 或控制台「管理 → 日志」
#   [emqx_pg_ingest] 入库完成 worker=1 file_name=20260926120000_... records=480

# 3) 查库
psql -h ... -d eadm -c "select count(*), max(inserttime) from lc_racebox;"
```

## 注意事项

1. **BMS 定长块有文档分歧**：`binary_protocol.md` 正文表格写 45B，而随文 Python 参考实现按
   `<HiiBBIIIIIHHHBB` = **40B** 解析；本插件按 40B 实现。**首次联调请用一条真实 `BM` 报文核对**，
   不一致时只改 `decode_simlive_bm/1` 里的定长定义即可。
2. 去重语义：`simlive` 用报文自带的 `file_name`；`rbx1` 仅提供消息级去重；`rbx2` 用稳定
   `session_key`、`batch_key` 和 `record_index` 三级去重，并用最终文件名兼容旧 Python 消费者。
3. 钩子**原样返回消息**（不改内容、不拦截），插件异常只记日志，不影响消息转发。
4. `pool_size` 为 worker 数；同一 `file_name` 永远落在同一 worker，**不要把 pool_size 调大后又改字段映射导致批次分裂**。
   RBX2 实际以稳定的 `session_key` 路由，同一轨迹段的 open/final 批次不会落到不同 worker。

## 测试

```bash
erlc -o ebin src/*.erl test/*.erl
erl -noshell -pa ebin -eval 'eunit:test(emqx_pg_ingest_codec_tests, [verbose]), init:stop().'
```

覆盖 13 条：CRC16 标准向量、CRC32 与 `erlang:crc32/1` 一致性、UUID v4 格式、魔数识别、
RBX1 完整解码与 km/h 换算、RBX2 稳定轨迹段键/记录序号、RBX1 长度/CRC 错误、simlive RB 解码与 CRC 错误与截断、
simlive BM 快照（含单体电压/温度数组、状态位）、主题 `+`/`#` 通配与第 N 段提取。
