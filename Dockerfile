# 把 emqx_pg_ingest 插件预装进 EMQX 镜像：容器启动即加载并启用，**不需要 Dashboard 上传、不需要 emqx ctl plugins allow**
#
# 用法（先把 Release 里的 emqx_pg_ingest-<版本>.tar.gz 下载到本文件同目录）：
#   docker build --build-arg PLUGIN_VSN=0.1.0 -t emqx-pg-ingest:5.9.1 .
#   docker run -d --name emqx -p 1883:1883 -p 18083:18083 emqx-pg-ingest:5.9
#
# 升级版本：只需改 --build-arg PLUGIN_VSN=<新版本> 重新构建（配置/镜像挂载不变）
# 插件依赖 emqx-plugin-helper v5.9.1，并用 OTP 27 构建；禁止使用会漂移的 latest。
ARG EMQX_IMAGE=emqx/emqx:5.9.1
FROM ${EMQX_IMAGE}

ARG PLUGIN_NAME=emqx_pg_ingest
ARG PLUGIN_VSN=0.1.0

# 1) 插件包放到位并解压（EMQX 5 的插件包是 .tar.gz release）
COPY --chown=emqx:emqx ${PLUGIN_NAME}-${PLUGIN_VSN}.tar.gz /opt/emqx/plugins/
RUN cd /opt/emqx/plugins \
    && mkdir -p ${PLUGIN_NAME}-${PLUGIN_VSN} \
    && tar zxf ${PLUGIN_NAME}-${PLUGIN_VSN}.tar.gz -C ${PLUGIN_NAME}-${PLUGIN_VSN}

# 2) 注册到 EMQX 配置（plugins.states），enable = true → 启动时自动加载并启用
RUN printf '\nplugins {\n  states = [\n    { name_vsn = "%s-%s", enable = true }\n  ]\n}\n' \
      "${PLUGIN_NAME}" "${PLUGIN_VSN}" >> /opt/emqx/etc/base.hocon

# 插件业务配置包含数据库连接信息，不烘焙进镜像；运行时仅挂载单个 JSON 文件。
# 不要再挂载整个 /opt/emqx/etc 或 /opt/emqx/plugins，否则会遮住上面的预装内容。
