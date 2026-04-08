# S2AG 摘要入库脚本使用指南

脚本路径：`data/scripts/ingest_s2ag_abstracts.py`
数据目录（服务器）：`/data/s2ag_abstract/raw/`
服务器：`zyg@49.52.27.139`

---

## 环境信息

| 项目 | 值 |
|------|-----|
| 服务器 | `49.52.27.139`，用户 `zyg` |
| Python 解释器 | `/home/zyg/DaseS/backend/.venv/bin/python`（Python 3.14） |
| uv 路径 | `/home/zyg/.local/bin/uv` |
| ES 地址 | `http://localhost:9200`（服务器内网） |
| ES 别名 | `dblp_search` |
| 数据目录 | `/data/s2ag_abstract/raw/`（S2AG 分片 `.gz` 文件） |
| 日志文件 | `/data/s2ag_abstract/ingest.log` |
| 断点状态 | `/data/s2ag_abstract/state.json` |

> **注意**：服务器上禁止直接使用 `uv run`，因 `torch` wheel 与服务器 glibc（manylinux_2_17）不兼容会导致安装失败。必须直接调用 `.venv/bin/python`。

---

## 首次部署（只需执行一次）

### 1. 同步脚本到服务器

```bash
# 在本地项目根目录执行
scp data/scripts/ingest_s2ag_abstracts.py \
  zyg@49.52.27.139:/data/s2ag_abstract/ingest_s2ag_abstracts.py
```

### 2. 在服务器上安装依赖

```bash
ssh zyg@49.52.27.139

# 安装脚本所需的依赖（elasticsearch 必须 <9.0，与服务器 ES 8.x 版本匹配）
/home/zyg/.local/bin/uv pip install \
  --python /home/zyg/DaseS/backend/.venv/bin/python \
  python-dotenv 'elasticsearch>=8.0,<9.0' requests
```

---

## 日常运行

### 后台全量运行（推荐）

```bash
ssh zyg@49.52.27.139

nohup /home/zyg/DaseS/backend/.venv/bin/python \
  /data/s2ag_abstract/ingest_s2ag_abstracts.py \
  --skip-download \
  --output-dir /data/s2ag_abstract \
  --es-host http://localhost:9200 \
  --es-alias dblp_search \
  > /data/s2ag_abstract/ingest.log 2>&1 &

echo "PID: $!"
```

> 支持断点续传：中断后重新执行相同命令，已完成的文件自动跳过。

---

## 监控进度

```bash
# 实时查看日志
tail -f /data/s2ag_abstract/ingest.log

# 查看已完成哪些文件
cat /data/s2ag_abstract/state.json

# 查看进程是否存活
ps aux | grep ingest_s2ag | grep -v grep

# 查看 raw 目录中的文件数量
ls /data/s2ag_abstract/raw/*.gz | wc -l
```

---

## 常用参数说明

```bash
--skip-download          # 跳过下载，直接处理已有 .gz 文件（服务器上用此参数）
--download-only          # 仅下载，不写入 ES
--limit N                # 限制处理 N 条记录（用于测试，0=全量）
--max-files N            # 限制处理 N 个分片文件（0=全量）
--overwrite              # 强制覆盖已有摘要（默认只填充空摘要）
--es-host URL            # ES 地址，默认 http://49.52.27.139:9200
--es-alias NAME          # ES 别名，默认 dblp_search
--output-dir PATH        # 输出根目录，raw/ 子目录存放 .gz 文件
```

---

## 测试验证（处理前 100 条）

```bash
ssh zyg@49.52.27.139

/home/zyg/DaseS/backend/.venv/bin/python \
  /data/s2ag_abstract/ingest_s2ag_abstracts.py \
  --skip-download \
  --output-dir /data/s2ag_abstract \
  --es-host http://localhost:9200 \
  --es-alias dblp_search \
  --limit 100
```

---

## 常见问题

| 问题 | 原因 | 解决方案 |
|------|------|----------|
| `uv run` 报 torch wheel 错误 | 服务器 glibc 版本（manylinux_2_17）不支持 torch 2.x | 改用 `.venv/bin/python` 直接调用 |
| `BadRequestError(400)` ES 版本不匹配 | elasticsearch 客户端 v9 不兼容 ES 服务端 v8 | 安装 `elasticsearch>=8.0,<9.0` |
| 脚本启动后卡住无输出 | `--skip-download` 模式下仍请求 S2AG API（旧版本问题） | 已修复，当前版本 skip 时不发起网络请求 |
| 重新运行跳过所有文件 | state.json 记录了已完成文件 | 删除 `state.json` 可强制重跑所有文件 |
