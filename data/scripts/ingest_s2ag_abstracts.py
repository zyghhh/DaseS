"""
S2AG 全量摘要下载与合并脚本

功能:
  1. 从 Semantic Scholar Academic Graph (S2AG) API 拉取最新 abstracts 数据集文件列表
  2. 流式下载各分片 gzip 文件到远程服务器 /data/S2AG_ABSTRACT/raw/（断点续传）
  3. 逐行解析 JSON，按批次更新 Elasticsearch 中对应 DOI 的摘要字段
  4. 使用状态文件记录已处理文件，支持中断后断点续传

注意:
  - 脚本设计为在远程服务器 49.52.27.139 上直接运行
  - 无需依赖 backend 包，所有配置可通过环境变量或 CLI 参数覆盖
  - ES 仅更新 abstract 为 NULL 的文档（已有摘要的不覆盖）

用法:
    # 完整运行（下载 + 入库）
    python ingest_s2ag_abstracts.py

    # 仅下载，不入库（适合网络好时预下载）
    python ingest_s2ag_abstracts.py --download-only

    # 跳过下载，仅处理已有文件
    python ingest_s2ag_abstracts.py --skip-download

    # 测试模式：处理前 N 条记录
    python ingest_s2ag_abstracts.py --limit 10000

    # 强制覆盖已有摘要
    python ingest_s2ag_abstracts.py --overwrite

    # 自定义输出目录和 ES 地址
    python ingest_s2ag_abstracts.py --output-dir /data/S2AG_ABSTRACT --es-host http://localhost:9200
"""

import argparse
import gzip
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import requests
from elasticsearch import Elasticsearch, NotFoundError
from elasticsearch.helpers import bulk

# ---------------------------------------------------------------------------
# 默认配置（均可通过环境变量或 CLI 参数覆盖）
# ---------------------------------------------------------------------------

DEFAULT_S2AG_API_KEY: str = os.environ.get(
    "S2AG_API_KEY", ""
)
DEFAULT_ES_HOST: str = os.environ.get("ES_HOST", "http://49.52.27.139:9200")
DEFAULT_ES_ALIAS: str = os.environ.get("ES_ALIAS", "dblp_search")
DEFAULT_OUTPUT_DIR: str = os.environ.get("S2AG_OUTPUT_DIR", "/data/S2AG_ABSTRACT")

# S2AG API 端点
S2AG_DATASET_URL = (
    "https://api.semanticscholar.org/datasets/v1/release/latest/dataset/abstracts"
)

# 下载连接超时（秒）
DOWNLOAD_TIMEOUT: int = 600
# 每次 ES 搜索的 DOI 批量大小
SEARCH_BATCH_SIZE: int = 500
# 每次 ES bulk 写入大小
BULK_BATCH_SIZE: int = 500
# 进度打印间隔（处理记录数）
LOG_INTERVAL: int = 100_000


# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------


def _now_str() -> str:
    """返回当前 UTC 时间字符串，用于日志前缀。"""
    return datetime.now(tz=timezone.utc).strftime("%H:%M:%S")


def _log(tag: str, msg: str) -> None:
    """统一日志格式。"""
    print(f"[{_now_str()}][{tag}] {msg}", flush=True)


# ---------------------------------------------------------------------------
# Step 0: 获取文件列表
# ---------------------------------------------------------------------------


def fetch_file_list(api_key: str) -> list[dict]:
    """从 S2AG API 获取最新 release 的 abstracts 数据集下载列表。

    Args:
        api_key: Semantic Scholar API 密钥。

    Returns:
        文件信息列表，每项包含 'url' 等字段。
    """
    headers = {"x-api-key": api_key}
    _log("fetch", f"正在请求文件列表: {S2AG_DATASET_URL}")
    try:
        resp = requests.get(S2AG_DATASET_URL, headers=headers, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        _log("fetch", f"❌ 请求失败: {e}")
        sys.exit(1)

    data = resp.json()
    files: list[dict] = data.get("files", [])
    release_id: str = data.get("release_id", "unknown")
    _log("fetch", f"Release ID: {release_id}，共 {len(files)} 个分片文件")
    return files


# ---------------------------------------------------------------------------
# Step 1: 下载分片文件（流式，断点续传）
# ---------------------------------------------------------------------------


def download_files(
    files: list[dict],
    raw_dir: Path,
    api_key: str,
    max_files: int = 0,
    rate_limit_secs: float = 1.0,
) -> list[Path]:
    """流式下载所有 S2AG abstracts 分片文件。

    已存在且大小大于 0 的文件自动跳过（断点续传）。
    相邻两次实际下载请求之间强制等待 rate_limit_secs 秒，遵守 S2AG 限速。

    Args:
        files: S2AG API 返回的文件信息列表。
        raw_dir: 本地存储目录。
        api_key: Semantic Scholar API 密钥。
        max_files: 最多下载文件数（0=全量）。
        rate_limit_secs: 两次下载请求间最小间隔秒数（默认 1.0）。

    Returns:
        所有本地文件路径列表（按顺序）。
    """
    raw_dir.mkdir(parents=True, exist_ok=True)
    local_paths: list[Path] = []

    total = len(files)
    # max_files 限制实际处理的文件数
    if max_files > 0:
        files = files[:max_files]
        _log("download", f"--max-files={max_files}，仅处理前 {max_files}/{total} 个文件")
    total = len(files)

    last_download_time: float = 0.0  # 上次实际发起下载的时间戳

    for idx, file_info in enumerate(files, start=1):
        # S2AG API 返回值可能是字符串 URL 或含 'url' 字段的字典
        if isinstance(file_info, str):
            url = file_info
        else:
            url = file_info.get("url", "")
        if not url:
            _log("download", f"⚠️  第 {idx} 个文件 url 为空，跳过。")
            continue

        # 从 URL 中提取文件名（取最后一段路径，去掉 query string）
        raw_name = url.split("?")[0].rstrip("/").split("/")[-1]
        # 若文件名不含分片序号前缀，补充索引号
        if not raw_name:
            raw_name = f"part-{idx:05d}.jsonl.gz"

        local_path = raw_dir / raw_name
        local_paths.append(local_path)

        if local_path.exists() and local_path.stat().st_size > 0:
            _log(
                "download",
                f"[{idx}/{total}] 已存在，跳过: {local_path.name} "
                f"({local_path.stat().st_size // 1024 // 1024} MB)",
            )
            continue

        # ---- 限速：距上次实际下载不足 rate_limit_secs 秒则等待 ----
        now = time.time()
        wait = rate_limit_secs - (now - last_download_time)
        if wait > 0 and last_download_time > 0:
            _log("download", f"  限速等待 {wait:.2f}s ...")
            time.sleep(wait)

        _log("download", f"[{idx}/{total}] 开始下载: {raw_name}")
        tmp_path = local_path.with_suffix(local_path.suffix + ".tmp")
        last_download_time = time.time()

        try:
            with requests.get(
                url, stream=True, timeout=DOWNLOAD_TIMEOUT
            ) as resp:
                resp.raise_for_status()
                total_bytes = int(resp.headers.get("content-length", 0))
                downloaded = 0
                with open(tmp_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=256 * 1024):
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total_bytes > 0:
                            pct = downloaded / total_bytes * 100
                            print(
                                f"\r  {pct:5.1f}%  "
                                f"{downloaded // 1024 // 1024}MB"
                                f" / {total_bytes // 1024 // 1024}MB",
                                end="",
                                flush=True,
                            )
            print()
            tmp_path.rename(local_path)
            _log(
                "download",
                f"[{idx}/{total}] ✅ 下载完成: {local_path.name} "
                f"({local_path.stat().st_size // 1024 // 1024} MB)",
            )
        except Exception as e:
            _log("download", f"❌ 下载失败: {raw_name} — {e}")
            if tmp_path.exists():
                tmp_path.unlink()
            # 下载失败不中止整体流程，记录后继续
            local_paths[-1] = None  # type: ignore[assignment]

    return [p for p in local_paths if p is not None]


# ---------------------------------------------------------------------------
# Step 2: 读取状态文件（断点续传）
# ---------------------------------------------------------------------------


def _state_path(output_dir: Path) -> Path:
    """返回状态文件路径。"""
    return output_dir / "state.json"


def load_state(output_dir: Path) -> set[str]:
    """从状态文件加载已完成处理的文件名集合。

    Args:
        output_dir: 输出根目录。

    Returns:
        已完成的文件名（不含路径）集合。
    """
    sp = _state_path(output_dir)
    if sp.exists():
        try:
            data = json.loads(sp.read_text())
            done: set[str] = set(data.get("done", []))
            _log("state", f"断点续传：已完成 {len(done)} 个文件")
            return done
        except Exception:
            pass
    return set()


def save_state(output_dir: Path, done: set[str]) -> None:
    """将已完成文件列表写入状态文件。

    Args:
        output_dir: 输出根目录。
        done: 已完成的文件名集合。
    """
    sp = _state_path(output_dir)
    sp.write_text(
        json.dumps({"done": sorted(done), "updated_at": _now_str()}, ensure_ascii=False)
    )


# ---------------------------------------------------------------------------
# Step 3: 解析 jsonl.gz 文件，生成 (doi, abstract) 对
# ---------------------------------------------------------------------------


def iter_abstracts(local_path: Path) -> Iterator[tuple[str, str]]:
    """流式解析 S2AG abstracts gzip 文件，逐行 yield (doi, abstract)。

    只 yield DOI 和摘要均非空的记录。

    Args:
        local_path: 本地 .jsonl.gz 文件路径。

    Yields:
        (doi, abstract) 元组。
    """
    with gzip.open(local_path, "rb") as gz:
        for raw_line in gz:
            line = raw_line.strip()
            if not line:
                continue
            try:
                record: dict = json.loads(line)
            except json.JSONDecodeError:
                continue

            abstract: str | None = record.get("abstract")
            if not abstract or not abstract.strip():
                continue

            external_ids: dict = record.get("externalids") or {}
            doi: str | None = external_ids.get("DOI")
            if doi and doi.strip():
                yield doi.strip(), abstract.strip()


# ---------------------------------------------------------------------------
# Step 4: 批量查询 ES，找出需要更新的文档
# ---------------------------------------------------------------------------


def _search_by_dois(
    es: Elasticsearch,
    alias: str,
    dois: list[str],
    overwrite: bool,
) -> list[dict]:
    """用 terms 查询在 ES 中找到对应 DOI 的文档，返回需要更新的文档列表。

    Args:
        es: Elasticsearch 客户端。
        alias: ES 别名/索引名。
        dois: DOI 列表。
        overwrite: 是否强制覆盖已有摘要。

    Returns:
        需要更新的文档列表，每项含 '_id', 'doi'。
    """
    if overwrite:
        # 覆盖模式：找所有匹配 DOI 的文档
        query: dict = {"terms": {"doi": dois}}
    else:
        # 默认模式：只找 abstract 为空的文档
        query = {
            "bool": {
                "must": {"terms": {"doi": dois}},
                "must_not": {"exists": {"field": "abstract"}},
            }
        }

    try:
        resp = es.search(
            index=alias,
            query=query,
            _source=["doi"],
            size=len(dois) + 10,  # 多留一些余量
        )
    except Exception as e:
        _log("es", f"⚠️  搜索失败: {e}")
        return []

    results: list[dict] = []
    for hit in resp["hits"]["hits"]:
        results.append({
            "_id": hit["_id"],
            "doi": hit["_source"].get("doi", ""),
        })
    return results


# ---------------------------------------------------------------------------
# Step 5: 批量更新 ES 摘要字段
# ---------------------------------------------------------------------------


def update_abstracts_batch(
    es: Elasticsearch,
    alias: str,
    doi_abstract_map: dict[str, str],
    overwrite: bool,
) -> tuple[int, int]:
    """将一批 DOI->摘要 映射更新到 ES。

    1. 按 DOI 搜索匹配的文档（abstract 为空 或 overwrite 模式）
    2. 构造 bulk update actions
    3. 执行 bulk 写入

    Args:
        es: Elasticsearch 客户端。
        alias: ES 别名/索引名。
        doi_abstract_map: {doi: abstract} 字典。
        overwrite: 是否强制覆盖已有摘要。

    Returns:
        (成功数, 跳过/失败数)
    """
    dois = list(doi_abstract_map.keys())
    matched_docs = _search_by_dois(es, alias, dois, overwrite)

    if not matched_docs:
        return 0, 0

    actions: list[dict] = []
    for doc in matched_docs:
        doc_doi: str = doc["doi"]
        abstract = doi_abstract_map.get(doc_doi)
        if not abstract:
            continue
        actions.append({
            "_op_type": "update",
            "_index": alias,
            "_id": doc["_id"],
            "doc": {
                "abstract": abstract,
                "abstract_source": "S2AG",
            },
        })

    if not actions:
        return 0, 0

    ok, errors = bulk(
        es,
        actions,
        raise_on_error=False,
        raise_on_exception=False,
    )
    err_count = len(errors) if isinstance(errors, list) else 0
    if errors and err_count > 0:
        for err in errors[:2]:
            _log("bulk", f"  错误示例: {err}")

    return ok, err_count


# ---------------------------------------------------------------------------
# 核心处理流程：处理单个文件
# ---------------------------------------------------------------------------


def process_file(
    es: Elasticsearch,
    alias: str,
    local_path: Path,
    overwrite: bool,
    limit: int,
    total_processed_so_far: int,
) -> tuple[int, int, int]:
    """处理单个 S2AG abstracts 分片文件。

    Args:
        es: Elasticsearch 客户端。
        alias: ES 别名/索引名。
        local_path: 本地文件路径。
        overwrite: 是否覆盖已有摘要。
        limit: 全局最大处理记录数（0=不限）。
        total_processed_so_far: 截至当前文件前已处理的有效记录总数。

    Returns:
        (本文件有效记录数, 本文件更新成功数, 本文件更新失败数)
    """
    _log("process", f"开始处理: {local_path.name}")
    file_start = time.time()

    doi_batch: dict[str, str] = {}
    file_valid = 0
    file_ok = 0
    file_err = 0
    global_count = total_processed_so_far

    for doi, abstract in iter_abstracts(local_path):
        doi_batch[doi] = abstract
        file_valid += 1
        global_count += 1

        if len(doi_batch) >= BULK_BATCH_SIZE:
            ok, err = update_abstracts_batch(es, alias, doi_batch, overwrite)
            file_ok += ok
            file_err += err
            doi_batch.clear()

        if file_valid % LOG_INTERVAL == 0:
            elapsed = time.time() - file_start
            rate = file_valid / elapsed if elapsed > 0 else 0
            _log(
                "process",
                f"  文件进度: {file_valid:,} 条有效 | 已更新 {file_ok:,} | "
                f"速率 {rate:,.0f} 条/秒",
            )

        if 0 < limit <= global_count:
            _log("process", f"  已达全局限制 {limit:,} 条，停止处理。")
            break

    # 处理最后一批
    if doi_batch:
        ok, err = update_abstracts_batch(es, alias, doi_batch, overwrite)
        file_ok += ok
        file_err += err
        doi_batch.clear()

    elapsed = time.time() - file_start
    _log(
        "process",
        f"✅ 文件完成: {local_path.name} | "
        f"有效记录 {file_valid:,} | "
        f"ES更新成功 {file_ok:,} | 失败 {file_err:,} | "
        f"耗时 {elapsed:.1f}s",
    )
    return file_valid, file_ok, file_err


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------


def run(
    api_key: str,
    output_dir: Path,
    es_host: str,
    es_alias: str,
    download_only: bool,
    skip_download: bool,
    overwrite: bool,
    limit: int,
    max_files: int = 0,
    rate_limit_secs: float = 1.0,
) -> None:
    """执行 S2AG 摘要全量下载与入库流水线。

    Args:
        api_key: Semantic Scholar API 密钥。
        output_dir: 本地输出根目录（原始文件存入 output_dir/raw/）。
        es_host: Elasticsearch 地址。
        es_alias: ES 别名/索引名。
        download_only: 仅下载，不写 ES。
        skip_download: 跳过下载步骤，直接处理已有文件。
        overwrite: 是否覆盖 ES 中已有的摘要。
        limit: 全局最大处理记录数（0=不限）。
        max_files: 最多下载/处理的文件数（0=全量）。
        rate_limit_secs: 两次下载请求间最小间隔秒数。
    """
    raw_dir = output_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 65)
    print("  S2AG 全量摘要下载与入库流水线")
    print(f"  API Key:    {api_key[:8]}...{api_key[-4:]}")
    print(f"  输出目录:   {output_dir}")
    print(f"  ES 地址:    {es_host}")
    print(f"  ES 别名:    {es_alias}")
    print(f"  覆盖模式:   {'是' if overwrite else '否（仅填充空摘要）'}")
    print(f"  处理上限:   {'全量' if limit == 0 else f'{limit:,} 条'}")
    print(f"  文件限制:   {'全量' if max_files == 0 else f'{max_files} 个分片'}")
    print(f"  限速间隔:   {rate_limit_secs}s / 次")
    print("=" * 65)
    pipeline_start = time.time()

    # --- Step 0: 获取文件列表 ---
    files = fetch_file_list(api_key)
    if not files:
        _log("run", "❌ 未获取到任何文件，退出。")
        sys.exit(1)

    # --- Step 1: 下载 ---
    if skip_download:
        # 直接扫描 raw_dir 中已有文件
        local_paths: list[Path] = sorted(raw_dir.glob("*.gz"))
        _log("run", f"跳过下载，扫描到 {len(local_paths)} 个本地文件")
    else:
        local_paths = download_files(files, raw_dir, api_key, max_files=max_files, rate_limit_secs=rate_limit_secs)

    if not local_paths:
        _log("run", "❌ 无可用本地文件，退出。")
        sys.exit(1)

    if download_only:
        _log("run", "✅ --download-only 模式：下载完成，跳过入库。")
        return

    # --- Step 2: 连接 ES ---
    _log("run", f"连接 Elasticsearch: {es_host}")
    es = Elasticsearch(hosts=[es_host], request_timeout=60)
    try:
        info = es.info()
        _log("run", f"ES 连接成功，版本: {info['version']['number']}")
    except Exception as e:
        _log("run", f"❌ ES 连接失败: {e}")
        sys.exit(1)

    # 验证别名存在
    try:
        es.indices.get_alias(name=es_alias)
        _log("run", f"ES 别名 '{es_alias}' 验证通过")
    except NotFoundError:
        _log("run", f"⚠️  别名 '{es_alias}' 不存在，尝试直接使用该名称作为索引名...")

    # --- Step 3: 断点续传 ---
    done_files = load_state(output_dir)

    # --- Step 4: 逐文件处理 ---
    total_files = len(local_paths)
    total_valid = 0
    total_ok = 0
    total_err = 0
    global_processed = 0

    for file_idx, local_path in enumerate(local_paths, start=1):
        fname = local_path.name

        if fname in done_files:
            _log("run", f"[{file_idx}/{total_files}] 已处理，跳过: {fname}")
            continue

        _log("run", f"[{file_idx}/{total_files}] 处理文件: {fname}")

        try:
            file_valid, file_ok, file_err = process_file(
                es=es,
                alias=es_alias,
                local_path=local_path,
                overwrite=overwrite,
                limit=limit,
                total_processed_so_far=global_processed,
            )
        except Exception as e:
            _log("run", f"❌ 处理文件时发生异常: {fname} — {e}")
            # 记录异常但继续下一个文件
            continue

        total_valid += file_valid
        total_ok += file_ok
        total_err += file_err
        global_processed += file_valid

        # 标记为已完成并保存状态
        done_files.add(fname)
        save_state(output_dir, done_files)

        # 全局限制检查
        if 0 < limit <= global_processed:
            _log("run", f"已达全局处理上限 {limit:,} 条，终止。")
            break

    es.close()

    # --- 最终报告 ---
    elapsed = time.time() - pipeline_start
    print("\n" + "=" * 65)
    print("  ✅ S2AG 摘要入库完成！")
    print(f"  处理文件数:     {len(done_files)}/{total_files}")
    print(f"  有效记录总数:   {total_valid:,}")
    print(f"  ES 更新成功:    {total_ok:,}")
    print(f"  ES 更新失败:    {total_err:,}")
    print(f"  总耗时:         {elapsed:.1f}s ({elapsed / 3600:.2f}h)")
    print("=" * 65)


# ---------------------------------------------------------------------------
# CLI 入口
# ---------------------------------------------------------------------------


def main() -> None:
    """命令行入口。"""
    parser = argparse.ArgumentParser(
        description="S2AG 全量摘要下载与 Elasticsearch 摘要回填脚本",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--api-key",
        default=DEFAULT_S2AG_API_KEY,
        help=f"Semantic Scholar API Key（默认使用内置密钥或环境变量 S2AG_API_KEY）",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help=f"原始文件下载目录（默认 {DEFAULT_OUTPUT_DIR}）",
    )
    parser.add_argument(
        "--es-host",
        default=DEFAULT_ES_HOST,
        help=f"Elasticsearch 地址（默认 {DEFAULT_ES_HOST}）",
    )
    parser.add_argument(
        "--es-alias",
        default=DEFAULT_ES_ALIAS,
        help=f"ES 别名或索引名（默认 {DEFAULT_ES_ALIAS}）",
    )
    parser.add_argument(
        "--download-only",
        action="store_true",
        help="仅下载文件，不写入 Elasticsearch",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="跳过下载步骤，直接处理 output-dir/raw/ 中已有文件",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="强制覆盖 ES 中已有的摘要（默认仅填充空摘要）",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="全局最大处理记录数（0=不限，用于测试）",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=0,
        help="最多下载/处理的分片文件数（0=全量；测试时建议设为 1）",
    )
    parser.add_argument(
        "--rate-limit",
        type=float,
        default=1.0,
        help="两次文件下载请求之间的最小等待秒数（默认 1.0，遵守 S2AG 限速）",
    )

    args = parser.parse_args()

    if args.download_only and args.skip_download:
        parser.error("--download-only 和 --skip-download 不能同时使用")

    run(
        api_key=args.api_key,
        output_dir=Path(args.output_dir),
        es_host=args.es_host,
        es_alias=args.es_alias,
        download_only=args.download_only,
        skip_download=args.skip_download,
        overwrite=args.overwrite,
        limit=args.limit,
        max_files=args.max_files,
        rate_limit_secs=args.rate_limit,
    )


if __name__ == "__main__":
    main()
