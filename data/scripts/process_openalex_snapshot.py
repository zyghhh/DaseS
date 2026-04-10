import os
import json
import gzip
import subprocess
from pathlib import Path
from elasticsearch import Elasticsearch, helpers
from tqdm import tqdm
import time

# --- 配置区 ---
ES_HOST = "http://49.52.27.139:9200"
ES_ALIAS = "dblp_search"
REMOTE_DIR = "/data/openalex"
REMOTE_USER_HOST = "zyg@49.52.27.139"  # 请确保已配置 SSH 免密登录
LOCAL_TMP_DIR = Path("./tmp_openalex")
MANIFEST_URL = "s3://openalex/data/works/manifest"
S3_PREFIX = "s3://openalex/"
BATCH_SIZE = 10 # 每次下载处理的分片数量
ES_FLUSH_SIZE = 500 # 累积多少条命中后向 ES 发送一次批量更新请求

# 139 服务器 ES 连接
try:
    es = Elasticsearch(hosts=[ES_HOST], request_timeout=120)
    es.info()
    print(f"成功连接 Elasticsearch: {ES_HOST}")
except Exception as e:
    print(f"❌ ES 连接失败: {e}")
    exit(1)

# 全局内存字典：存放 { "10.xxx": "es_id" }
# 只缓存有 DOI 且无摘要的文档
MISSING_DOIS = {}
TOTAL_UPDATED = 0

def load_missing_dois_from_es():
    """从 ES 滚动拉取所有有 DOI 但无摘要的文档 ID"""
    print("正在从 ES 拉取待补全的 DOI 列表（可能需要几分钟）...")
    query = {
        "query": {
            "bool": {
                "must": [{"exists": {"field": "doi"}}],
                "must_not": [{"exists": {"field": "abstract_source"}}]
            }
        },
        "_source": ["doi"]
    }
    
    try:
        resp = es.search(index=ES_ALIAS, body=query, scroll='5m', size=10000)
        scroll_id = resp['_scroll_id']
        total_hits = resp['hits']['total']['value']
        print(f"ES 中共有 {total_hits:,} 条待补全记录。")
        
        with tqdm(total=total_hits, desc="拉取 DOI") as pbar:
            while True:
                hits = resp['hits']['hits']
                if not hits: break
                
                for hit in hits:
                    doi = hit['_source'].get('doi', '').lower().strip()
                    if doi:
                        MISSING_DOIS[doi] = hit['_id']
                
                pbar.update(len(hits))
                resp = es.scroll(scroll_id=scroll_id, scroll='5m')
                scroll_id = resp['_scroll_id']
                
        es.clear_scroll(scroll_id=scroll_id)
        print(f"✅ 成功加载 {len(MISSING_DOIS):,} 个不重复的待补全 DOI 到内存。")
    except Exception as e:
        print(f"❌ 拉取待补全 DOI 失败: {e}")
        exit(1)

def reconstruct_abstract(inverted_index):
    """还原摘要"""
    if not inverted_index: return None
    max_idx = 0
    for pos in inverted_index.values():
        if pos: max_idx = max(max_idx, max(pos))
    words = [""] * (max_idx + 1)
    for word, pos in inverted_index.items():
        for p in pos: words[p] = word
    return " ".join(words).strip()

def get_s3_files():
    """获取所有 works 分片列表（按时间倒序排列，优先处理最新数据）"""
    print("正在获取 S3 文件清单...")
    cmd = ["aws", "s3", "cp", MANIFEST_URL, "-", "--no-sign-request"]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        manifest = json.loads(result.stdout)
        
        # 提取路径
        files = [entry['url'].replace(S3_PREFIX, "") for entry in manifest['entries']]
        
        # OpenAlex 的路径带有 updated_date=YYYY-MM-DD，可以直接按字符串降序排序
        # 这样就能从 2024 年（最新数据）开始倒着往下刷，命中率极高
        files.sort(reverse=True)
        
        return files
    except Exception as e:
        print(f"❌ 获取文件清单失败 (请检查 awscli 是否已安装): {e}")
        exit(1)

def download_file(s3_path, local_path):
    print(f"正在下载: {s3_path}")
    cmd = ["aws", "s3", "cp", f"{S3_PREFIX}{s3_path}", str(local_path), "--no-sign-request"]
    subprocess.run(cmd, check=True)

def transfer_and_cleanup(local_path):
    """处理完成后，传输到 139 服务器并删除本地文件"""
    print(f"传输 {local_path.name} 到服务器...")
    subprocess.run(["ssh", REMOTE_USER_HOST, f"mkdir -p {REMOTE_DIR}"], check=False)
    subprocess.run(["scp", str(local_path), f"{REMOTE_USER_HOST}:{REMOTE_DIR}/"], check=True)
    local_path.unlink()
    print(f"已删除本地文件: {local_path.name}")

def flush_updates_to_es(updates):
    """将命中的摘要批量写入 ES"""
    global TOTAL_UPDATED
    if not updates: return
    
    try:
        success, _ = helpers.bulk(es, updates, raise_on_error=False)
        TOTAL_UPDATED += success
        print(f"  --> [ES写入] 成功插入 {success} 条摘要 (累计成功: {TOTAL_UPDATED:,})")
    except Exception as e:
        print(f"  --> [ES写入报错]: {e}")

def process_file(file_path):
    """本地字典极速匹配，命中的推送到 ES"""
    print(f"开始处理分片: {file_path.name}")
    updates = []
    matched_in_this_file = 0
    
    try:
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    doi = data.get("doi")
                    
                    if not doi: continue
                    doi_clean = doi.replace("https://doi.org/", "").lower().strip()
                    
                    # 🔥 反向精准匹配核心：直接查字典
                    if doi_clean in MISSING_DOIS:
                        inverted_index = data.get("abstract_inverted_index")
                        if inverted_index:
                            abstract = reconstruct_abstract(inverted_index)
                            if abstract:
                                es_id = MISSING_DOIS[doi_clean]
                                updates.append({
                                    "_op_type": "update",
                                    "_index": ES_ALIAS,
                                    "_id": es_id,
                                    "doc": {
                                        "abstract": abstract,
                                        "abstract_source": "OpenAlex_Snapshot"
                                    }
                                })
                                matched_in_this_file += 1
                                # 从字典中剔除已找到的，防止后续分片重复更新浪费性能
                                del MISSING_DOIS[doi_clean]

                    if len(updates) >= ES_FLUSH_SIZE:
                        flush_updates_to_es(updates)
                        updates = []
                        
                except json.JSONDecodeError:
                    continue
                    
        if updates:
            flush_updates_to_es(updates)
            
        print(f"✅ 分片 {file_path.name} 扫描完毕，命中 {matched_in_this_file:,} 条。待补全剩余: {len(MISSING_DOIS):,} 条")
        
    except Exception as e:
        print(f"处理文件 {file_path.name} 时出错: {e}")

def main():
    LOCAL_TMP_DIR.mkdir(exist_ok=True)
    
    # 1. 预热内存字典
    load_missing_dois_from_es()
    if not MISSING_DOIS:
        print("🎉 ES 中没有需要补全的记录，程序退出。")
        return

    all_files = get_s3_files()
    total_files = len(all_files)
    print(f"共发现 {total_files} 个分片文件。")
    
    processed_log = Path("openalex_processed.log")
    processed_files = set()
    if processed_log.exists():
        with open(processed_log, 'r') as f:
            processed_files = set(f.read().splitlines())
    
    for i in range(0, total_files, BATCH_SIZE):
        current_batch = all_files[i:i+BATCH_SIZE]
        local_files = []
        
        # 下载阶段
        for s3_path in current_batch:
            if s3_path in processed_files:
                print(f"跳过已处理分片: {s3_path}")
                continue
            
            safe_name = s3_path.replace("/", "_").replace("data_works_", "")
            local_path = LOCAL_TMP_DIR / safe_name
            download_file(s3_path, local_path)
            local_files.append((s3_path, local_path))
        
        if not local_files: continue
        
        print(f"\n正在处理第 {i//BATCH_SIZE + 1} 批次分片...")
        for s3_path, lp in tqdm(local_files):
            process_file(lp)
            transfer_and_cleanup(lp)
            
            with open(processed_log, 'a') as f:
                f.write(s3_path + '\n')
            
            # 如果字典空了，说明全部找齐了，提前下班
            if not MISSING_DOIS:
                print(f"\n🎉 所有待补全的 {TOTAL_UPDATED:,} 条记录均已找齐！提前结束流水线。")
                return

if __name__ == "__main__":
    main()