"""
DBLP RDF NT → Neo4j CSV 批量导入脚本

将 dblp-YYYY-MM-DD.nt.gz 流式解析为 Neo4j admin import 所需的 CSV 文件。

设计原则:
  - 零内存图加载：逐行读取，不使用 rdflib，纯正则/字符串处理。
  - 单次流式扫描：按 subject 分组缓冲，flush 时写 CSV。
  - CCF 等级匹配：从 ccf_catalogue.json 查 conf/xxx 或 journals/xxx。

节点 CSV（Neo4j admin import 标准格式）:
  nodes_work.csv         — Work（论文）
  nodes_author.csv       — Author（作者）
  nodes_venue.csv        — Venue（期刊/会议）
  nodes_concept.csv      — Concept（占位，待 OpenAlex 填充）
  nodes_institution.csv  — Institution（占位，待外部数据填充）

关系 CSV:
  rels_authored.csv        — (Author)-[:AUTHORED {position}]->(Work)
  rels_published_in.csv    — (Work)-[:PUBLISHED_IN]->(Venue)
  rels_cites.csv           — 占位，待 S2AG/OpenAlex 填充
  rels_has_topic.csv       — 占位，待 OpenAlex 填充
  rels_concept_parent.csv  — 占位
  rels_affiliated_with.csv — 占位

用法:
    # 测试模式（处理前 200 万行）
    python data/scripts/ingest_dblp_neo4j.py --limit 2000000

    # 全量导入
    python data/scripts/ingest_dblp_neo4j.py

    # 指定路径
    python data/scripts/ingest_dblp_neo4j.py \\
        --input data/raw/dblp-2026-04-01.nt.gz \\
        --output-dir data/processed/neo4j/

完成后按提示执行 neo4j-admin 命令导入 Neo4j。
"""

import argparse
import csv
import gc
import gzip
import json
import re
import sys
import time
from collections import Counter
from pathlib import Path
from typing import IO

# ---------------------------------------------------------------------------
# 路径常量
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT = REPO_ROOT / "data" / "raw" / "dblp-2026-04-01.nt.gz"
DEFAULT_OUTPUT = REPO_ROOT / "data" / "processed" / "neo4j"
DEFAULT_CCF_MAP = REPO_ROOT / "data" / "raw" / "ccf_catalogue.json"

# ---------------------------------------------------------------------------
# NT 行解析（纯正则，不依赖 rdflib）
# ---------------------------------------------------------------------------

# 匹配三种词项形式
_IRI_RE = re.compile(r"^<([^>]*)>")
_BNODE_RE = re.compile(r"^(_:\S+)")
_LITERAL_RE = re.compile(
    r'^"((?:[^"\\]|\\.)*)"'          # 引号内的字面值（支持转义）
    r"(?:@[\w-]+|\^\^<[^>]+>)?"      # 可选语言标签或类型标注
)

# 快速跳过不需要解析的行（空行 / 注释）
_SKIP_RE = re.compile(r"^\s*(?:#|$)")


def _parse_term(s: str) -> tuple[str, str]:
    """从字符串 s 开头解析一个 NT 词项，返回 (值, 剩余字符串)。

    Args:
        s: 待解析的字符串。

    Returns:
        (词项值, 解析后剩余字符串)

    Raises:
        ValueError: 无法识别的词项格式。
    """
    s = s.lstrip()
    m = _IRI_RE.match(s)
    if m:
        return m.group(1), s[m.end():]
    m = _BNODE_RE.match(s)
    if m:
        return m.group(1), s[m.end():]
    m = _LITERAL_RE.match(s)
    if m:
        # 反转义：将 \" \\ \n \t 还原
        raw = m.group(1)
        raw = raw.replace('\\"', '"').replace("\\\\", "\\").replace("\\n", "\n").replace("\\t", "\t")
        return raw, s[m.end():]
    raise ValueError(f"无法解析词项: {s[:80]!r}")


def parse_nt_line(line: str) -> tuple[str, str, str] | None:
    """解析单行 NT 三元组，返回 (subject, predicate, object)。

    Args:
        line: 原始行字符串。

    Returns:
        (subject, predicate, object) 或 None（跳过行）。
    """
    if _SKIP_RE.match(line):
        return None
    line = line.rstrip()
    if not line.endswith("."):
        return None
    line = line[:-1].rstrip()  # 去掉末尾 ' .'
    if not line:
        return None
    try:
        subj, rest = _parse_term(line)
        pred, rest = _parse_term(rest)
        obj, _ = _parse_term(rest)
        return subj, pred, obj
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# 谓词映射表（完整 URI → 短名）
# ---------------------------------------------------------------------------

PRED_MAP: dict[str, str] = {
    # DBLP schema
    "https://dblp.org/rdf/schema#title":               "title",
    "https://dblp.org/rdf/schema#yearOfPublication":   "year",
    "https://dblp.org/rdf/schema#doi":                 "doi",
    "https://dblp.org/rdf/schema#authoredBy":          "authoredBy",
    "https://dblp.org/rdf/schema#hasSignature":        "hasSignature",
    "https://dblp.org/rdf/schema#publishedIn":         "publishedIn",
    "https://dblp.org/rdf/schema#publishedInBook":     "publishedInBook",
    "https://dblp.org/rdf/schema#publishedInStream":   "publishedInStream",
    "https://dblp.org/rdf/schema#listedOnTocPage":     "listedOnTocPage",
    "https://dblp.org/rdf/schema#pagination":          "pagination",
    "https://dblp.org/rdf/schema#numberOfCreators":    "numCreators",
    "https://dblp.org/rdf/schema#signatureCreator":    "sigCreator",
    "https://dblp.org/rdf/schema#signatureOrdinal":    "sigOrdinal",
    "https://dblp.org/rdf/schema#signaturePublication": "sigPub",
    "https://dblp.org/rdf/schema#signatureDblpName":   "sigName",
    "https://dblp.org/rdf/schema#bibtexType":          "bibtexType",
    "https://dblp.org/rdf/schema#primaryCreatorName":  "primaryName",
    # RDF / OWL / FOAF
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#type": "rdfType",
    "http://xmlns.com/foaf/0.1/name":                  "foafName",
    "http://www.w3.org/2002/07/owl#sameAs":            "sameAs",
    "http://www.w3.org/2000/01/rdf-schema#label":      "rdfsLabel",
}

# ---------------------------------------------------------------------------
# 工具：venue_key 提取
# ---------------------------------------------------------------------------

# streams URI:  https://dblp.org/streams/journals/tpami  → journals/tpami
_STREAM_RE = re.compile(r"dblp\.org/streams/((conf|journals)/[^/]+)")
# TocPage URI:  https://dblp.org/db/conf/cvpr/cvpr2023   → conf/cvpr
_TOC_RE = re.compile(r"dblp\.org/db/((conf|journals)/[^/]+)")
# rec URI key:  https://dblp.org/rec/conf/cvpr/Smith23   → conf/cvpr
_REC_KEY_RE = re.compile(r"dblp\.org/rec/((conf|journals)/[^/]+)")
# DOI 提取:    https://doi.org/10.1000/123               → 10.1000/123
_DOI_RE = re.compile(r"doi\.org/(.+)")


def _extract_venue_key(uri: str) -> str | None:
    """从 URI 中提取 'conf/xxx' 或 'journals/xxx' 形式的 venue_key。

    Args:
        uri: 任意 URI 字符串。

    Returns:
        venue_key 字符串或 None。
    """
    m = _STREAM_RE.search(uri)
    if m:
        return m.group(1)
    m = _TOC_RE.search(uri)
    if m:
        return m.group(1)
    return None


def _extract_doi_from_uri(uri: str) -> str | None:
    """从 doi.org URI 中提取 DOI 字符串。

    Args:
        uri: 可能包含 doi.org 的 URI。

    Returns:
        DOI 字符串或 None。
    """
    m = _DOI_RE.search(uri)
    return m.group(1).rstrip("/") if m else None


def _dblp_key_from_rec_uri(uri: str) -> str:
    """从 dblp.org/rec/ URI 提取 DBLP key，如 conf/cvpr/Smith23。

    Args:
        uri: rec 类型 URI。

    Returns:
        DBLP key 字符串。
    """
    idx = uri.find("/rec/")
    return uri[idx + 5:] if idx != -1 else uri


# ---------------------------------------------------------------------------
# CSV 写入辅助
# ---------------------------------------------------------------------------

SEP = "|"  # neo4j-admin import 分隔符（避免标题中的逗号）


def _csv_writer(fobj: IO) -> csv.writer:
    """创建使用 | 分隔的 CSV writer。

    Args:
        fobj: 文件对象。

    Returns:
        csv.writer 实例。
    """
    return csv.writer(fobj, delimiter=SEP, quotechar='"', quoting=csv.QUOTE_MINIMAL)


def _safe(v: object) -> str:
    """将值转换为安全字符串（替换换行符）。

    Args:
        v: 任意值。

    Returns:
        清理后的字符串。
    """
    if v is None:
        return ""
    return str(v).replace("\n", " ").replace("\r", " ")


# ---------------------------------------------------------------------------
# 主转换器
# ---------------------------------------------------------------------------


_ORCID_RE = re.compile(r"orcid\.org/([\d\-X]+)", re.IGNORECASE)


class DblpNt2Neo4j:
    """DBLP RDF NT 流 → Neo4j CSV 批量导入文件转换器。

    Attributes:
        _nt_gz_path: 输入 NT.gz 文件路径。
        _out_dir: 输出目录路径。
        _ccf_map: CCF 等级映射字典。
        _limit: 最大处理行数，0 表示全量。
        _pid_meta: 预扫描收集的 {pid_uri: {"name": ..., "orcid": ...}}。
    """

    def __init__(
        self,
        nt_gz_path: Path,
        out_dir: Path,
        ccf_map: dict[str, str],
        limit: int = 0,
    ) -> None:
        """初始化转换器。

        Args:
            nt_gz_path: DBLP NT.gz 文件路径。
            out_dir: CSV 输出目录。
            ccf_map: {venue_prefix: rank} 映射字典。
            limit: 最大处理行数（0=全量）。
        """
        self._nt_gz_path = nt_gz_path
        self._out_dir = out_dir
        self._ccf_map = ccf_map
        self._limit = limit

        # 预扫描缓存：pid_uri → {name, orcid}（在 parse() 前填充）
        self._pid_meta: dict[str, dict[str, str]] = {}

        # 已处理的去重集合
        self._seen_works: set[str] = set()
        self._seen_authors: set[str] = set()

        # Venue 缓冲区（延迟写入，以便 stream rdfsLabel 覆盖 publication 名）
        # 结构: {venue_key: {"name": str, "ccf_rank": str, "type": str,
        #                    "name_cnt": Counter, "has_stream_name": bool}}
        self._venue_buffer: dict[str, dict] = {}

        # 计数器
        self._cnt_lines = 0
        self._cnt_works = 0
        self._cnt_authors = 0
        self._cnt_authored = 0
        self._cnt_pub_in = 0

        # CSV 文件句柄（parse() 中打开）
        self._f_work: IO | None = None
        self._f_author: IO | None = None
        self._f_venue: IO | None = None
        self._f_authored: IO | None = None
        self._f_pub_in: IO | None = None

        self._w_work: csv.writer | None = None
        self._w_author: csv.writer | None = None
        self._w_venue: csv.writer | None = None
        self._w_authored: csv.writer | None = None
        self._w_pub_in: csv.writer | None = None

    # ------------------------------------------------------------------
    # CCF 等级查找
    # ------------------------------------------------------------------

    def _get_ccf_rank(self, venue_key: str) -> str:
        """从 ccf_map 中查找 venue_key 对应的 CCF 等级。

        venue_key 格式为 'conf/xxx' 或 'journals/xxx'，
        取前两段与 ccf_catalogue.json 的 key 对比。

        Args:
            venue_key: venue 路径，如 'conf/cvpr' 或 'journals/tpami'。

        Returns:
            'A'、'B'、'C' 之一，或空字符串（未匹配）。
        """
        if not venue_key:
            return ""
        parts = venue_key.split("/")
        prefix = f"{parts[0]}/{parts[1]}" if len(parts) >= 2 else venue_key
        return self._ccf_map.get(prefix, "")

    # ------------------------------------------------------------------
    # 实体处理
    # ------------------------------------------------------------------

    def _proc_signature(self, subject: str, triples: list[tuple[str, str]]) -> None:
        """处理签名空白节点，写 AUTHORED 关系。

        Args:
            subject: 空白节点 ID，格式 _:Sig_HASH_N。
            triples: 该节点的所有 (谓词短名, 对象值) 列表。
        """
        props: dict[str, str] = {}
        for pred, obj in triples:
            if pred in ("sigCreator", "sigPub", "sigOrdinal", "sigName"):
                props[pred] = obj

        creator = props.get("sigCreator", "")
        pub = props.get("sigPub", "")
        if not creator or not pub:
            return

        # 从空白节点 ID 末尾提取序号（_:Sig_HASH_N → N）
        try:
            ordinal = int(subject.rsplit("_", 1)[-1])
        except ValueError:
            ordinal = props.get("sigOrdinal", "0")
            try:
                ordinal = int(ordinal)
            except (ValueError, TypeError):
                ordinal = 0

        sig_name = props.get("sigName", "")

        # 确保 Author 节点已写入
        # 优先使用预扫描获取的 primaryCreatorName / foafName 和 ORCID
        # 若预扫描未命中（数据缺失），回退到 sig_name
        if creator not in self._seen_authors:
            self._seen_authors.add(creator)
            pid = creator.split("/pid/", 1)[-1] if "/pid/" in creator else creator
            meta = self._pid_meta.get(creator, {})
            name = meta.get("name") or sig_name  # 预扫描 > sig_name 兜底
            orcid = meta.get("orcid", "")
            self._w_author.writerow([
                _safe(creator),  # author_id:ID(Author)
                _safe(pid),      # dblp_pid
                _safe(name),     # name（预扫描 primaryCreatorName，兜底 sigName）
                _safe(orcid),    # orcid
                "",              # h_index（外部补全）
                "",              # nationality（外部补全）
            ])
            self._cnt_authors += 1

        self._w_authored.writerow([
            _safe(creator),  # :START_ID(Author)
            _safe(pub),      # :END_ID(Work)
            ordinal,         # position:int
            _safe(sig_name), # dblp_name
        ])
        self._cnt_authored += 1

    def _prepass_person_data(self) -> None:
        """第一遍快速扫描，收集所有 /pid/ 节点的 name 与 ORCID。

        扫描结果存入 self._pid_meta，供主解析阶段使用，
        使 Author 节点在首次写入（来自 Sig 空白节点）时即可填充完整信息。
        """
        print("[prepass] 开始预扫描 Person 数据（name / ORCID）…", flush=True)
        t0 = time.time()
        cnt = 0

        cur_subj = ""
        cur_name = ""
        cur_orcid = ""

        with gzip.open(self._nt_gz_path, "rt", encoding="utf-8", errors="replace") as f:
            for raw in f:
                line = raw.rstrip()
                if not line or line.startswith("#"):
                    continue

                # 只关心 /pid/ subject 的行，跳过其他所有行
                if not line.startswith("<https://dblp.org/pid/"):
                    # 若当前在 pid 块内，遇到非 pid 行说明块结束
                    if cur_subj:
                        self._pid_meta[cur_subj] = {
                            "name": cur_name,
                            "orcid": cur_orcid,
                        }
                        cnt += 1
                        cur_subj = ""
                        cur_name = ""
                        cur_orcid = ""
                    continue

                # 解析三元组
                try:
                    triple = parse_nt_line(line)
                except Exception:
                    continue
                if triple is None:
                    continue
                subj, pred_uri, obj = triple

                # subject 切换
                if subj != cur_subj:
                    if cur_subj:
                        self._pid_meta[cur_subj] = {
                            "name": cur_name,
                            "orcid": cur_orcid,
                        }
                        cnt += 1
                    cur_subj = subj
                    cur_name = ""
                    cur_orcid = ""

                short = PRED_MAP.get(pred_uri, "")
                if short == "primaryName":
                    cur_name = obj
                elif short == "foafName" and not cur_name:
                    cur_name = obj
                elif short == "sameAs" and not cur_orcid:
                    m = _ORCID_RE.search(obj)
                    if m:
                        cur_orcid = m.group(1)

        # 处理最后一个块
        if cur_subj:
            self._pid_meta[cur_subj] = {"name": cur_name, "orcid": cur_orcid}
            cnt += 1

        elapsed = time.time() - t0
        print(
            f"[prepass] 完成：{cnt:,} 个 Person，"
            f"含 ORCID {sum(1 for v in self._pid_meta.values() if v['orcid']):,} 个，"
            f"耗时 {elapsed:.1f}s",
            flush=True,
        )

    def _proc_person(self, subject: str, triples: list[tuple[str, str]]) -> None:
        """处理 Person 节点，写入未被 Sig 节点覆盖的 Author（如无论文的学者）。

        Person 节点段在文件 ~243M 行后才出现，此时大部分 Author 已由 Sig
        节点写入。_prepass_person_data() 已提前收集所有 pid 的 name/orcid，
        因此 Sig 节点写入时即可填充完整信息。

        Args:
            subject: Person URI（dblp.org/pid/...）。
            triples: 该节点的 (谓词短名, 对象值) 列表。
        """
        is_person = any(
            pred == "rdfType" and "Person" in obj for pred, obj in triples
        )
        if not is_person:
            return

        if subject in self._seen_authors:
            # 已由签名节点写入过（name/orcid 已通过预扫描填充），跳过
            return

        # 未出现在任何论文中的学者（无 Sig 节点）→ 从预扫描元数据写入
        meta = self._pid_meta.get(subject, {})
        name = meta.get("name", "")
        orcid = meta.get("orcid", "")

        self._seen_authors.add(subject)
        pid = subject.split("/pid/", 1)[-1] if "/pid/" in subject else subject
        self._w_author.writerow([
            _safe(subject),  # author_id:ID(Author)
            _safe(pid),      # dblp_pid
            _safe(name),     # name
            _safe(orcid),    # orcid
            "",              # h_index（外部补全）
            "",              # nationality（外部补全）
        ])
        self._cnt_authors += 1

    def _proc_publication(self, subject: str, triples: list[tuple[str, str]]) -> None:
        """处理 Publication 节点，写 Work 节点与 PUBLISHED_IN 关系。

        Args:
            subject: Publication URI（dblp.org/rec/...）。
            triples: 该节点的 (谓词短名, 对象值) 列表。
        """
        # 去重（同一 subject 可能有多条 rdfType 行导致多次 flush）
        if subject in self._seen_works:
            return

        props: dict[str, list[str]] = {}
        for pred, obj in triples:
            props.setdefault(pred, []).append(obj)

        # 过滤非论文类型（Publication 是超类，所有类型都带这个 type）
        # 只要不是 stream/person 就作为 Work 处理
        rdf_types = props.get("rdfType", [])
        bibtex_type = props.get("bibtexType", [""])[0]

        # 跳过 Stream 类型（单独处理）
        if any("Stream" in t for t in rdf_types):
            return

        title = props.get("title", [""])[0]
        if not title:
            return  # 无标题则跳过

        year_raw = props.get("year", [""])[0]
        try:
            year = int(year_raw)
        except (ValueError, TypeError):
            year = ""

        # DOI 提取（优先 dblp:doi 谓词，其次 sameAs 里找 doi.org）
        doi = ""
        doi_uris = props.get("doi", [])
        if doi_uris:
            doi = _extract_doi_from_uri(doi_uris[0]) or ""
        if not doi:
            for same in props.get("sameAs", []):
                d = _extract_doi_from_uri(same)
                if d and "10.48550/arXiv" not in d:
                    doi = d
                    break

        dblp_key = _dblp_key_from_rec_uri(subject)
        pages = props.get("pagination", [""])[0]

        # bibtexType 末段（Inproceedings / Article / ...）
        pub_type = bibtex_type.split("#")[-1] if bibtex_type else ""

        # venue_key 优先级：publishedInStream > listedOnTocPage
        venue_key = ""
        stream_uris = props.get("publishedInStream", [])
        if stream_uris:
            venue_key = _extract_venue_key(stream_uris[0]) or ""
        if not venue_key:
            toc_uris = props.get("listedOnTocPage", [])
            if toc_uris:
                venue_key = _extract_venue_key(toc_uris[0]) or ""
        if not venue_key:
            # 从 rec URI 自身提取（conf/xxx 或 journals/xxx）
            m = _REC_KEY_RE.search(subject)
            if m:
                venue_key = m.group(1)

        # venue 名称（字面值）
        venue_name = (
            props.get("publishedIn", [""])[0]
            or props.get("publishedInBook", [""])[0]
        )

        ccf_rank = self._get_ccf_rank(venue_key)

        # 写 Work 节点
        self._seen_works.add(subject)
        self._w_work.writerow([
            _safe(subject),    # work_id:ID(Work)
            _safe(title),      # title
            year,              # year:int
            _safe(doi),        # doi
            _safe(dblp_key),   # dblp_key
            _safe(pub_type),   # pub_type
            _safe(pages),      # pages
            _safe(ccf_rank),   # ccf_rank
            "",                # citation_count（外部补全）
        ])
        self._cnt_works += 1

        # 写 Venue 缓冲区（延迟写入，stream rdfsLabel 可在此覆盖 pub 名）
        if venue_key:
            vtype = "conf" if venue_key.startswith("conf/") else "journal"
            if venue_key not in self._venue_buffer:
                # 新增 venue 条目
                self._venue_buffer[venue_key] = {
                    "name": venue_name,
                    "ccf_rank": ccf_rank,
                    "type": vtype,
                    "name_cnt": Counter([venue_name] if venue_name else []),
                    "has_stream_name": False,
                }
            else:
                # 已存在：累积 name 计数，以便最终取最常出现的名称
                entry = self._venue_buffer[venue_key]
                if venue_name and not entry["has_stream_name"]:
                    entry["name_cnt"][venue_name] += 1
                # ccf_rank 若尚未填充则更新
                if ccf_rank and not entry["ccf_rank"]:
                    entry["ccf_rank"] = ccf_rank

            self._w_pub_in.writerow([
                _safe(subject),    # :START_ID(Work)
                _safe(venue_key),  # :END_ID(Venue)
            ])
            self._cnt_pub_in += 1

    def _proc_stream(self, subject: str, triples: list[tuple[str, str]]) -> None:
        """处理 Stream 节点（期刊/会议流），用 rdfsLabel 补充/覆盖 Venue 名称。

        Stream 节点在文件末尾（约 565M 行后），此时所有 publication 已处理完毕。
        rdfsLabel 是最权威的全称，优先级最高。

        Args:
            subject: Stream URI（dblp.org/streams/...）。
            triples: 该节点的 (谓词短名, 对象值) 列表。
        """
        venue_key = _extract_venue_key(subject)
        if not venue_key:
            return

        name = ""
        for pred, obj in triples:
            if pred in ("rdfsLabel", "title", "foafName", "primaryName"):
                name = obj
                break

        ccf_rank = self._get_ccf_rank(venue_key)
        vtype = "conf" if venue_key.startswith("conf/") else "journal"

        if venue_key not in self._venue_buffer:
            # stream 先于 publication（极少情况），直接新增
            self._venue_buffer[venue_key] = {
                "name": name,
                "ccf_rank": ccf_rank,
                "type": vtype,
                "name_cnt": Counter([name] if name else []),
                "has_stream_name": bool(name),
            }
        else:
            # 用 stream rdfsLabel 覆盖 pub 收集的名称（更权威）
            entry = self._venue_buffer[venue_key]
            if name:
                entry["name"] = name
                entry["has_stream_name"] = True
            if ccf_rank and not entry["ccf_rank"]:
                entry["ccf_rank"] = ccf_rank

    # ------------------------------------------------------------------
    # flush 调度
    # ------------------------------------------------------------------

    def _flush(self, subject: str, triples: list[tuple[str, str]]) -> None:
        """根据 subject 类型路由到对应处理函数。

        注意：Publication（/rec/）节点不经由此方法处理，
        而是通过 pub_acc 累积后在 parse() 中直接调用 _proc_publication。

        Args:
            subject: 当前 subject 的字符串标识。
            triples: 该 subject 的所有 (谓词短名, 对象值) 列表。
        """
        if not subject or not triples:
            return
        if "_:Sig_" in subject:
            self._proc_signature(subject, triples)
        elif "/pid/" in subject:
            self._proc_person(subject, triples)
        elif "/streams/" in subject:
            self._proc_stream(subject, triples)
        # /rec/ 节点由 pub_acc 机制处理，此处不再路由
        # 其他 subject（DataCite 标识符节点等）直接丢弃

    # ------------------------------------------------------------------
    # 主解析循环
    # ------------------------------------------------------------------

    def parse(self) -> None:
        """执行两遍扫描，将 NT.gz 转换为 Neo4j CSV 文件。

        第一遍（prepass）：快速收集所有 /pid/ 节点的 name 与 ORCID，
        写入 self._pid_meta。
        第二遍（main pass）：流式解析，写出所有 CSV 文件。
        """
        self._out_dir.mkdir(parents=True, exist_ok=True)

        # --- 第一遍：预扫描 Person 数据 ---
        self._prepass_person_data()

        t0 = time.time()

        # 打开所有输出文件
        self._f_work = open(self._out_dir / "nodes_work.csv", "w", encoding="utf-8", newline="")
        self._f_author = open(self._out_dir / "nodes_author.csv", "w", encoding="utf-8", newline="")
        self._f_venue = open(self._out_dir / "nodes_venue.csv", "w", encoding="utf-8", newline="")
        self._f_authored = open(self._out_dir / "rels_authored.csv", "w", encoding="utf-8", newline="")
        self._f_pub_in = open(self._out_dir / "rels_published_in.csv", "w", encoding="utf-8", newline="")

        self._w_work = _csv_writer(self._f_work)
        self._w_author = _csv_writer(self._f_author)
        self._w_venue = _csv_writer(self._f_venue)
        self._w_authored = _csv_writer(self._f_authored)
        self._w_pub_in = _csv_writer(self._f_pub_in)

        # 写表头
        self._w_work.writerow([
            "work_id:ID(Work)", "title", "year:int", "doi",
            "dblp_key", "pub_type", "pages", "ccf_rank", "citation_count:int",
        ])
        self._w_author.writerow([
            "author_id:ID(Author)", "dblp_pid", "name",
            "orcid", "h_index:int", "nationality",
        ])
        self._w_venue.writerow([
            "venue_id:ID(Venue)", "venue_key", "name",
            "ccf_rank", "type", "issn", "impact_factor:float",
        ])
        self._w_authored.writerow([
            ":START_ID(Author)", ":END_ID(Work)", "position:int", "dblp_name",
        ])
        self._w_pub_in.writerow([
            ":START_ID(Work)", ":END_ID(Venue)",
        ])

        # 写占位 CSV（仅 header）
        self._write_placeholder_csvs()

        # 流式扫描
        # /rec/ 论文节点使用 pub_acc 字典累积所有三元组，直到空行才 flush，
        # 以解决 NT 文件中空白节点（Sig/ID）中断导致三元组分裂的问题。
        # 其他节点（/pid/、/streams/、空白节点）仍使用 current_subject 顺序处理。
        current_subject: str = ""
        current_triples: list[tuple[str, str]] = []
        pub_acc: dict[str, list[tuple[str, str]]] = {}  # {rec_uri: [triples]}

        print(f"[parse] 开始扫描: {self._nt_gz_path}")
        limit_str = f"{self._limit:,} 行" if self._limit > 0 else "全量"
        print(f"[parse] 模式: {limit_str}")

        with gzip.open(self._nt_gz_path, "rt", encoding="utf-8", errors="replace") as gz:
            for raw_line in gz:
                self._cnt_lines += 1

                if self._limit > 0 and self._cnt_lines > self._limit:
                    print(f"[parse] 已达限制 {self._limit:,} 行，停止扫描。")
                    break

                triple = parse_nt_line(raw_line)
                if triple is None:
                    # 空行 = 一个完整实体块的结束信号
                    # 先 flush 当前非-pub subject（Sig/ID 空白节点、Stream 等）
                    if current_subject:
                        self._flush(current_subject, current_triples)
                        current_subject = ""
                        current_triples = []
                    # 再 flush 本块内累积的所有 Publication 节点
                    for pub_uri, pub_triples in pub_acc.items():
                        self._proc_publication(pub_uri, pub_triples)
                    pub_acc.clear()
                    continue

                subj, pred, obj = triple
                short_pred = PRED_MAP.get(pred, "")

                if "/rec/" in subj:
                    # Publication 节点：写入 pub_acc 累积器，不触发立即 flush
                    # 这样同一论文被空白节点中断后再次出现时，三元组仍完整累积
                    if current_subject and "/rec/" not in current_subject:
                        # 从非-pub subject 切换到 pub subject：flush 非-pub subject
                        self._flush(current_subject, current_triples)
                        current_subject = ""
                        current_triples = []
                    if short_pred:
                        pub_acc.setdefault(subj, []).append((short_pred, obj))
                else:
                    # 非 Publication 节点（空白节点、/pid/、/streams/ 等）
                    if not short_pred:
                        # 未知谓词：更新 subject 分组，但不添加三元组
                        if subj != current_subject:
                            self._flush(current_subject, current_triples)
                            current_subject = subj
                            current_triples = []
                        continue

                    if subj != current_subject:
                        self._flush(current_subject, current_triples)
                        current_subject = subj
                        current_triples = []

                    current_triples.append((short_pred, obj))

                # 进度打印
                if self._cnt_lines % 500_000 == 0:
                    elapsed = time.time() - t0
                    rate = self._cnt_lines / elapsed if elapsed > 0 else 0
                    print(
                        f"[parse] 行 {self._cnt_lines:>10,} | "
                        f"Work {self._cnt_works:,} | "
                        f"Author {self._cnt_authors:,} | "
                        f"Venue(buf) {len(self._venue_buffer):,} | "
                        f"AUTHORED {self._cnt_authored:,} | "
                        f"速率 {rate:,.0f} 行/秒",
                        flush=True,
                    )

                # 定期 GC，防止内存持续增长
                if self._cnt_lines % 2_000_000 == 0:
                    gc.collect()

        # 处理文件末尾最后一个 subject（无空行结尾时）
        if current_subject:
            self._flush(current_subject, current_triples)
        for pub_uri, pub_triples in pub_acc.items():
            self._proc_publication(pub_uri, pub_triples)
        pub_acc.clear()

        # 将 venue 缓冲区写入 CSV
        # name 取策略：has_stream_name → stream rdfsLabel；否则取 name_cnt 最常出现值；
        # 若仍为空则用 venue_key 末段大写作缩写兜底
        for vk, entry in self._venue_buffer.items():
            name = entry["name"]
            if not name and not entry["has_stream_name"] and entry["name_cnt"]:
                name = entry["name_cnt"].most_common(1)[0][0]
            if not name:
                # 缩写兜底：conf/cvpr → CVPR
                name = vk.split("/")[-1].upper()
            self._w_venue.writerow([
                _safe(vk),              # venue_id:ID(Venue)
                _safe(vk),              # venue_key
                _safe(name),            # name
                _safe(entry["ccf_rank"]),  # ccf_rank
                entry["type"],          # type
                "",                     # issn（外部补全）
                "",                     # impact_factor（外部补全）
            ])
        self._cnt_venues = len(self._venue_buffer)

        for fobj in [
            self._f_work, self._f_author, self._f_venue,
            self._f_authored, self._f_pub_in,
        ]:
            if fobj:
                fobj.close()

        elapsed = time.time() - t0
        self._print_summary(elapsed)

    def _write_placeholder_csvs(self) -> None:
        """写出仅含表头的占位 CSV，供后续外部数据源填充。"""
        placeholders: dict[str, list[str]] = {
            "nodes_concept.csv": [
                "concept_id:ID(Concept)", "name", "level:int",
                "score:float", "wikidata_id",
            ],
            "nodes_institution.csv": [
                "institution_id:ID(Institution)", "ror_id", "name",
                "type", "country",
            ],
            "rels_cites.csv": [
                ":START_ID(Work)", ":END_ID(Work)",
            ],
            "rels_has_topic.csv": [
                ":START_ID(Work)", ":END_ID(Concept)", "score:float",
            ],
            "rels_concept_parent.csv": [
                ":START_ID(Concept)", ":END_ID(Concept)",
            ],
            "rels_affiliated_with.csv": [
                ":START_ID(Author)", ":END_ID(Institution)", "year:int",
            ],
        }
        for fname, header in placeholders.items():
            path = self._out_dir / fname
            with open(path, "w", encoding="utf-8", newline="") as f:
                _csv_writer(f).writerow(header)

    def _print_summary(self, elapsed: float) -> None:
        """打印统计摘要与 neo4j-admin import 命令。

        Args:
            elapsed: 总耗时（秒）。
        """
        out = self._out_dir
        print("\n" + "=" * 65)
        print("  DBLP RDF → Neo4j CSV 转换完成")
        print("=" * 65)
        print(f"  总行数:         {self._cnt_lines:>12,}")
        print(f"  Work 节点:      {self._cnt_works:>12,}")
        print(f"  Author 节点:    {self._cnt_authors:>12,}")
        print(f"  Venue 节点:     {self._cnt_venues:>12,}")
        print(f"  AUTHORED 关系:  {self._cnt_authored:>12,}")
        print(f"  PUBLISHED_IN:   {self._cnt_pub_in:>12,}")
        print(f"  耗时:           {elapsed:.1f}s ({elapsed / 60:.1f}min)")
        print(f"  输出目录:       {out}")
        print("=" * 65)
        print()
        print("  下一步：在 Neo4j 容器内执行批量导入命令：")
        print()
        print("  docker exec -it dases-neo4j-1 bash -c \\")
        print("    'neo4j-admin database import full \\")
        print(f'      --nodes=Work="/var/lib/neo4j/import/dblp/nodes_work.csv" \\')
        print(f'      --nodes=Author="/var/lib/neo4j/import/dblp/nodes_author.csv" \\')
        print(f'      --nodes=Venue="/var/lib/neo4j/import/dblp/nodes_venue.csv" \\')
        print(f'      --nodes=Concept="/var/lib/neo4j/import/dblp/nodes_concept.csv" \\')
        print(f'      --nodes=Institution="/var/lib/neo4j/import/dblp/nodes_institution.csv" \\')
        print(f'      --relationships=AUTHORED="/var/lib/neo4j/import/dblp/rels_authored.csv" \\')
        print(f'      --relationships=PUBLISHED_IN="/var/lib/neo4j/import/dblp/rels_published_in.csv" \\')
        print(f'      --relationships=CITES="/var/lib/neo4j/import/dblp/rels_cites.csv" \\')
        print(f'      --relationships=HAS_TOPIC="/var/lib/neo4j/import/dblp/rels_has_topic.csv" \\')
        print(f'      --delimiter="|" --quote=\'"\'')
        print(f'      --skip-bad-relationships=true \\')
        print(f'      --skip-duplicate-nodes=true \\')
        print("      neo4j'")
        print()
        print("  注意：neo4j-admin import 要求数据库处于停止状态。")
        print("        执行前请确保 Neo4j 容器的 neo4j 数据库已停止：")
        print("        docker exec -it dases-neo4j-1 neo4j stop")
        print("=" * 65)


# ---------------------------------------------------------------------------
# CLI 入口
# ---------------------------------------------------------------------------


def _load_ccf_map(path: Path) -> dict[str, str]:
    """加载 CCF 映射 JSON 文件。

    Args:
        path: ccf_catalogue.json 文件路径。

    Returns:
        {venue_prefix: rank} 字典，加载失败返回空字典。
    """
    if not path.exists():
        print(f"[ccf] 警告: 未找到 CCF 映射文件 {path}，Venue 节点将无 ccf_rank。")
        return {}
    with open(path, "r", encoding="utf-8") as f:
        data: dict[str, str] = json.load(f)
    print(f"[ccf] 已加载 CCF 映射: {len(data)} 条 ({path.name})")
    return data


def main() -> None:
    """命令行入口。"""
    parser = argparse.ArgumentParser(
        description="DBLP RDF NT.gz → Neo4j CSV 转换脚本",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--input",
        default=str(DEFAULT_INPUT),
        help=f"DBLP NT.gz 文件路径（默认: {DEFAULT_INPUT.name}）",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT),
        help=f"CSV 输出目录（默认: data/processed/neo4j/）",
    )
    parser.add_argument(
        "--ccf-map",
        default=str(DEFAULT_CCF_MAP),
        help=f"CCF 映射 JSON 路径（默认: {DEFAULT_CCF_MAP.name}）",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="最大处理行数，0=全量（默认 0）",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"错误: 输入文件不存在: {input_path}")
        sys.exit(1)

    ccf_map = _load_ccf_map(Path(args.ccf_map))

    converter = DblpNt2Neo4j(
        nt_gz_path=input_path,
        out_dir=Path(args.output_dir),
        ccf_map=ccf_map,
        limit=args.limit,
    )
    converter.parse()


if __name__ == "__main__":
    main()
