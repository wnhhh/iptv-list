#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
IPTV 聚合服务
- 定时拉取多个上游直播源列表
- 逐条探测可用性，只保留真正能出流的地址
- 去重、分类、按延迟排序
- 通过内置 HTTP 服务输出 iptv.m3u / iptv.txt / status.json
"""
import os
import json
import re
import sys
import time
import shutil
import subprocess
import threading
import traceback
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutTimeoutError
from urllib.parse import urljoin, urlsplit, parse_qs

import requests

CN = timezone(timedelta(hours=8))
DATA = os.environ.get("DATA_DIR", "/data")
SOURCES_FILE = os.environ.get("SOURCES_FILE", "/app/sources.json")
PORT = int(os.environ.get("PORT", 8080))
WORKERS = int(os.environ.get("WORKERS", 50))
TIMEOUT = float(os.environ.get("TIMEOUT", 6))
RETRY = int(os.environ.get("RETRY", 1))
DEEP_CHECK = os.environ.get("DEEP_CHECK", "0") == "1"   # 是否继续拉取分片验证可播
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "")         # 非空时保护 /refresh /publish /logs
LOG_MAX_BYTES = int(os.environ.get("LOG_MAX_BYTES", 300 * 1024))
MAX_PER_NAME = int(os.environ.get("MAX_PER_NAME", 2))
MAX_CHANNELS = int(os.environ.get("MAX_CHANNELS", 0))
SCHEDULE = os.environ.get("SCHEDULE", "03:20,15:20")
RUN_ON_START = os.environ.get("RUN_ON_START", "1") == "1"
FROZEN_CHECK = os.environ.get("FROZEN_CHECK", "1") == "1"      # 是否剔除"冻结流"
FROZEN_AFTER = float(os.environ.get("FROZEN_AFTER", 1800))     # 序号多久不变判为冻结(秒)
FROZEN_BUDGET = float(os.environ.get("FROZEN_BUDGET", 45))     # 冻结检测整体时间预算(秒)
# 探测分级：健康 / 可疑 / 死链 三档超时与重试策略
FAST_TIMEOUT = float(os.environ.get("FAST_TIMEOUT", 3))         # 可疑源首试
DEAD_TIMEOUT = float(os.environ.get("DEAD_TIMEOUT", 2))         # 死链单次
DEAD_AFTER = int(os.environ.get("DEAD_AFTER", 3))               # 连续几轮失败认定死链
HEAD_FIRST = os.environ.get("HEAD_FIRST", "1") == "1"           # 先用 HEAD 快速排除 404
PROBE_STATE = os.path.join(DATA, "probe_stats.json")
HISTORY = {}
LIVE_STATE = os.path.join(DATA, "liveness.json")
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

# 推送到 GitHub（使用部署密钥，未配置仓库时自动跳过）
GITHUB_REPO = os.environ.get("GITHUB_REPO", "").strip()
GITHUB_BRANCH = os.environ.get("GITHUB_BRANCH", "").strip()
GIT_SSH_KEY = os.environ.get("GIT_SSH_KEY", "/secrets/deploy_key")
GIT_WORKDIR = os.path.join(DATA, "repo")
PUBLISH_FILES = ["iptv.m3u", "iptv.txt", "status.json"]

# 下载镜像链：逗号分隔，空项表示直连；仅对 raw.githubusercontent.com 地址生效
MIRRORS = [x.strip() for x in os.environ.get("MIRRORS", "https://ghfast.top/").split(",")]
GROUPS = ["央视频道", "卫视频道", "地方频道", "体育频道", "新闻财经",
          "电影频道", "纪录频道", "儿童频道", "音乐频道"]

STATE = {"running": False, "last": None, "next": None, "log": []}
LOCK = threading.Lock()
STOP = threading.Event()


def now_cn():
    return datetime.now(CN)


def log(msg):
    line = f"[{now_cn().strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    print(line, flush=True)
    STATE["log"] = (STATE["log"] + [line])[-200:]
    try:
        os.makedirs(DATA, exist_ok=True)
        path = os.path.join(DATA, "run.log")
        with open(path, "a", encoding="utf-8") as f:
            f.write(line + "\n")
        # 限制日志体积，避免长期运行撑爆磁盘
        if os.path.getsize(path) > LOG_MAX_BYTES:
            with open(path, encoding="utf-8") as f:
                keep = f.readlines()[-400:]
            with open(path, "w", encoding="utf-8") as f:
                f.writelines(keep)
    except OSError:
        pass


def load_sources():
    with open(SOURCES_FILE, encoding="utf-8") as f:
        cfg = json.load(f)
    out = []
    for s in cfg.get("sources", []):
        if not s.get("enabled", True):
            continue
        if "url" in s:
            url = s["url"]
        else:
            url = ("https://raw.githubusercontent.com/{repo}/refs/heads/{branch}/{path}"
                   .format(repo=s["repo"], branch=s.get("branch", "main"), path=s["path"]))
        out.append({"name": s.get("name") or s.get("repo") or url, "url": url,
                    "priority": s.get("priority", 5)})
    return out


def fetch(url, timeout=20):
    """带镜像回退的下载。只对 GitHub raw 地址套用镜像，避免拼出非法 URL。"""
    candidates = []
    for m in MIRRORS:
        if m == "":
            candidates.append(url)
        elif "raw.githubusercontent.com/" not in url:
            continue
        elif m == "https://cdn.jsdelivr.net/":
            mm = re.match(r"https://raw\.githubusercontent\.com/([^/]+/[^/]+)/refs/heads/([^/]+)/(.+)", url)
            if mm:
                candidates.append(f"https://cdn.jsdelivr.net/gh/{mm.group(1)}@{mm.group(2)}/{mm.group(3)}")
        else:
            candidates.append(m + url)
    for target in candidates:
        try:
            with requests.get(target, timeout=(8, timeout), headers=UA) as r:
                if r.status_code == 200 and len(r.content) > 100:
                    return r.text
        except requests.RequestException:
            continue
    return None


def parse_entries(text, source):
    """兼容 m3u 与 `频道名,url` + #genre# 两种格式"""
    out, group, cur = [], None, None
    for line in re.split(r"[\r\n]+", text):
        line = line.strip()
        if not line:
            continue
        if "#genre#" in line:
            group = line.split(",")[0].strip()
            continue
        if line.startswith("#EXTINF"):
            m = re.search(r'group-title="([^"]*)"', line)
            if m:
                group = m.group(1)
            cur = line.rsplit(",", 1)[-1].strip() if "," in line else None
        elif line.startswith("#"):
            continue
        elif line.startswith(("http://", "https://", "rtmp://", "rtsp://")):
            if cur:
                out.append({"group": group or "", "name": cur, "url": line,
                            "source": source})
                cur = None
        elif "," in line and line.count(",") == 1:
            name, url = line.split(",", 1)
            url = url.strip()
            if url.startswith(("http://", "https://", "rtmp://", "rtsp://")):
                out.append({"group": group or "", "name": name.strip(), "url": url,
                            "source": source})
    return out


def segment_url(body, base):
    """从 m3u8 里取第一个子清单或分片地址，相对地址转绝对"""
    for line in body.decode("utf-8", "ignore").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        return line if line.startswith(("http://", "https://")) else urljoin(base, line)
    return None


def probe(item):
    """探测单条地址可用性。按历史分三档，越可疑的超时越短、重试越少：
    健康(上轮成功)  → 完整超时 + 正常重试
    可疑(上轮失败)  → 首试快超时 + 正常重试
    死链(连续失败)  → 单次快超时，不重试
    另外对"没成功过"的地址先发一个 HEAD，404/410/451 直接判死，省掉一次 GET。"""
    url = item["url"]
    hist = HISTORY.get(url) or {}
    fails = int(hist.get("fails", 0))
    prev_ok = bool(hist.get("ok"))

    if fails >= DEAD_AFTER:
        tier, tries, t_first, t_next = "dead", 1, DEAD_TIMEOUT, DEAD_TIMEOUT
    elif prev_ok:
        tier, tries, t_first, t_next = "ok", RETRY + 1, TIMEOUT, TIMEOUT
    else:
        tier, tries, t_first, t_next = "suspect", RETRY + 1, FAST_TIMEOUT, TIMEOUT

    ok, ms, body = False, 0, b""
    # 注意：不要复用连接。这批 IPTV 服务器回完响应就断连，keep-alive 会
    # 造成大量 RemoteDisconnected 与超时重试，实测比每次新建连接还慢。
    headers = dict(UA, Connection="close")

    head_404 = False
    if HEAD_FIRST and tier != "dead" and not prev_ok and url.startswith("http"):
        try:
            with requests.head(url, timeout=(FAST_TIMEOUT, FAST_TIMEOUT),
                               headers=headers, allow_redirects=True) as hr:
                # 只当线索用，不能直接判死：实测不少 IPTV 服务器对 HEAD 回 404，GET 却是 200
                head_404 = hr.status_code in (404, 410, 451)
        except requests.RequestException:
            pass          # 不支持 HEAD / 超时，忽略
    if head_404:
        tier, tries, t_first, t_next = "head404", 1, DEAD_TIMEOUT, DEAD_TIMEOUT

    for attempt in range(tries):
        t0 = time.time()
        status, err = 0, False
        timeout = t_first if attempt == 0 else t_next
        try:
            with requests.get(url, timeout=(timeout, timeout), stream=True,
                              headers=headers) as r:
                status = r.status_code
                if status == 200:
                    body = next(r.iter_content(4096), b"") or b""
            ms = int((time.time() - t0) * 1000)
            ok = status == 200 and (
                b"#EXTM3U" in body or b"#EXTINF" in body
                or body[:1] == b"\x47" or b"FLV" in body[:64])
            # 可选深度校验：只认明确的 4xx，超时/空内容一律不动原判
            if ok and DEEP_CHECK and b"#EXTM3U" in body:
                seg = segment_url(body, url)
                if seg:
                    try:
                        with requests.get(seg, timeout=(timeout, timeout + 4),
                                          stream=True, headers=headers) as r2:
                            if r2.status_code in (403, 404, 410, 451):
                                ok = False
                                item["deep_fail"] = r2.status_code
                    except requests.RequestException:
                        item["deep"] = "unchecked"
                if not ok:
                    break     # 已确认死链，不必再重试
        except requests.RequestException:
            err = True
            ms = int(timeout * 1000)
        if ok:
            break
        # 只有“可能是抖动”的情况才值得重试；404/403 或返回 HTML 页面重试也是白等
        retryable = err or status == 0 or status >= 500 or (status == 200 and not body)
        if attempt < tries - 1 and retryable:
            time.sleep(0.8)
            continue
        break
    item["ok"] = ok
    item["ms"] = ms
    item["tier"] = tier
    # 复用这次已读到的首块内容：直接能从播放列表里拿到 MEDIA-SEQUENCE 就不必再单独请求一次
    item["seq"] = seq_in_text(body.decode("utf-8", "ignore")) if ok else None
    return item


def guess_group(name):
    n = name.replace(" ", "")
    if re.search(r"CCTV|央视|CGTN|中央", n, re.I):
        return "央视频道"
    if "卫视" in n:
        return "卫视频道"
    if re.search(r"体育|足球|篮球|NBA|英超|西甲|赛事", n, re.I):
        return "体育频道"
    if re.search(r"电影|影院|CHC|HBO|剧场", n, re.I):
        return "电影频道"
    if re.search(r"纪录|探索|Discovery|纪实", n, re.I):
        return "纪录频道"
    if re.search(r"少儿|卡通|动画|儿童", n):
        return "儿童频道"
    if re.search(r"音乐|MV|演唱会", n, re.I):
        return "音乐频道"
    if re.search(r"新闻|财经|资讯", n):
        return "新闻财经"
    return "地方频道"


def norm(name):
    return re.sub(r"[\s\-_（）()]+", "", name).lower()


# ---- 台名规范化 ----
# 各上游写法不统一：同一路 CCTV-1 可能叫 "CCTV-1" / "CCTV1综合" / "CCTV-1 (720p)"，
# 不统一就会在列表里各占一行。这里把画质标记、CCTV 编号写法抹平。
QUALITY_RE = re.compile(
    r"[\(\[（【][^\)\]）】]{0,12}"
    r"(?:1080p?|720p?|576i?|480p?|2160p?|[48]k|uhd|fhd|hd|sd|bd|hevc|h\.?26[45])"
    r"[^\)\]）】]{0,6}[\)\]）】]", re.I)
TAIL_QUALITY_RE = re.compile(r"(超高清|高清|标清|蓝光)$")
# CCTV 台名里这些中文后缀只是描述，不构成不同频道
CCTV_DESC = {"综合", "财经", "综艺", "体育", "电影", "电视剧", "纪录", "科教",
             "戏曲", "社会与法", "新闻", "少儿", "音乐", "农业农村",
             "国防军事", "体育赛事", "新闻频道"}


def canon_name(raw):
    n = QUALITY_RE.sub("", raw).strip()
    n = TAIL_QUALITY_RE.sub("", n).strip()
    n = n.replace("（", "(").replace("）", ")")
    n = re.sub(r"\s+", "", n)
    m = re.match(r"(?i)^cctv[-_]?(\d{1,2})(\+?)(.*)$", n)
    if m:
        num, plus, rest = m.group(1), m.group(2), m.group(3)
        # 中文描述后缀归并成编号写法；CCTV4欧洲/美洲、CCTV4K/8K 这类是不同频道，保留
        if rest == "" or rest in CCTV_DESC:
            return f"CCTV-{num}{plus}"
        return f"CCTV-{num}{plus}{rest}"
    return n


def atomic_write(path, text):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
    os.replace(tmp, path)


def load_history():
    """上一次每轮探测的结果，用来分级超时/重试，以及记住连续失败的死链。"""
    try:
        with open(PROBE_STATE, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def save_history(probed):
    now = int(time.time())
    new = {}
    for x in probed:
        u = x["url"]
        prev = HISTORY.get(u) or {}
        if x.get("ok"):
            new[u] = {"ok": 1, "fails": 0, "tier": x.get("tier"), "ts": now}
        else:
            new[u] = {"ok": 0, "fails": int(prev.get("fails", 0)) + 1,
                      "tier": x.get("tier"), "ts": now}
    atomic_write(PROBE_STATE, json.dumps(new, ensure_ascii=False))
    return new


def seq_in_text(text):
    """从播放列表文本里取 MEDIA-SEQUENCE；master 列表没有这个标签，返回 None 表示需要再跟一层。"""
    if "#EXTM3U" not in text:
        return None
    m = re.search(r"#EXT-X-MEDIA-SEQUENCE:\s*(\d+)", text)
    return m.group(1) if m else None


def playlist_seq(url, depth=0):
    """取 HLS 播放列表的 MEDIA-SEQUENCE(master 列表向下跟一层)。
    返回 None 表示"判断不了"(非 HLS / 取不到 / 没写序号)，这类不参与冻结判定。
    注意必须用 stream=True 只读前 8KB：部分地址是无限 TS 流，一次性读全会被吊死。"""
    try:
        with requests.get(url, timeout=(3, 5), headers=dict(UA, Connection="close"),
                          stream=True) as r:
            if r.status_code != 200:
                return None
            body = next(r.iter_content(8192), b"") or b""
            body = body.decode("utf-8", "ignore")
    except requests.RequestException:
        return None
    if seq_in_text(body) is not None:
        return seq_in_text(body)
    if "#EXTM3U" in body and "#EXT-X-STREAM-INF" in body and depth < 2:
        for line in body.splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                return playlist_seq(urljoin(url, line), depth + 1)
    return None


def frozen_key(item):
    """冻结检测的状态键：台名 + 去掉查询串的地址。
    不能只用完整 URL——带 token/时间戳的地址每轮都变(如咪咕)，那样永远建不起跨轮基线。"""
    p = urlsplit(item["url"])
    return f'{item["name"]}|{p.scheme}://{p.netloc}{p.path}'


def drop_frozen(items):
    """冻结流检测：MEDIA-SEQUENCE 长时间不变 = 编码端已停，客户端每次都从头播同一段画面
    (这类流 HTTP 200、能拉分片，普通可用性探测抓不到)。状态存 data/liveness.json 跨轮次比对，
    序号一变即解除冻结。"""
    try:
        with open(LIVE_STATE, encoding="utf-8") as f:
            state = json.load(f)
    except (OSError, ValueError):
        state = {}
    t_frozen = time.time()
    targets, known = {}, {}
    for x in items:
        k = frozen_key(x)
        targets.setdefault(k, x["url"])
        if x.get("seq"):
            known.setdefault(k, x["seq"])     # 探测阶段已经读到播放列表，直接复用，省一次请求
    keys = sorted(targets)
    need = [k for k in keys if k not in known]
    seqs = dict(known)
    if need:
        pool = ThreadPoolExecutor(min(256, max(8, WORKERS * 5)))
        futs = {pool.submit(playlist_seq, targets[k]): k for k in need}
        try:
            for fu in as_completed(futs, timeout=FROZEN_BUDGET):
                try:
                    seqs[futs[fu]] = fu.result()
                except Exception:
                    seqs[futs[fu]] = None
        except FutTimeoutError:
            log(f"  冻结检测超时（预算 {int(FROZEN_BUDGET)}s），已完成 {len(seqs)}/{len(keys)}，其余按未知处理")
        pool.shutdown(wait=False)
    now = time.time()
    frozen, new_state, detail = set(), {}, []
    for k in keys:
        seq, prev = seqs.get(k), state.get(k) or {}
        if seq is None:
            if prev.get("seq"):
                new_state[k] = dict(prev, seen=now)
            continue
        if prev.get("seq") == seq:
            ts = prev.get("ts", now)      # ts = 首次看到该序号的时间，不随本轮刷新
            # 一旦判定冻结就一直剔除，直到序号变化；避免阈值调大后又被放回来
            frz = bool(prev.get("frozen")) or (now - ts >= FROZEN_AFTER)
            if frz:
                frozen.add(k)
                if len(detail) < 8:       # 明细只打前 8 条，避免刷屏把汇总挤掉
                    detail.append(f"  冻结流({int((now - ts) / 60)}分钟未变/{seq}): {targets[k][:90]}")
            new_state[k] = {"seq": seq, "ts": ts, "seen": now, "frozen": frz}
        else:
            new_state[k] = {"seq": seq, "ts": now, "seen": now, "frozen": False}
    cutoff = now - 7 * 86400              # 7 天没再出现的条目直接清掉
    new_state = {k: v for k, v in new_state.items() if v.get("seen", now) >= cutoff}
    atomic_write(LIVE_STATE, json.dumps(new_state, ensure_ascii=False))
    for d in detail:
        log(d)
    if len(frozen) > len(detail):
        log(f"  …另有 {len(frozen) - len(detail)} 条冻结流，明细见 data/liveness.json")
    kept = items if not frozen else [x for x in items if frozen_key(x) not in frozen]
    names = {x["name"] for x in items if frozen_key(x) in frozen}
    log(f"冻结检测：{len(keys)} 条线路（复用 {len(known)} / 补拉 {len(need)}），"
        + (f"剔除 {len(items) - len(kept)} 条（{len(names)} 个台）" if frozen else "未发现冻结流")
        + f"，耗时 {time.time() - t_frozen:.1f}s")
    return kept


def git(args, cwd=None, timeout=120):
    env = os.environ.copy()
    env["GIT_SSH_COMMAND"] = (
        f"ssh -i {GIT_SSH_KEY} -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new "
        f"-o UserKnownHostsFile=/dev/null -o BatchMode=yes"
    )
    env["GIT_TERMINAL_PROMPT"] = "0"
    return subprocess.run(["git"] + args, cwd=cwd or GIT_WORKDIR, env=env,
                          capture_output=True, text=True, timeout=timeout)


def push_to_github():
    """把产物提交并推送到仓库；内容没变则跳过，失败不影响主流程"""
    if not GITHUB_REPO:
        return
    remote = f"git@github.com:{GITHUB_REPO}.git"
    try:
        if not os.path.isdir(os.path.join(GIT_WORKDIR, ".git")):
            r = git(["clone", "--depth", "1", remote, GIT_WORKDIR], cwd=DATA)
            if r.returncode != 0:
                log("GitHub 克隆失败：" + (r.stderr or r.stdout).strip()[:300])
                return
        branch = GITHUB_BRANCH
        if not branch:
            r = git(["rev-parse", "--abbrev-ref", "HEAD"])
            branch = r.stdout.strip() or "main"
        if branch == "HEAD":          # 空仓库时 rev-parse 返回 HEAD
            branch = "main"
        if git(["ls-remote", "--heads", "origin"]).stdout.strip():
            r = git(["fetch", "--depth", "1", "origin", branch])
            if r.returncode != 0:
                log("GitHub fetch 失败：" + (r.stderr or r.stdout).strip()[:300])
                return
            git(["reset", "--hard", f"origin/{branch}"])
        else:
            log("仓库为空，本次将创建首个提交")
        for f in PUBLISH_FILES:
            src = os.path.join(DATA, f)
            if os.path.exists(src):
                shutil.copy(src, os.path.join(GIT_WORKDIR, f))
        git(["add", "-A"])
        if git(["diff", "--cached", "--quiet"]).returncode == 0:
            log("GitHub 内容无变化，跳过提交")
            return
        msg = f"chore: update playlist {now_cn():%Y-%m-%d %H:%M} (CST)"
        r = git(["-c", "user.name=iptv-agg", "-c", "user.email=iptv-agg@users.noreply.github.com",
                 "commit", "-m", msg])
        if r.returncode != 0:
            log("Git 提交失败：" + (r.stderr or r.stdout).strip()[:300])
            return
        r = git(["push", "origin", f"HEAD:{branch}"])
        if r.returncode == 0:
            log(f"已推送 GitHub {GITHUB_REPO}@{branch}")
        else:
            log("推送失败：" + (r.stderr or r.stdout).strip()[:300])
    except subprocess.TimeoutExpired:
        log("GitHub 推送超时")
    except Exception:
        log("GitHub 推送异常：\n" + traceback.format_exc())


def run_once():
    global HISTORY
    if not LOCK.acquire(blocking=False):
        log("已有任务在跑，跳过本次")
        return
    STATE["running"] = True
    t_start = time.time()
    try:
        sources = load_sources()
        log(f"开始抓取，共 {len(sources)} 个上游")

        def grab(s):
            text = fetch(s["url"])
            return s, (parse_entries(text, s["name"]) if text else None)

        entries, per_source, failed_sources = [], {}, []
        with ThreadPoolExecutor(min(6, max(1, len(sources)))) as ex:
            for s, items in ex.map(grab, sources):
                if items is None:
                    failed_sources.append(s["name"])
                    per_source[s["name"]] = {"entries": 0, "alive": 0, "error": "下载失败"}
                    log(f"  下载失败: {s['name']}")
                    continue
                for it in items:
                    it["priority"] = s["priority"]
                entries += items
                per_source[s["name"]] = {"entries": len(items), "alive": 0}
                log(f"  {s['name']}: {len(items)} 条")

        if not entries:
            log("所有上游都没拿到条目，保留上一次结果")
            return

        # 先按 URL 去重再探测：上游之间重复地址很多，白测既慢又容易被服务器限流
        uniq = {}
        for e in entries:
            u = e["url"]
            if u not in uniq or e["priority"] < uniq[u]["priority"]:
                uniq[u] = e
        total = len(entries)
        entries = list(uniq.values())

        HISTORY = load_history()
        dead_n = sum(1 for v in HISTORY.values() if int(v.get("fails", 0)) >= DEAD_AFTER)
        log(f"开始探测 {len(entries)} 条（原始 {total} 条，去重省 {total - len(entries)} 条），"
            f"并发 {WORKERS}，分级超时 {FAST_TIMEOUT:g}/{TIMEOUT:g}/{DEAD_TIMEOUT:g}s，"
            f"死链记忆 {dead_n} 条，HEAD预筛={'开' if HEAD_FIRST else '关'}")
        with ThreadPoolExecutor(WORKERS) as ex:
            probed = list(ex.map(probe, entries))
        alive = [x for x in probed if x["ok"]]
        alive_raw = len(alive)
        HISTORY = save_history(probed)
        dead_now = sum(1 for v in HISTORY.values() if int(v.get("fails", 0)) >= DEAD_AFTER)
        log(f"探测完成：可用 {alive_raw}/{len(probed)}，死链库 {dead_now} 条")

        # URL 去重
        best = {}
        for x in alive:
            u = x["url"]
            if u not in best or x["ms"] < best[u]["ms"]:
                best[u] = x
        alive = list(best.values())

        # 台名规范化（统一 CCTV 编号 / 去掉画质标记），否则同一台会以多个名字重复出现
        for x in alive:
            x["name"] = canon_name(x["name"])

        # 同名频道限流
        by_name = {}
        for x in alive:
            by_name.setdefault(norm(x["name"]), []).append(x)
        final = []
        for v in by_name.values():
            v.sort(key=lambda y: (y["ms"], y["priority"]))
            final += v[:max(1, MAX_PER_NAME)]

        for x in final:
            if x["group"] not in GROUPS:
                x["group"] = guess_group(x["name"])
        final.sort(key=lambda y: (GROUPS.index(y["group"]), y["ms"]))
        if MAX_CHANNELS > 0:
            final = final[:MAX_CHANNELS]
        if FROZEN_CHECK:
            final = drop_frozen(final)

        # 按台名归并：不同上游会把同一个台分到不同分组，先按名称收成一条，保留分组首次出现的顺序
        merged, cidx = [], {}
        for x in final:
            k = norm(x["name"])
            if k not in cidx:
                cidx[k] = len(merged)
                merged.append({"name": x["name"], "group": x["group"], "source": x["source"],
                               "ms": x["ms"], "urls": [x["url"]]})
            else:
                e = merged[cidx[k]]
                if len(e["urls"]) < max(1, MAX_PER_NAME) and x["url"] not in e["urls"]:
                    e["urls"].append(x["url"])
        if len(merged) != len(final):
            log(f"同名归并：{len(final)} 条线路 -> {len(merged)} 个频道")
        final = merged

        for name, st in per_source.items():
            st["alive"] = sum(1 for x in final if x["source"] == name)

        m3u = ["#EXTM3U"]
        for x in final:
            for u in x["urls"]:
                m3u.append(f'#EXTINF:-1 tvg-name="{x["name"]}" group-title="{x["group"]}",{x["name"]}')
                m3u.append(u)
        atomic_write(os.path.join(DATA, "iptv.m3u"), "\n".join(m3u) + "\n")

        txt, url_lines, name_lines = [], 0, 0
        for g in GROUPS:
            sub = [x for x in final if x["group"] == g]
            if not sub:
                continue
            txt.append(f"{g},#genre#")
            # 一行一个台，多线路用 # 分隔（TVBox / MBox 的多线路约定，播放失败自动切线路）
            for x in sub:
                txt.append(f'{x["name"]},{"#".join(x["urls"])}')
                name_lines += 1
                url_lines += len(x["urls"])
        # 首行写更新时间注释（客户端按 # 注释跳过，不影响解析；浏览器打开可直接看到）
        txt.insert(0, f"#更新时间 {now_cn():%Y-%m-%d %H:%M:%S} (北京时间) | "
                      f"{name_lines} 个频道 / {url_lines} 条线路")
        atomic_write(os.path.join(DATA, "iptv.txt"), "\n".join(txt) + "\n")
        log(f"同名合并：{url_lines} 条线路 -> {name_lines} 个频道行")

        counts = {}
        for x in final:
            counts[x["group"]] = counts.get(x["group"], 0) + 1

        # 保留最近 5 次历史
        hist = os.path.join(DATA, "history")
        os.makedirs(hist, exist_ok=True)
        shutil.copy(os.path.join(DATA, "iptv.m3u"),
                    os.path.join(hist, now_cn().strftime("%Y%m%d-%H%M") + ".m3u"))
        files = sorted(os.listdir(hist))
        for f in files[:-5]:
            os.remove(os.path.join(hist, f))

        status = {
            "updated": now_cn().strftime("%Y-%m-%d %H:%M:%S"),
            "duration_s": round(time.time() - t_start, 1),
            "sources": per_source,
            "failed_sources": failed_sources,
            "tested": len(probed),
            "raw_entries": total,
            "deep_check": DEEP_CHECK,
            "alive": len(alive),
            "alive_raw": alive_raw,
            "discarded": len(probed) - alive_raw,
            "deep_rejected": sum(1 for x in probed if x.get("deep_fail")),
            "channels": len(final),
            "groups": counts,
            "dead_known": sum(1 for v in HISTORY.values() if int(v.get("fails", 0)) >= DEAD_AFTER),
            "median_ms": sorted(x["ms"] for x in final)[len(final) // 2] if final else 0,
        }
        STATE["last"] = status
        atomic_write(os.path.join(DATA, "status.json"),
                     json.dumps(status, ensure_ascii=False, indent=1))
        log(f"完成：{len(final)} 个频道，用时 {status['duration_s']}s -> iptv.m3u / iptv.txt")
        push_to_github()
    except Exception:
        log("任务异常：\n" + traceback.format_exc())
    finally:
        STATE["running"] = False
        LOCK.release()


def next_run_time():
    times = []
    for part in SCHEDULE.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            h, m = part.split(":")
            times.append((int(h), int(m)))
        except ValueError:
            continue
    if not times:
        times = [(3, 20)]
    now = now_cn()
    today = [now.replace(hour=h, minute=m, second=0, microsecond=0) for h, m in times]
    future = [t for t in today if t > now]
    return (min(future) if future else min(today) + timedelta(days=1)), times


def scheduler():
    nxt, times = next_run_time()
    STATE["next"] = nxt.strftime("%Y-%m-%d %H:%M:%S")
    log(f"定时计划 {SCHEDULE}（北京时间），下次 {STATE['next']}")
    if RUN_ON_START:
        log("启动即执行一次")
        run_once()
        nxt, times = next_run_time()
        STATE["next"] = nxt.strftime("%Y-%m-%d %H:%M:%S")
    while not STOP.is_set():
        if now_cn() >= nxt:
            run_once()
            nxt, times = next_run_time()
            STATE["next"] = nxt.strftime("%Y-%m-%d %H:%M:%S")
            log(f"下次执行 {STATE['next']}")
        STOP.wait(20)


PAGE = """<!doctype html><html lang="zh"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>IPTV 聚合服务</title><style>
*{box-sizing:border-box}body{margin:0;font:14px/1.6 -apple-system,"Segoe UI","Microsoft YaHei",sans-serif;
background:#f6f7f9;color:#1f2328}.w{max-width:880px;margin:0 auto;padding:28px 20px}
h1{font-size:20px;margin:0 0 4px}.sub{color:#656d76;font-size:13px;margin-bottom:20px}
.card{background:#fff;border:1px solid #d8dee4;border-radius:10px;padding:18px;margin-bottom:16px}
.row{display:flex;gap:12px;flex-wrap:wrap}.kpi{flex:1;min-width:120px;background:#f6f8fa;border-radius:8px;padding:12px}
.kpi b{display:block;font-size:22px;line-height:1.2}.kpi span{color:#656d76;font-size:12px}
a.btn{display:inline-block;padding:8px 14px;border:1px solid #d0d7de;border-radius:8px;
text-decoration:none;color:#0969da;margin:4px 8px 4px 0;background:#fff}
a.btn:hover{background:#f3f4f6}table{width:100%;border-collapse:collapse;font-size:13px}
th,td{text-align:left;padding:6px 8px;border-bottom:1px solid #eaeef2}th{color:#656d76;font-weight:500}
code{background:#f6f8fa;padding:2px 5px;border-radius:4px;font-size:12px}
</style></head><body><div class="w">
<h1>IPTV 聚合服务</h1><div class="sub" id="sub">加载中…</div>
<div class="card"><div class="row" id="kpi"></div>
<div style="margin-top:14px">
<a class="btn" href="/iptv.m3u">下载 iptv.m3u</a>
<a class="btn" href="/iptv.txt">下载 iptv.txt</a>
<a class="btn" href="/status.json">status.json</a>
<a class="btn" href="/healthz">healthz</a>
<a class="btn" href="#" onclick="fetch('/refresh',{method:'POST'}).then(()=>alert('已触发抓取，约 40 秒后自动刷新页面'));return false">立即抓取</a></div></div>
<div class="card"><div style="font-weight:600;margin-bottom:8px">上游明细</div>
<table id="src"><thead><tr><th>上游</th><th>抓到</th><th>存活</th></tr></thead><tbody></tbody></table></div>
<div class="card"><div style="font-weight:600;margin-bottom:8px">运行日志（最近 30 行）</div>
<pre id="log" style="margin:0;white-space:pre-wrap;font-size:12px;color:#57606a"></pre></div>
</div><script>
fetch('/status.json').then(r=>r.json()).then(d=>{
 document.getElementById('sub').textContent='最后更新 '+d.updated+'　用时 '+d.duration_s+'s　探测 '+d.tested+' 条';
 const c=d.groups||{};let h='';
 h+=`<div class="kpi"><b>${d.channels}</b><span>收录频道</span></div>`;
 h+=`<div class="kpi"><b>${d.alive_raw||d.alive}</b><span>探测存活</span></div>`;
 h+=`<div class="kpi"><b>${d.median_ms}ms</b><span>中位延迟</span></div>`;
 h+=`<div class="kpi"><b>${d.failed_sources.length}</b><span>失败上游</span></div>`;
 document.getElementById('kpi').innerHTML=h;
 let t='';for(const k in d.sources){const s=d.sources[k];
  t+=`<tr><td>${k}</td><td>${s.entries}</td><td>${s.alive}</td></tr>`}
 document.querySelector('#src tbody').innerHTML=t;
}).catch(e=>document.getElementById('sub').textContent='状态读取失败');
fetch('/logs').then(r=>r.text()).then(t=>{document.getElementById('log').textContent=t.split('\\n').slice(-30).join('\\n')});
setInterval(()=>location.reload(),60000);
</script></body></html>"""


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "iptvagg/1.2"
    timeout = 30          # 慢连接最多占 30 秒线程

    def _admin_ok(self):
        if not ADMIN_TOKEN:
            return True
        if self.headers.get("X-Admin-Token") == ADMIN_TOKEN:
            return True
        return parse_qs(urlsplit(self.path).query).get("token", [""])[0] == ADMIN_TOKEN

    def _require_admin(self):
        if self._admin_ok():
            return True
        self._send(401, json.dumps({"error": "unauthorized"}))
        return False

    def _send(self, code, body, ctype="application/json; charset=utf-8",
              extra=None, head_only=False):
        data = body.encode("utf-8") if isinstance(body, str) else body
        try:
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Access-Control-Allow-Origin", "*")
            for k, v in (extra or {}).items():
                self.send_header(k, v)
            self.end_headers()
            if not head_only:
                self.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError):
            # 客户端提前断开（TVBox / 播放器只读头部就会关连接），正常现象
            self.close_connection = True

    def do_HEAD(self):
        self._dispatch(head_only=True)

    def do_POST(self):
        if not self._require_admin():
            return
        p = self.path.split("?")[0]
        if p == "/refresh":
            threading.Thread(target=run_once, daemon=True).start()
            return self._send(202, json.dumps({"started": "refresh"}))
        if p == "/publish":
            threading.Thread(target=push_to_github, daemon=True).start()
            return self._send(202, json.dumps({"started": "publish"}))
        return self._send(405, json.dumps({"error": "method not allowed"}))

    def log_message(self, fmt, *args):
        # 播放器会高频轮询，访问日志只进 run.log 太重，这里静默
        pass

    def do_GET(self):
        self._dispatch(head_only=False)

    def _dispatch(self, head_only=False):
        p = self.path.split("?")[0]
        if p in ("/", "/index.html"):
            return self._send(200, PAGE, "text/html; charset=utf-8", head_only=head_only)
        if p in ("/iptv.m3u", "/tv.m3u", "/live.m3u"):
            return self._file("iptv.m3u", "audio/x-mpegurl", head_only)
        if p in ("/iptv.txt", "/live.txt"):
            return self._file("iptv.txt", "text/plain; charset=utf-8", head_only)
        if p == "/status.json":
            return self._file("status.json", "application/json; charset=utf-8", head_only)
        if p == "/logs":
            if not self._require_admin():
                return
            try:
                with open(os.path.join(DATA, "run.log"), encoding="utf-8") as f:
                    return self._send(200, f.read()[-8000:], "text/plain; charset=utf-8", head_only=head_only)
            except OSError:
                return self._send(200, "", "text/plain; charset=utf-8", head_only=head_only)
        if p == "/healthz":
            body = json.dumps({"ok": True, "running": STATE["running"], "next": STATE["next"]})
            return self._send(200, body, head_only=head_only)
        if p == "/refresh":
            if not self._require_admin():
                return
            threading.Thread(target=run_once, daemon=True).start()
            return self._send(202, json.dumps({"started": True}), head_only=head_only)
        return self._send(404, json.dumps({"error": "not found"}), head_only=head_only)

    def _file(self, name, ctype, head_only=False):
        path = os.path.join(DATA, name)
        try:
            st = os.stat(path)
        except OSError:
            msg = json.dumps({"error": "尚未生成，请等待首次抓取完成"})
            return self._send(503, msg, head_only=head_only)
        # ETag + Last-Modified：播放器下次带 If-None-Match 时直接回 304，省流量也少日志
        etag = '"{:x}-{:x}"'.format(int(st.st_mtime), st.st_size)
        headers = {
            "ETag": etag,
            "Last-Modified": self.date_time_string(st.st_mtime),
            "Cache-Control": "no-cache",
        }
        if self.headers.get("If-None-Match") == etag:
            try:
                self.send_response(304)
                for k, v in headers.items():
                    self.send_header(k, v)
                self.send_header("Content-Length", "0")
                self.end_headers()
            except (BrokenPipeError, ConnectionResetError):
                self.close_connection = True
            return
        try:
            with open(path, "rb") as f:
                data = f.read()
        except OSError:
            return self._send(503, json.dumps({"error": "读取失败"}), head_only=head_only)
        self._send(200, data, ctype, headers, head_only=head_only)


class Server(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True
    request_queue_size = 128

    def handle_error(self, request, client_address):
        exc = sys.exc_info()[1]
        if isinstance(exc, (BrokenPipeError, ConnectionResetError)):
            return
        super().handle_error(request, client_address)


def main():
    os.makedirs(DATA, exist_ok=True)
    t = threading.Thread(target=scheduler, daemon=True)
    t.start()
    srv = Server(("0.0.0.0", PORT), Handler)
    log(f"HTTP 服务已启动，端口 {PORT}")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        STOP.set()


if __name__ == "__main__":
    main()
