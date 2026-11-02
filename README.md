# iptv-list

IPTV 直播源自动聚合：多上游抓取 → 可用性探测 → 冻结流检测 → 输出统一播放列表。

## 发布产物（由服务每轮自动更新）

| 文件 | 说明 |
|---|---|
| `iptv.txt` | TVBox / MBox 格式：`频道名,url1#url2`，分组用 `分组名,#genre#` |
| `iptv.m3u` | 标准 M3U，一条流一个 `#EXTINF` |
| `status.json` | 本轮统计：各上游条数 / 存活数 / 耗时 / 分组分布 |

`iptv.txt` 首行是 `#更新时间 ... (北京时间)` 注释，标明最近一次生成时间（客户端会跳过 `#` 开头的行）。

## 源码

`aggregator/` 是聚合服务本体（Python 3.12 + requests，无 Web 框架）：

- `app.py` — 抓取、探测、冻结检测、输出、定时调度、内置 HTTP 服务
- `sources.json` — 上游清单
- `docker-compose.yml` — 部署参数（全部可用环境变量覆盖）
- `Dockerfile` / `requirements.txt`

主要能力：

- **分级探测**：按历史把地址分成 健康 / 可疑 / 死链 三档，死链单次 2s 不重试
- **冻结流检测**：`#EXT-X-MEDIA-SEQUENCE` 跨轮不变 → 判定编码端停摆并剔除（这类流 HTTP 200、能拉分片，但永远从同一画面开始播）
- **台名规范化**：统一 `CCTV-1` / `CCTV1综合` / `CCTV-1 (720p)` 等写法
- **同名归并**：一行一个台，多线路用 `#` 连接，播放失败由客户端自动切线路

### 本地部署

```bash
cd aggregator
cp .env.example .env          # 填要推送的仓库（留空则不推送）
mkdir -p data secrets         # secrets/deploy_key 为推送用的部署私钥
docker compose up -d --build
```

默认监听 `8080`，常用端点：`/iptv.txt`、`/iptv.m3u`、`/status.json`、`/logs`、`/healthz`、`POST /refresh`。

> `.env`、`secrets/`、`data/` 均不入库。
