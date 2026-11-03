#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TT-Sync 云端备份状态与清单比对服务 (Manifest V2 增强版)
- 内存占用: ~8-12MB (Python 3 原生零依赖)
- CPU 占用: 平时 0%，极低开销
- 功能:
  1. 严格遵循原版 TT-Sync 排除规则扫描备份目录，生成完整文件清单 (path, size, mtime_ms)
  2. 计算整体 SHA-256 清单指纹 (manifest_hash) 与对话指纹 (chats_hash)
  3. 供前端【高级同步】面板先做“数据一致性比对”，不一致时再按修改时间判断哪边更新
"""

import os
import json
import time
import hashlib
from urllib.parse import urlparse, parse_qs
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler

# ==================== 【用户配置项】 ====================
PORT = 8844
SYNC_DATA_DIR = "/path/to/tt-sync/default-user"  # 请修改为你的 tt-sync 备份数据目录 (例如 /root/tt-sync/data)
CACHE_TTL = 2  # 默认缓存 2 秒；前端传入 ?refresh=1 时可强制立即重扫
# =======================================================

# 原版 TT-Sync 排除的目录前缀（相对于 default-user）
EXCLUDED_PREFIXES = (
    "user/lan-sync",
    "user/cache",
    "_cache",
    "_tauritavern/prompt-cache",
)

_last_scan_time = 0.0
_cached_result = None


def is_excluded_path(rel_path: str) -> bool:
    """严格对齐原版 TT-Sync (crates/ttsync-core/src/dataset/path.rs) 的排除规则"""
    norm = rel_path.replace("\\", "/").strip("/")
    if not norm:
        return True

    for prefix in EXCLUDED_PREFIXES:
        if norm == prefix or norm.startswith(prefix + "/"):
            return True

    parts = norm.split("/")
    for part in parts:
        if (
            part == ".staging"
            or part == ".git"
            or part == "__pycache__"
            or part.startswith(".tmp-")
            or part.endswith(".ttsync.tmp")
            or part.startswith(".")
        ):
            return True

    return False


def scan_manifest(folder_path: str, force: bool = False):
    """扫描备份目录，生成 Manifest 清单、SHA-256 指纹及最新文件元数据"""
    global _last_scan_time, _cached_result

    now = time.time()
    if not force and _cached_result is not None and (now - _last_scan_time < CACHE_TTL):
        res = dict(_cached_result)
        res["server_time"] = int(now)
        return res

    files_map = {}
    latest_mtime_ms = 0
    latest_file = "无"
    total_bytes = 0

    if os.path.exists(folder_path):
        try:
            for root, dirs, files in os.walk(folder_path):
                # 原地过滤隐藏/临时目录，加速遍历
                dirs[:] = [
                    d for d in dirs
                    if not d.startswith(".")
                    and d not in (".staging", "__pycache__", "_cache")
                    and not d.startswith(".tmp-")
                ]
                for f in files:
                    if f.startswith(".") or f.endswith(".ttsync.tmp") or f.startswith(".tmp-"):
                        continue
                    filepath = os.path.join(root, f)
                    rel_path = os.path.relpath(filepath, folder_path).replace("\\", "/")
                    if is_excluded_path(rel_path):
                        continue
                    try:
                        st = os.stat(filepath)
                        size = int(st.st_size)
                        mtime_ms = int(round(st.st_mtime * 1000))
                    except OSError:
                        continue

                    files_map[rel_path] = [size, mtime_ms]
                    total_bytes += size

                    if mtime_ms > latest_mtime_ms:
                        latest_mtime_ms = mtime_ms
                        latest_file = rel_path
        except Exception:
            pass
    else:
        latest_file = f"目录不存在: {folder_path}"

    # 排序并计算整体 Manifest 指纹与对话清单指纹
    sorted_paths = sorted(files_map.keys())
    manifest_hasher = hashlib.sha256()
    chats_hasher = hashlib.sha256()

    for p in sorted_paths:
        sz, mt_ms = files_map[p]
        mt_sec = mt_ms // 1000
        manifest_hasher.update(f"{p}|{sz}|{mt_sec}\n".encode("utf-8"))
        if p.startswith("chats/") or p.startswith("group chats/"):
            chats_hasher.update(f"{p}|{sz}\n".encode("utf-8"))

    int_time = latest_mtime_ms // 1000
    human_time = (
        datetime.fromtimestamp(int_time).strftime("%Y-%m-%d %H:%M:%S")
        if int_time > 0
        else "暂无记录"
    )

    _cached_result = {
        "version": 2,
        "manifest_hash": manifest_hasher.hexdigest() if sorted_paths else "",
        "chats_hash": chats_hasher.hexdigest() if sorted_paths else "",
        "files_count": len(sorted_paths),
        "total_bytes": total_bytes,
        "timestamp": int_time,
        "timestamp_ms": latest_mtime_ms,
        "human_time": human_time,
        "latest_file": latest_file,
        "server_time": int(now),
        "files": files_map,
    }
    _last_scan_time = now
    return _cached_result


class TTTrackerHandler(BaseHTTPRequestHandler):
    def _send_cors_headers(self):
        """发送完整的跨域头，支持所有前端 Origin 和 WebView 协议"""
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS, HEAD")
        self.send_header(
            "Access-Control-Allow-Headers",
            "Content-Type, Authorization, X-Requested-With, Origin, Accept, Cache-Control",
        )
        self.send_header("Access-Control-Max-Age", "86400")

    def do_OPTIONS(self):
        self.send_response(204)
        self._send_cors_headers()
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query)
        force_refresh = "refresh" in qs or "force" in qs

        data = scan_manifest(SYNC_DATA_DIR, force=force_refresh)
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")

        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
        self._send_cors_headers()
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass


def run():
    server_address = ("0.0.0.0", PORT)
    httpd = HTTPServer(server_address, TTTrackerHandler)
    print("=" * 60)
    print("[TT-Sync Tracker v2] 云端清单比对与侦测服务已就绪！")
    print(f"  - 监听端口 : {PORT}")
    print(f"  - 监控目录 : {SYNC_DATA_DIR}")
    print(f"  - 访问测试 : http://<你的服务器IP>:{PORT}")
    print("=" * 60)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n服务已平稳停止。")
        httpd.server_close()


if __name__ == "__main__":
    run()
