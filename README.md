# TT-Sync 高级同步面板 (TT-Sync Advanced Sync)

一个专为 [TauriTavern](https://github.com/Darkatse/TauriTavern) 打造的增强型同步面板插件。

---

## ✨ 核心特性 (v2.0)

1. **🪄 魔法棒菜单快捷入口**：
   - 自动挂载在左下角魔法棒菜单（`#extensionsMenu`）的 **「同步面板」正下方**，显示为 **「高级同步」**（自带实时状态指示圆点）。
2. **🔍 先比对数据一致性，再看时间新旧（对齐原版 `TT-Sync` 逻辑）**：
   - **第一步（一致性比对）**：对比本地与云端的文件清单（`path`、`size_bytes`、`modified_ms`）及同步基准指纹。只要两边数据一致，始终显示 **🟢 数据一致（无需同步）**，杜绝刚上传完又误报“云端有新数据”。
   - **第二步（时间新旧判定）**：仅当检测到本地与云端数据不一致时，才根据差异文件的修改时间判断是 **🟠 云端较新（建议下载）**、**🔵 本地较新（建议上传）** 还是 **🔴 两端均有修改（冲突预警）**。
3. **⚡ 原生同步无缝整合**：
   - 界面复刻原版同步面板风格，精简聚焦，仅保留 **增量/镜像模式切换**、**云端服务器上传/下载（调用 Tauri 原生 `tt_sync_pull` / `tt_sync_push`）** 以及 **本插件侦测服务连接**。
   - **自动提取 IP**：自动从已配对的 `TT-Sync` 服务器地址提取 IP 并预填 `:8844` 侦测地址。
   - **Rust 原生通道穿透**：优先通过 Tauri Rust 底层发起侦测请求，彻底绕过 Android/桌面端 WebView 的 Mixed Content（HTTPS 页面请求 HTTP 接口）与 CORS 跨域拦截。

---

## 📦 安装与更新方式

在 TauriTavern 的 **扩展管理（拼图图标）** 中更新本扩展，或从 URL 安装：
```text
https://github.com/wywllin/tt-sync-badge.git
```

## 🖥️ 服务端配套脚本 (`server/tt_sync_tracker.py`)

将 `server/tt_sync_tracker.py` 部署到运行 `tt-sync` 的服务器上（默认端口 `8844`，请在脚本中配置你的备份目录），重启服务即可开启完整 `v2` 文件清单比对：
```bash
systemctl restart tt-tracker
```
