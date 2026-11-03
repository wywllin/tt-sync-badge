(function () {
    const STORAGE_KEY_URL = "tt_sync_server_url";
    const BASELINE_PREFIX = "tt_adv_sync_baseline:";
    const ALIAS_PREFIX = "tauritavern:tt_sync_server_alias:";
    const SELECTION_STORAGE_KEY = "tauritavern:sync_dataset_selection";
    const LEGACY_SELECTION_KEY = "tauritavern:sync_v2_dataset_selection";

    // 运行时状态
    let trackerUrl = localStorage.getItem(STORAGE_KEY_URL) || "";
    let trackerAutoFilled = false;

    let syncStatus = {
        running: false,
        deviceName: "TauriTavern",
        syncMode: "Incremental",
        overwritePolicy: "exact",
    };
    let datasetCatalog = null;
    let pairedServers = [];
    let localChatsSnap = { fingerprint: "", count: 0, totalBytes: 0, maxDateMs: 0, map: {} };
    let cloudData = null;
    let cloudError = "";
    let isBusy = false;

    // 本地未同步改动追踪
    let localDirtyAtMs = 0;
    let localDirtyReason = "";

    // ───────────────────────── 工具函数 ─────────────────────────

    function getTauriInvoke() {
        return window.__TAURI__?.core?.invoke || null;
    }

    function getTauriListen() {
        return window.__TAURI__?.event?.listen || null;
    }

    function withArgAliases(args) {
        if (!args || typeof args !== "object") return args;
        const out = { ...args };
        for (const [k, v] of Object.entries(args)) {
            if (k.includes("_")) {
                const camel = k.replace(/_+([a-zA-Z0-9])/g, (_, c) => c.toUpperCase());
                if (!(camel in out)) out[camel] = v;
            } else {
                const snake = k.replace(/[A-Z]/g, (c) => `_${c.toLowerCase()}`);
                if (snake !== k && !(snake in out)) out[snake] = v;
            }
        }
        return out;
    }

    async function tauriInvoke(cmd, args) {
        const invoke = getTauriInvoke();
        if (!invoke) throw new Error("当前环境不支持 Tauri 原生调用");
        return args === undefined ? invoke(cmd) : invoke(cmd, withArgAliases(args));
    }

    function formatDateTime(ms) {
        if (!ms || ms <= 0) return "从未";
        const d = new Date(ms);
        if (Number.isNaN(d.getTime())) return "未知";
        const pad = (n) => String(n).padStart(2, "0");
        return `${d.getFullYear()}/${pad(d.getMonth() + 1)}/${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
    }

    function formatTimeAgo(ms) {
        if (!ms || ms <= 0) return "";
        const diffSec = Math.floor((Date.now() - ms) / 1000);
        if (diffSec < 5) return "刚刚";
        if (diffSec < 60) return `${diffSec}秒前`;
        if (diffSec < 3600) return `${Math.floor(diffSec / 60)}分钟前`;
        if (diffSec < 86400) return `${Math.floor(diffSec / 3600)}小时前`;
        return `${Math.floor(diffSec / 86400)}天前`;
    }

    function formatBytes(bytes) {
        const n = Number(bytes || 0);
        if (n < 1024) return `${n} B`;
        if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
        return `${(n / (1024 * 1024)).toFixed(2)} MB`;
    }

    function normalizeUrl(input) {
        let url = String(input || "").trim();
        if (!url) return "";
        if (!/^https?:\/\//i.test(url)) {
            url = "http://" + url;
        }
        return url.replace(/\/+$/, "");
    }

    // 清理旧版本遗留的悬浮球或扩展面板元素
    function cleanupLegacyElements() {
        for (const id of [
            "tt-sync-floating-badge",
            "tt-sync-detail-panel",
            "tt-sync-settings-wrapper",
        ]) {
            const el = document.getElementById(id);
            if (el) el.remove();
        }
    }

    // ───────────────────────── 基线存储 (Baseline) ─────────────────────────

    function loadBaseline(serverId) {
        if (!serverId) return null;
        try {
            const raw = localStorage.getItem(`${BASELINE_PREFIX}${serverId}`);
            return raw ? JSON.parse(raw) : null;
        } catch {
            return null;
        }
    }

    function saveBaseline(serverId, data) {
        if (!serverId) return;
        try {
            localStorage.setItem(`${BASELINE_PREFIX}${serverId}`, JSON.stringify(data));
        } catch (e) {
            console.warn("[高级同步] 保存基准快照失败:", e);
        }
    }

    // 将当前本地状态与云端状态对齐保存为“一致基准”
    function recordAlignedBaseline(server, cloud, localChats) {
        if (!server || !cloud) return;
        const cloudMtimeMs = cloud.timestamp_ms || (cloud.timestamp ? cloud.timestamp * 1000 : 0);
        const cloudFp = cloud.manifest_hash || `${cloud.timestamp || 0}_${cloud.latest_file || ""}`;
        localDirtyAtMs = 0;
        localDirtyReason = "";
        saveBaseline(server.server_device_id, {
            lastSyncMs: server.last_sync_ms || Date.now(),
            cloudFingerprint: cloudFp,
            cloudMtimeMs,
            cloudFiles: cloud.files || null,
            localChatsFingerprint: localChats?.fingerprint || "",
            localChatsMap: localChats?.map || null,
            alignedAtMs: Date.now(),
        });
    }

    // ───────────────────────── 数据采集与请求 ─────────────────────────

    // 获取本地实时对话清单 (通过 Tauri 原生接口获取所有对话的大小和修改时间)
    async function fetchLocalChatsInventory() {
        try {
            const [charChats, groupChats] = await Promise.all([
                tauriInvoke("list_chat_summaries", {
                    characterFilter: null,
                    includeMetadata: false,
                }).catch(() => []),
                tauriInvoke("list_group_chat_summaries", {
                    chatIds: null,
                    includeMetadata: false,
                }).catch(() => []),
            ]);

            const map = {};
            let totalBytes = 0;
            let maxDateMs = 0;

            for (const item of Array.isArray(charChats) ? charChats : []) {
                const charName = String(item.character_name || "").trim();
                const fileName = String(item.file_name || "").trim();
                const key = charName ? `chats/${charName}/${fileName}` : `chats/${fileName}`;
                const size = Number(item.file_size || 0);
                const dateMs = Number(item.date || 0);
                map[key] = [size, dateMs];
                totalBytes += size;
                if (dateMs > maxDateMs) maxDateMs = dateMs;
            }

            for (const item of Array.isArray(groupChats) ? groupChats : []) {
                const fileName = String(item.file_name || "").trim();
                const key = `group chats/${fileName}`;
                const size = Number(item.file_size || 0);
                const dateMs = Number(item.date || 0);
                map[key] = [size, dateMs];
                totalBytes += size;
                if (dateMs > maxDateMs) maxDateMs = dateMs;
            }

            const sortedKeys = Object.keys(map).sort();
            const fpParts = sortedKeys.map((k) => `${k}:${map[k][0]}`);
            const fingerprint = `${sortedKeys.length}|${totalBytes}|${fpParts.join(";")}`;

            localChatsSnap = {
                fingerprint,
                count: sortedKeys.length,
                totalBytes,
                maxDateMs,
                map,
            };
        } catch (err) {
            console.warn("[高级同步] 读取本地对话摘要失败:", err);
        }
        return localChatsSnap;
    }

    // 读取 Tauri 原生同步状态与已配对的 TT-Sync 服务器
    async function fetchTauriSyncState() {
        try {
            const [rawStatus, rawServers, rawCatalog] = await Promise.all([
                tauriInvoke("lan_sync_get_status").catch(() => null),
                tauriInvoke("tt_sync_list_servers").catch(() => []),
                tauriInvoke("sync_get_dataset_catalog").catch(() => null),
            ]);

            if (rawStatus) {
                syncStatus = {
                    running: Boolean(rawStatus.running),
                    deviceName: String(rawStatus.device_name || "TauriTavern"),
                    syncMode: rawStatus.sync_mode || "Incremental",
                    overwritePolicy: rawStatus.overwrite_policy || "exact",
                };
            }

            if (rawCatalog) {
                datasetCatalog = {
                    policyVersion: Number(rawCatalog.policy_version),
                    supportedDatasetIds: Array.isArray(rawCatalog.supported_dataset_ids)
                        ? rawCatalog.supported_dataset_ids.map(String)
                        : [],
                    defaultDatasetIds: Array.isArray(rawCatalog.default_dataset_ids)
                        ? rawCatalog.default_dataset_ids.map(String)
                        : [],
                };
            }

            pairedServers = Array.isArray(rawServers) ? rawServers : [];

            // 如果用户尚未手动设置 8844 侦测地址（或仍是 127.0.0.1），自动从已配对的 TT-Sync 服务器提取 IP！
            if (
                pairedServers.length > 0 &&
                (!trackerUrl || trackerUrl.includes("127.0.0.1") || trackerUrl.includes("localhost"))
            ) {
                try {
                    const parsed = new URL(pairedServers[0].base_url);
                    if (parsed.hostname) {
                        trackerUrl = `http://${parsed.hostname}:8844`;
                        trackerAutoFilled = true;
                        localStorage.setItem(STORAGE_KEY_URL, trackerUrl);
                    }
                } catch {}
            }
        } catch (err) {
            console.warn("[高级同步] 获取原生同步状态失败:", err);
        }
    }

    // 请求 8844 云端侦测服务（优先走 Tauri Rust 后端请求以彻底绕过 Mixed Content 与 CORS 限制）
    async function fetchCloudTracker(forceRefresh = false) {
        if (!trackerUrl) {
            cloudData = null;
            cloudError = "尚未配置插件侦测服务地址 (IP:8844)";
            return null;
        }

        const qs = `t=${Date.now()}${forceRefresh ? "&refresh=1" : ""}`;

        // 通道 1: 通过 Tauri 原生 Rust HTTP 客户端请求 (/manifest.png 后缀可直通 content_service 校验并拿到原始字节)
        if (getTauriInvoke()) {
            try {
                const targetUrl = `${trackerUrl}/manifest.png?${qs}`;
                const result = await tauriInvoke("download_external_import_url", { url: targetUrl });
                if (result && result.data) {
                    const bytes = new Uint8Array(result.data);
                    const text = new TextDecoder("utf-8").decode(bytes);
                    const parsed = JSON.parse(text);
                    cloudData = parsed;
                    cloudError = "";
                    return parsed;
                }
            } catch (rustErr) {
                console.debug("[高级同步] Rust 通道请求回退至 fetch:", rustErr);
            }
        }

        // 通道 2: 标准浏览器 fetch
        try {
            const controller = new AbortController();
            const timer = setTimeout(() => controller.abort(), 6000);
            const res = await fetch(`${trackerUrl}/?${qs}`, {
                method: "GET",
                cache: "no-store",
                signal: controller.signal,
            });
            clearTimeout(timer);

            if (!res.ok) {
                throw new Error(`HTTP ${res.status} ${res.statusText}`);
            }
            const parsed = await res.json();
            cloudData = parsed;
            cloudError = "";
            return parsed;
        } catch (err) {
            cloudData = null;
            const msg = err?.message || String(err);
            if (err?.name === "AbortError") {
                cloudError = `连接超时 (6秒无响应)：请检查服务器 ${trackerUrl} 是否已启动且防火墙放行了 8844 端口`;
            } else {
                cloudError = `无法连接侦测服务 (${trackerUrl})：${msg}`;
            }
            return null;
        }
    }

    // ───────────────────────── 核心三步比对算法 ─────────────────────────
    /**
     * 严格遵循原版 TT-Sync 思路：
     * 第一步：先比对本地与云端的数据清单是否一致（一致则直接显示“数据一致”，绝不误报）
     * 第二步：若不一致，找出差异文件，并比对修改时间判断“云端较新”还是“本地较新”
     */
    function evaluateSyncComparison(server) {
        if (!cloudData || cloudError) {
            return {
                state: "offline",
                badgeText: "⚪ 侦测未连接",
                title: "未获取云端侦测数据",
                detail: cloudError || "请检查下方插件侦测连接配置",
                recommend: null,
            };
        }

        const cloudMtimeMs =
            cloudData.timestamp_ms || (cloudData.timestamp ? cloudData.timestamp * 1000 : 0);
        const cloudFp =
            cloudData.manifest_hash ||
            `${cloudData.timestamp || 0}_${cloudData.latest_file || ""}`;
        const lastSyncMs = Number(server?.last_sync_ms || 0);

        let baseline = loadBaseline(server?.server_device_id);

        // 【智能基准自动对齐】：
        // 如果原版记录的本机“上次同步时间 (last_sync_ms)” 晚于或等于 云端最新文件的修改时间 (cloudMtimeMs)，
        // 说明云端没有任何文件是在本机上次同步之后修改的（例如你刚在 12:33 上传了 12:14 修改的文件）！
        if (lastSyncMs > 0 && lastSyncMs >= cloudMtimeMs) {
            if (
                !baseline ||
                lastSyncMs > (baseline.lastSyncMs || 0) ||
                (!baseline.cloudFiles && cloudData.files)
            ) {
                recordAlignedBaseline(server, cloudData, localChatsSnap);
                baseline = loadBaseline(server?.server_device_id);
            }
        }

        // ── 1. 检查云端相比上次同步基准是否有文件变化 ──
        let cloudChanged = false;
        let cloudDiffFiles = [];

        if (cloudData.files && baseline?.cloudFiles) {
            const curFiles = cloudData.files;
            const baseFiles = baseline.cloudFiles;
            const allPaths = new Set([...Object.keys(curFiles), ...Object.keys(baseFiles)]);
            for (const p of allPaths) {
                const cur = curFiles[p];
                const base = baseFiles[p];
                if (!cur || !base) {
                    cloudDiffFiles.push(p);
                } else {
                    const curSec = Math.floor(cur[1] / 1000);
                    const baseSec = Math.floor(base[1] / 1000);
                    if (cur[0] !== base[0] || curSec !== baseSec) {
                        cloudDiffFiles.push(p);
                    }
                }
            }
            cloudChanged = cloudDiffFiles.length > 0;
        } else if (baseline?.cloudFingerprint) {
            cloudChanged = cloudFp !== baseline.cloudFingerprint;
            if (cloudChanged && cloudData.latest_file) {
                cloudDiffFiles.push(cloudData.latest_file);
            }
        } else {
            // 尚无本地基准时，看云端最新文件时间是否晚于本机上次同步时间
            cloudChanged = cloudMtimeMs > lastSyncMs;
            if (cloudChanged && cloudData.latest_file) {
                cloudDiffFiles.push(cloudData.latest_file);
            }
        }

        // ── 2. 检查本机相比上次同步基准是否有新改动 ──
        let localChanged = false;
        let localDiffFiles = [];
        let localLatestEditMs = lastSyncMs;

        if (baseline?.localChatsMap && localChatsSnap.map) {
            const curChats = localChatsSnap.map;
            const baseChats = baseline.localChatsMap;
            const allChatKeys = new Set([...Object.keys(curChats), ...Object.keys(baseChats)]);
            for (const k of allChatKeys) {
                const cur = curChats[k];
                const base = baseChats[k];
                if (!cur || !base || cur[0] !== base[0]) {
                    localDiffFiles.push(k);
                    if (cur && cur[1] > localLatestEditMs) {
                        localLatestEditMs = cur[1];
                    }
                }
            }
        } else if (baseline?.localChatsFingerprint && localChatsSnap.fingerprint) {
            if (localChatsSnap.fingerprint !== baseline.localChatsFingerprint) {
                localDiffFiles.push("本地聊天记录");
                if (localChatsSnap.maxDateMs > localLatestEditMs) {
                    localLatestEditMs = localChatsSnap.maxDateMs;
                }
            }
        }

        if (localDirtyAtMs > (baseline?.lastSyncMs || lastSyncMs || 0)) {
            if (localDirtyReason && !localDiffFiles.includes(localDirtyReason)) {
                localDiffFiles.push(localDirtyReason);
            }
            if (localDirtyAtMs > localLatestEditMs) {
                localLatestEditMs = localDirtyAtMs;
            }
        }

        localChanged = localDiffFiles.length > 0;

        // ── 3. 如果既没有云端差异，也没有本地差异 -> 数据一致！ ──
        if (!cloudChanged && !localChanged) {
            const fileStats =
                typeof cloudData.files_count === "number"
                    ? `（共比对 ${cloudData.files_count} 个文件，${formatBytes(cloudData.total_bytes)}）`
                    : "";
            return {
                state: "synced",
                badgeText: "🟢 数据一致",
                title: "✅ 本地与云端数据完全一致（无需同步）",
                detail: `云端最新变动文件: ${cloudData.latest_file || "无"} (${cloudData.human_time || "未知"}) ${fileStats}`,
                recommend: null,
            };
        }

        // ── 4. 数据不一致时，根据变动方向与时间新旧给出提示 ──
        if (cloudChanged && !localChanged) {
            const diffDesc =
                cloudDiffFiles.length > 0
                    ? `变动文件 (${cloudDiffFiles.length}): ${cloudDiffFiles.slice(0, 3).join(", ")}${cloudDiffFiles.length > 3 ? " 等" : ""}`
                    : `最新变动文件: ${cloudData.latest_file}`;
            return {
                state: "cloud-newer",
                badgeText: "🟠 云端较新",
                title: `⚠️ 数据不一致 · 云端数据较新 (${formatTimeAgo(cloudMtimeMs)})，建议点击【下载】`,
                detail: `云端修改时间: ${cloudData.human_time} · ${diffDesc}`,
                recommend: "pull",
            };
        }

        if (!cloudChanged && localChanged) {
            const diffDesc = `本地变更 (${localDiffFiles.length}): ${localDiffFiles.slice(0, 3).join(", ")}${localDiffFiles.length > 3 ? " 等" : ""}`;
            return {
                state: "local-newer",
                badgeText: "🔵 本地较新",
                title: `⬆️ 数据不一致 · 本地数据较新 (${formatTimeAgo(localLatestEditMs || Date.now())})，建议点击【上传】`,
                detail: `${diffDesc} · 云端备份停留在 ${cloudData.human_time}`,
                recommend: "push",
            };
        }

        // 两端都有独立修改 (Conflict)
        const newerSide =
            cloudMtimeMs >= localLatestEditMs ? "云端最后修改时间更新" : "本地最后修改时间更新";
        return {
            state: "conflict",
            badgeText: "🔴 两端均有修改",
            title: `❗ 数据不一致 · 本地与云端均有独立修改（${newerSide}）`,
            detail: `云端变动: ${cloudDiffFiles.slice(0, 2).join(", ") || cloudData.latest_file} (${cloudData.human_time}) | 本地变动: ${localDiffFiles.slice(0, 2).join(", ")}`,
            recommend: cloudMtimeMs >= localLatestEditMs ? "pull" : "push",
        };
    }

    // ───────────────────────── 原生同步操作封装 ─────────────────────────

    function getEffectiveDatasetSelection() {
        for (const key of [SELECTION_STORAGE_KEY, LEGACY_SELECTION_KEY]) {
            const raw = localStorage.getItem(key);
            if (raw) {
                try {
                    const parsed = JSON.parse(raw);
                    if (parsed && Array.isArray(parsed.dataset_ids) && parsed.dataset_ids.length > 0) {
                        return parsed;
                    }
                } catch {}
            }
        }
        if (datasetCatalog) {
            return {
                policy_version: datasetCatalog.policyVersion,
                dataset_ids: [...datasetCatalog.defaultDatasetIds],
            };
        }
        return { policy_version: 2, dataset_ids: [] };
    }

    async function handleToggleSyncMode() {
        if (isBusy) return;
        const currentMode = syncStatus.syncMode || "Incremental";
        const nextMode = currentMode === "Mirror" ? "Incremental" : "Mirror";

        if (nextMode === "Mirror") {
            const ok = window.confirm(
                "切换为【镜像模式 (Mirror)】？\n\n注意：镜像模式会让目标端与发起方完全一致，目标端多出的文件将会被删除！"
            );
            if (!ok) return;
        }

        try {
            isBusy = true;
            renderModalContent();
            await tauriInvoke("lan_sync_set_sync_mode", { mode: nextMode, persist: true });
            syncStatus.syncMode = nextMode;
        } catch (err) {
            alert(`切换同步模式失败: ${err?.message || err}`);
        } finally {
            isBusy = false;
            await refreshAll(false);
        }
    }

    async function handleRenameServer(server) {
        if (!server) return;
        const id = server.server_device_id;
        const currentAlias = localStorage.getItem(`${ALIAS_PREFIX}${id}`) || "";
        const next = window.prompt(
            "重命名已配对服务器（仅本地显示，留空恢复默认名称）：",
            currentAlias || server.server_device_name || "TT-Sync"
        );
        if (next === null) return;
        const trimmed = next.trim();
        if (trimmed) {
            localStorage.setItem(`${ALIAS_PREFIX}${id}`, trimmed);
        } else {
            localStorage.removeItem(`${ALIAS_PREFIX}${id}`);
        }
        renderModalContent();
    }

    async function handleRemoveServer(server) {
        if (!server || isBusy) return;
        const displayName =
            localStorage.getItem(`${ALIAS_PREFIX}${server.server_device_id}`) ||
            server.server_device_name ||
            "TT-Sync";
        if (!window.confirm(`确定要移除服务器 "${displayName}" 吗？`)) return;

        try {
            isBusy = true;
            renderModalContent();
            await tauriInvoke("tt_sync_remove_server", {
                serverDeviceId: server.server_device_id,
            });
        } catch (err) {
            alert(`移除服务器失败: ${err?.message || err}`);
        } finally {
            isBusy = false;
            await refreshAll(false);
        }
    }

    async function showNativeOrFallbackReport(report) {
        try {
            const mod = await import("/scripts/tauri/setting/setting-panel/sync-listeners.js");
            if (typeof mod?.showSyncReportResult === "function") {
                await mod.showSyncReportResult(report);
                return;
            }
        } catch {}

        const res = report?.result;
        if (!res) return;
        if (res.status === "failed") {
            alert(`同步失败: ${res.message || "未知错误"}`);
            return;
        }
        if (res.status === "completed" && res.summary) {
            const isPull = report?.job?.intent !== "replicate_local_to_remote";
            const msg = [
                `TT-Sync ${isPull ? "下载" : "上传"}完成！`,
                `传输文件数: ${res.summary.files_total}`,
                res.summary.files_deleted > 0 ? `删除文件数: ${res.summary.files_deleted}` : null,
                `传输大小: ${formatBytes(res.summary.bytes_total)}`,
                isPull ? "\n页面即将刷新以加载最新数据。" : null,
            ]
                .filter(Boolean)
                .join("\n");
            alert(msg);
            if (isPull) {
                window.location.reload();
            }
        }
    }

    async function handlePullServer(server) {
        if (!server || isBusy) return;
        const cmp = evaluateSyncComparison(server);

        if (cmp.state === "local-newer" || cmp.state === "conflict") {
            const confirmPull = window.confirm(
                "⚠️ 覆盖风险提示：\n检测到本机有尚未上传的较新修改！\n如果现在执行【下载】，云端较旧的数据可能会覆盖本机修改。\n\n确定仍要从云端下载吗？"
            );
            if (!confirmPull) return;
        }

        try {
            isBusy = true;
            renderModalContent();

            const options = {
                selection: getEffectiveDatasetSelection(),
                overwrite_policy: syncStatus.overwritePolicy || "exact",
                require_bundle_zstd: true,
            };
            const mode = syncStatus.syncMode || "Incremental";
            const report = await tauriInvoke("tt_sync_pull", {
                serverDeviceId: server.server_device_id,
                mode,
                options,
            });

            if (report?.result?.status === "completed") {
                await Promise.all([
                    fetchTauriSyncState(),
                    fetchCloudTracker(true),
                    fetchLocalChatsInventory(),
                ]);
                const updatedServer =
                    pairedServers.find((s) => s.server_device_id === server.server_device_id) ||
                    server;
                recordAlignedBaseline(updatedServer, cloudData, localChatsSnap);
                renderModalContent();
            }

            await showNativeOrFallbackReport(report);
        } catch (err) {
            alert(`下载失败: ${err?.message || err}`);
        } finally {
            isBusy = false;
            await refreshAll(true);
        }
    }

    async function handlePushServer(server) {
        if (!server || isBusy) return;
        const cmp = evaluateSyncComparison(server);

        if (cmp.state === "cloud-newer" || cmp.state === "conflict") {
            const confirmPush = window.confirm(
                "⚠️ 覆盖风险提示：\n检测到云端备份存在比本机更新的数据！\n如果现在执行【上传】，本机数据将会覆盖云端较新的备份。\n\n确定仍要上传到云端吗？"
            );
            if (!confirmPush) return;
        }

        try {
            isBusy = true;
            renderModalContent();

            const options = {
                selection: getEffectiveDatasetSelection(),
                overwrite_policy: syncStatus.overwritePolicy || "exact",
                require_bundle_zstd: true,
            };
            const mode = syncStatus.syncMode || "Incremental";
            const report = await tauriInvoke("tt_sync_push", {
                serverDeviceId: server.server_device_id,
                mode,
                options,
            });

            if (report?.result?.status === "completed") {
                // 上传完成后立即强制刷新云端清单 (?refresh=1)，并将最新云端指纹记录为一致基准！
                await Promise.all([
                    fetchTauriSyncState(),
                    fetchCloudTracker(true),
                    fetchLocalChatsInventory(),
                ]);
                const updatedServer =
                    pairedServers.find((s) => s.server_device_id === server.server_device_id) ||
                    server;
                recordAlignedBaseline(updatedServer, cloudData, localChatsSnap);
                renderModalContent();
            }

            await showNativeOrFallbackReport(report);
        } catch (err) {
            alert(`上传失败: ${err?.message || err}`);
        } finally {
            isBusy = false;
            await refreshAll(true);
        }
    }

    // ───────────────────────── 界面渲染 (图一 & 图二) ─────────────────────────

    // 更新魔法棒菜单项上的小状态点
    function updateMenuShortcutDot() {
        const dot = document.getElementById("tt-adv-menu-dot");
        if (!dot) return;
        const server = pairedServers[0];
        if (!server || !cloudData || cloudError) {
            dot.style.background = "#9ca3af";
            return;
        }
        const cmp = evaluateSyncComparison(server);
        if (cmp.state === "synced") {
            dot.style.background = "#22c55e";
        } else if (cmp.state === "cloud-newer") {
            dot.style.background = "#f59e0b";
        } else if (cmp.state === "local-newer") {
            dot.style.background = "#3b82f6";
        } else {
            dot.style.background = "#ef4444";
        }
    }

    // 在图一（魔法棒菜单 #extensionsMenu）的“同步面板”正下方挂载“高级同步”
    function ensureWandMenuItem() {
        const menu = document.getElementById("extensionsMenu");
        if (!menu) return;

        let item = document.getElementById("tauritavern_shortcut_advanced_sync");
        if (!item) {
            item = document.createElement("div");
            item.id = "tauritavern_shortcut_advanced_sync";
            item.className = "list-group-item flex-container flexGap5";
            item.setAttribute("role", "button");
            item.setAttribute("tabindex", "0");
            item.innerHTML = `
                <div class="fa-fw fa-solid fa-cloud-arrow-up extensionsMenuExtensionButton"></div>
                <span style="flex: 1;">高级同步</span>
                <span id="tt-adv-menu-dot" style="width: 8px; height: 8px; border-radius: 50%; background: #9ca3af; display: inline-block; margin-right: 4px;"></span>
            `;
            item.addEventListener("click", (e) => {
                e.stopPropagation();
                menu.style.display = "none";
                openAdvancedSyncModal();
            });
        }

        const nativeSyncItem = document.getElementById("tauritavern_shortcut_sync");
        if (nativeSyncItem && nativeSyncItem.parentElement) {
            if (nativeSyncItem.nextElementSibling !== item) {
                nativeSyncItem.insertAdjacentElement("afterend", item);
            }
        } else {
            const shortcutsContainer = document.getElementById(
                "tauritavern_extensions_menu_shortcuts"
            );
            if (shortcutsContainer) {
                if (!shortcutsContainer.contains(item)) {
                    shortcutsContainer.appendChild(item);
                }
            } else if (!menu.contains(item)) {
                menu.prepend(item);
            }
        }
        updateMenuShortcutDot();
    }

    // 打开图二风格的高级同步弹窗
    function openAdvancedSyncModal() {
        let backdrop = document.getElementById("tt-adv-sync-backdrop");
        if (!backdrop) {
            backdrop = document.createElement("div");
            backdrop.id = "tt-adv-sync-backdrop";
            backdrop.addEventListener("click", (e) => {
                if (e.target === backdrop) {
                    closeAdvancedSyncModal();
                }
            });
            document.body.appendChild(backdrop);
        }
        backdrop.style.display = "flex";
        renderModalContent();
        void refreshAll(true);
    }

    function closeAdvancedSyncModal() {
        const backdrop = document.getElementById("tt-adv-sync-backdrop");
        if (backdrop) {
            backdrop.style.display = "none";
        }
    }

    function renderModalContent() {
        const backdrop = document.getElementById("tt-adv-sync-backdrop");
        if (!backdrop || backdrop.style.display === "none") return;

        const isMirror = syncStatus.syncMode === "Mirror";
        const modeLabel = isMirror ? "镜像模式" : "增量模式";
        const modeClass = isMirror ? "mode-mirror" : "mode-incremental";

        // 保留折叠状态
        const prevFoldOpen = document.getElementById("tt-adv-conn-fold")?.hasAttribute("open");
        const shouldOpenFold = prevFoldOpen ?? Boolean(cloudError || !trackerUrl);

        const cardsHtml =
            pairedServers.length === 0
                ? `<div class="tt-adv-device-card">
                       <div style="color: #9ca3af; font-size: 13px;">
                           暂未检测到已配对的 TT-Sync 云服务器，请先在原版同步面板完成服务器配对。
                       </div>
                   </div>`
                : pairedServers
                      .map((server, idx) => {
                          const id = server.server_device_id;
                          const displayName =
                              localStorage.getItem(`${ALIAS_PREFIX}${id}`) ||
                              server.server_device_name ||
                              "云";
                          const lastSyncText = formatDateTime(server.last_sync_ms);
                          const cmp = evaluateSyncComparison(server);
                          const cloudTimeText = cloudData?.human_time
                              ? `${cloudData.human_time} (${formatTimeAgo(
                                    cloudData.timestamp_ms || cloudData.timestamp * 1000
                                )})`
                              : "未获取";

                          return `
                              <div class="tt-adv-device-card" data-server-idx="${idx}">
                                  <div class="tt-adv-device-title-row">
                                      <button type="button" class="tt-adv-device-name-btn" data-action="rename" data-idx="${idx}" title="点击重命名" ${isBusy ? "disabled" : ""}>
                                          <i class="fa-solid fa-server"></i>
                                          <span>${escapeHtml(displayName)}</span>
                                          <i class="fa-solid fa-pen-to-square"></i>
                                      </button>
                                      <span style="font-size: 12px; font-weight: 600;">${cmp.badgeText}</span>
                                  </div>

                                  <div class="tt-adv-device-meta">
                                      <code>TT-Sync</code>
                                      <span>${escapeHtml(server.base_url || "")}</span>
                                  </div>

                                  <div class="tt-adv-device-meta">
                                      <span>上次同步: ${escapeHtml(lastSyncText)}</span>
                                      <span>·</span>
                                      <span>云端文件时间: ${escapeHtml(cloudTimeText)}</span>
                                  </div>

                                  <div class="tt-adv-compare-box ${cmp.state}">
                                      <div class="tt-adv-compare-header">
                                          <span>${escapeHtml(cmp.title)}</span>
                                          ${
                                              cmp.state !== "synced" && cmp.state !== "offline"
                                                  ? `<button type="button" class="tt-adv-btn" data-action="align" data-idx="${idx}" style="padding: 2px 7px; font-size: 11px;" title="将当前本地与云端状态标记为一致">视为一致</button>`
                                                  : ""
                                          }
                                      </div>
                                      <div class="tt-adv-compare-sub">${escapeHtml(cmp.detail)}</div>
                                  </div>

                                  <div class="tt-adv-device-actions">
                                      <button type="button" class="tt-adv-btn ${cmp.recommend === "pull" ? "recommend-action" : ""}" data-action="pull" data-idx="${idx}" ${isBusy ? "disabled" : ""}>
                                          <i class="fa-solid fa-download"></i>
                                          <span>下载</span>
                                      </button>
                                      <button type="button" class="tt-adv-btn ${cmp.recommend === "push" ? "recommend-action" : ""}" data-action="push" data-idx="${idx}" ${isBusy ? "disabled" : ""}>
                                          <i class="fa-solid fa-upload"></i>
                                          <span>上传</span>
                                      </button>
                                      <button type="button" class="tt-adv-btn icon-only" data-action="remove" data-idx="${idx}" title="移除服务器" ${isBusy ? "disabled" : ""}>
                                          <i class="fa-solid fa-trash-can"></i>
                                      </button>
                                  </div>
                              </div>
                          `;
                      })
                      .join("");

        const connSummary =
            cloudData && !cloudError
                ? `🟢 已连接 · ${trackerUrl}`
                : `🔴 未连接 · ${trackerUrl || "点击配置"}`;

        backdrop.innerHTML = `
            <div class="tt-adv-sync-modal" role="dialog" aria-modal="true">
                <div class="tt-adv-sync-body">
                    <!-- 顶部栏 (参照图二) -->
                    <div class="tt-adv-header">
                        <span class="tt-adv-header-title">高级同步</span>
                        <button type="button" class="tt-adv-btn ${modeClass}" id="tt-adv-btn-mode" ${isBusy ? "disabled" : ""}>
                            <i class="fa-solid fa-code-branch"></i>
                            <span>${modeLabel}</span>
                        </button>
                    </div>

                    <!-- 设备与同步区 (参照图二) -->
                    <div class="tt-adv-section">
                        <div class="tt-adv-section-header">
                            <span class="tt-adv-section-title">设备</span>
                            <button type="button" class="tt-adv-btn icon-only" id="tt-adv-btn-refresh" title="刷新并重新比对" ${isBusy ? "disabled" : ""}>
                                <i class="fa-solid fa-arrows-rotate ${isBusy ? "fa-spin" : ""}"></i>
                            </button>
                        </div>
                        ${cardsHtml}
                    </div>

                    <!-- 本插件连接配置 (折叠栏，参照图二下方折叠项) -->
                    <details class="tt-adv-fold" id="tt-adv-conn-fold" ${shouldOpenFold ? "open" : ""}>
                        <summary>
                            <span class="tt-adv-fold-title">插件侦测连接</span>
                            <span class="tt-adv-fold-summary">${escapeHtml(connSummary)}</span>
                            <i class="fa-solid fa-chevron-down tt-adv-fold-chevron"></i>
                        </summary>
                        <div class="tt-adv-fold-body">
                            <div style="font-size: 12px; color: #9ca3af;">
                                服务端侦测地址 (默认端口 8844，用于读取云端文件清单与修改时间)：
                            </div>
                            <div class="tt-adv-input-row">
                                <input type="text" id="tt-adv-input-url" class="tt-adv-input" value="${escapeHtml(trackerUrl)}" placeholder="http://123.45.67.89:8844" />
                                <button type="button" class="tt-adv-btn" id="tt-adv-btn-save-url" ${isBusy ? "disabled" : ""}>
                                    <i class="fa-solid fa-plug"></i>
                                    <span>连接</span>
                                </button>
                            </div>
                            ${
                                cloudError
                                    ? `<div class="tt-adv-diag">⚠️ ${escapeHtml(cloudError)}</div>`
                                    : `<div style="font-size: 12px; color: #86efac;">
                                           ✅ 侦测服务正常 (${
                                               cloudData?.version === 2
                                                   ? `v2 清单模式 · 共监控 ${cloudData.files_count} 个文件`
                                                   : "v1 时间戳模式 · 建议更新服务端 tt_sync_tracker.py 解锁完整文件清单比对"
                                           })
                                       </div>`
                            }
                        </div>
                    </details>
                </div>

                <!-- 底部通栏关闭按钮 (参照图二) -->
                <div class="tt-adv-footer">
                    <button type="button" class="tt-adv-close-btn" id="tt-adv-btn-close">关闭</button>
                </div>
            </div>
        `;

        // 绑定事件
        document.getElementById("tt-adv-btn-close")?.addEventListener("click", closeAdvancedSyncModal);
        document.getElementById("tt-adv-btn-mode")?.addEventListener("click", handleToggleSyncMode);
        document.getElementById("tt-adv-btn-refresh")?.addEventListener("click", () => {
            void refreshAll(true);
        });

        document.getElementById("tt-adv-btn-save-url")?.addEventListener("click", () => {
            const raw = document.getElementById("tt-adv-input-url")?.value || "";
            trackerUrl = normalizeUrl(raw);
            localStorage.setItem(STORAGE_KEY_URL, trackerUrl);
            void refreshAll(true);
        });

        backdrop.querySelectorAll("[data-action]").forEach((btn) => {
            btn.addEventListener("click", (e) => {
                const action = e.currentTarget.getAttribute("data-action");
                const idx = Number(e.currentTarget.getAttribute("data-idx"));
                const server = pairedServers[idx];
                if (!server) return;

                if (action === "rename") void handleRenameServer(server);
                else if (action === "pull") void handlePullServer(server);
                else if (action === "push") void handlePushServer(server);
                else if (action === "remove") void handleRemoveServer(server);
                else if (action === "align") {
                    recordAlignedBaseline(server, cloudData, localChatsSnap);
                    renderModalContent();
                    updateMenuShortcutDot();
                }
            });
        });
    }

    function escapeHtml(str) {
        return String(str ?? "")
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;");
    }

    // ───────────────────────── 全局刷新与事件监听 ─────────────────────────

    async function refreshAll(forceCloudScan = false) {
        await fetchTauriSyncState();
        await Promise.all([
            fetchCloudTracker(forceCloudScan),
            fetchLocalChatsInventory(),
        ]);
        updateMenuShortcutDot();
        renderModalContent();
    }

    function installEventListeners() {
        // 监听原版 TT-Sync 完成事件，当用户在原版面板或自动同步上传/下载完成时，自动同步更新基准
        window.addEventListener("tauritavern:tt-sync-servers-changed", async () => {
            await Promise.all([
                fetchTauriSyncState(),
                fetchCloudTracker(true),
                fetchLocalChatsInventory(),
            ]);
            if (pairedServers[0] && cloudData) {
                recordAlignedBaseline(pairedServers[0], cloudData, localChatsSnap);
            }
            updateMenuShortcutDot();
            renderModalContent();
        });

        const listen = getTauriListen();
        if (listen) {
            listen("sync:job", async (event) => {
                const report = event?.payload?.report;
                if (report?.result?.status === "completed") {
                    await Promise.all([
                        fetchTauriSyncState(),
                        fetchCloudTracker(true),
                        fetchLocalChatsInventory(),
                    ]);
                    if (pairedServers[0] && cloudData) {
                        recordAlignedBaseline(pairedServers[0], cloudData, localChatsSnap);
                    }
                    updateMenuShortcutDot();
                    renderModalContent();
                }
            }).catch(() => {});
        }

        // 监听魔法棒菜单 DOM 变化，确保“高级同步”始终固定在“同步面板”下方
        const observeMenu = () => {
            const menu = document.getElementById("extensionsMenu");
            if (!menu) {
                setTimeout(observeMenu, 500);
                return;
            }
            ensureWandMenuItem();
            const observer = new MutationObserver(() => {
                ensureWandMenuItem();
            });
            observer.observe(menu, { childList: true, subtree: true });
        };
        observeMenu();

        // 监听酒馆本地修改事件（发消息、改设置等），实时感知本地产生的新数据
        const hookSillyTavernEvents = async () => {
            try {
                const evMod = await import("/scripts/events.js");
                const { eventSource, event_types } = evMod || {};
                if (!eventSource || !event_types) return;

                const markDirty = (reason) => {
                    localDirtyAtMs = Date.now();
                    localDirtyReason = reason;
                    setTimeout(() => {
                        void fetchLocalChatsInventory().then(() => {
                            updateMenuShortcutDot();
                            renderModalContent();
                        });
                    }, 600);
                };

                for (const [evKey, label] of [
                    ["MESSAGE_SENT", "本地新发送消息"],
                    ["MESSAGE_RECEIVED", "本地新接收回复"],
                    ["MESSAGE_EDITED", "本地编辑了消息"],
                    ["MESSAGE_DELETED", "本地删除了消息"],
                    ["MESSAGE_SWIPED", "本地切换了消息分支"],
                    ["SETTINGS_UPDATED", "本地修改了设置/预设"],
                    ["CHARACTER_EDITED", "本地修改了角色卡"],
                    ["WORLDINFO_UPDATED", "本地修改了世界书"],
                ]) {
                    if (event_types[evKey]) {
                        eventSource.on(event_types[evKey], () => markDirty(label));
                    }
                }
            } catch {}
        };
        void hookSillyTavernEvents();
    }

    function init() {
        cleanupLegacyElements();
        ensureWandMenuItem();
        installEventListeners();
        void refreshAll(false);
        // 每 30 秒静默比对一次
        setInterval(() => {
            void refreshAll(false);
        }, 30000);
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", init);
    } else {
        init();
    }
})();
