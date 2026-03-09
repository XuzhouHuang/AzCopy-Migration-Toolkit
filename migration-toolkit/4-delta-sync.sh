#!/bin/bash
# ═══════════════════════════════════════════════════════════
#  4-delta-sync.sh — 增量同步
#  在全量拷贝完成后执行，捕获源端新增/修改的文件
#  可反复执行多次，直到最终切换前的最后一次 delta sync
#
#  用法:
#    bash 4-delta-sync.sh          # 启动增量同步（自动后台）
#    bash 4-delta-sync.sh status   # 查看增量同步进度
#    bash 4-delta-sync.sh stop     # 停止增量同步
# ═══════════════════════════════════════════════════════════

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/config.env"

PLAN_FILE="${SCRIPT_DIR}/batch_plan.txt"
TIMELINE="${LOG_DIR}/timeline.log"
SYNC_COUNT_FILE="${LOG_DIR}/delta_sync_count.txt"
DELTA_PID_FILE="${LOG_DIR}/delta_scheduler.pid"
DELTA_LOG="${LOG_DIR}/delta_scheduler.log"

mkdir -p "$LOG_DIR" "$PLAN_DIR"

# ─── 构造端点 ───
if [ "$STORAGE_TYPE" = "blob" ]; then
    SRC_BASE="https://${SRC_ACCOUNT}.blob.core.chinacloudapi.cn"
    DST_BASE="https://${DST_ACCOUNT}.blob.core.chinacloudapi.cn"
else
    SRC_BASE="https://${SRC_ACCOUNT}.file.core.chinacloudapi.cn"
    DST_BASE="https://${DST_ACCOUNT}.file.core.chinacloudapi.cn"
fi

# ─── 子命令: stop ───
if [ "${1:-}" = "stop" ]; then
    echo "正在停止增量同步..."

    # 停调度器
    if [ -f "$DELTA_PID_FILE" ]; then
        delta_pid=$(cat "$DELTA_PID_FILE")
        if kill -0 "$delta_pid" 2>/dev/null; then
            kill "$delta_pid" 2>/dev/null || true
            echo "  [OK] 增量同步调度器已停止 (PID: ${delta_pid})"
        else
            echo "  [WARN] 增量同步调度器未在运行"
        fi
        rm -f "$DELTA_PID_FILE"
    else
        echo "  [WARN] 未找到调度器 PID 文件"
    fi

    # 通过 PID 文件停掉每个容器的 azcopy 进程
    killed=0
    # 查找最近一次 delta 的目录
    if [ -f "$SYNC_COUNT_FILE" ]; then
        last_sync=$(cat "$SYNC_COUNT_FILE")
        last_sync_dir="${LOG_DIR}/delta_${last_sync}"
        for pid_file in "${last_sync_dir}"/*.pid; do
            [ -f "$pid_file" ] || continue
            container_pid=$(cat "$pid_file")
            if kill -0 "$container_pid" 2>/dev/null; then
                pkill -P "$container_pid" 2>/dev/null || true
                kill "$container_pid" 2>/dev/null || true
                killed=$((killed + 1))
            fi
            rm -f "$pid_file"
        done
    fi
    if [ "$killed" -gt 0 ]; then
        echo "  [OK] 已停止 ${killed} 个容器同步进程"
    fi

    echo "[$(date -u)] 增量同步手动停止" >> "$TIMELINE"
    exit 0
fi

# ─── 子命令: status ───
if [ "${1:-}" = "status" ]; then
    # 当前第几次同步
    if [ -f "$SYNC_COUNT_FILE" ]; then
        sync_num=$(cat "$SYNC_COUNT_FILE")
    else
        echo "[FAIL] 尚未运行过增量同步"
        exit 1
    fi

    SYNC_QUEUE="${LOG_DIR}/delta_${sync_num}/queue_status.txt"

    if [ ! -f "$SYNC_QUEUE" ]; then
        echo "[FAIL] 找不到增量同步 #${sync_num} 的状态文件"
        exit 1
    fi

    # 调度器状态
    if [ -f "$DELTA_PID_FILE" ] && kill -0 "$(cat "$DELTA_PID_FILE")" 2>/dev/null; then
        sched_status="[OK] 运行中 (PID: $(cat "$DELTA_PID_FILE"))"
    else
        sched_status="[STOPPED] 未运行"
    fi

    echo "═══════════════════════════════════════════════════════"
    echo "  增量同步 #${sync_num} 进度  $(date -u)"
    echo "  源端: ${SRC_ACCOUNT} → 目标: ${DST_ACCOUNT}"
    echo "  调度器: ${sched_status}"
    echo "═══════════════════════════════════════════════════════"
    echo ""

    while IFS='|' read -r container status; do
        container=$(echo "$container" | xargs)
        status=$(echo "$status" | xargs)
        log="${LOG_DIR}/delta_${sync_num}/${container}.log"

        case "$status" in
            completed) icon="[OK] 完成" ;;
            running)   icon="[RUNNING] 运行中" ;;
            failed)    icon="[FAIL] 失败" ;;
            queued)    icon="[QUEUED] 排队中" ;;
            *)         icon="[?] ${status}" ;;
        esac

        echo "  ${container}  ${icon}"

        if [ -f "$log" ] && [ "$status" != "queued" ]; then
            progress=$(grep -oP '\d+\.\d+ %.*Total' "$log" 2>/dev/null | tail -1)
            if [ -n "$progress" ]; then
                echo "    └── ${progress}"
            fi
        fi
        echo ""
    done < "$SYNC_QUEUE"

    completed=$(grep -c '|completed' "$SYNC_QUEUE" 2>/dev/null) || true
    completed=${completed:-0}
    running=$(grep -c '|running' "$SYNC_QUEUE" 2>/dev/null) || true
    running=${running:-0}
    failed=$(grep -c '|failed' "$SYNC_QUEUE" 2>/dev/null) || true
    failed=${failed:-0}
    queued=$(grep -c '|queued' "$SYNC_QUEUE" 2>/dev/null) || true
    queued=${queued:-0}
    total=$((completed + running + failed + queued))

    azcopy_procs=$(pgrep -c azcopy 2>/dev/null) || true
    azcopy_procs=${azcopy_procs:-0}

    echo "═══════════════════════════════════════════════════════"
    echo "  [OK] 完成: ${completed}  [RUNNING] 运行中: ${running}  [QUEUED] 排队: ${queued}  [FAIL] 失败: ${failed}  共: ${total}"
    echo "  azcopy 进程: ${azcopy_procs}"
    echo "═══════════════════════════════════════════════════════"
    exit 0
fi

# ─── 防止重复启动 ───
if [ -f "$DELTA_PID_FILE" ]; then
    old_pid=$(cat "$DELTA_PID_FILE")
    if kill -0 "$old_pid" 2>/dev/null; then
        echo "[WARN] 增量同步已在运行中 (PID: ${old_pid})"
        echo "   查看进度: bash 4-delta-sync.sh status"
        echo "   停止同步: bash 4-delta-sync.sh stop"
        exit 1
    else
        rm -f "$DELTA_PID_FILE"
    fi
fi

# ─── 获取容器列表 ───
# 如果指定了 CONTAINER_NAME，只同步该容器
# 否则实时查询源端所有容器（不依赖 batch_plan.txt，能捕获新建容器）
declare -A CONTAINERS

if [ -n "${CONTAINER_NAME:-}" ]; then
    CONTAINERS["$CONTAINER_NAME"]=1
    echo "  指定容器: ${CONTAINER_NAME}"
else
    echo "  正在查询源端所有容器..."
    # 用 Python SDK 实时查询（inventory 步骤已安装 SDK）
    if [ "$STORAGE_TYPE" = "blob" ]; then
        container_list=$(python3 -c "
from azure.storage.blob import BlobServiceClient
svc = BlobServiceClient(account_url='${SRC_BASE}', credential='${SRC_SAS}')
for c in svc.list_containers():
    print(c.name)
" 2>/dev/null)
    else
        container_list=$(python3 -c "
from azure.storage.fileshare import ShareServiceClient
svc = ShareServiceClient(account_url='${SRC_BASE}', credential='${SRC_SAS}')
for s in svc.list_shares():
    print(s.name)
" 2>/dev/null)
    fi

    if [ -n "$container_list" ]; then
        while IFS= read -r cname; do
            [ -n "$cname" ] && CONTAINERS["$cname"]=1
        done <<< "$container_list"
    fi

    # 如果 Python 查询失败，回退到 batch_plan.txt
    if [ ${#CONTAINERS[@]} -eq 0 ]; then
        echo "  [WARN] 实时查询失败，回退到 batch_plan.txt"
        if [ ! -f "$PLAN_FILE" ]; then
            echo "  [FAIL] 找不到 batch_plan.txt，请先运行 python3 2-inventory.py"
            exit 1
        fi
        while IFS='|' read -r batch_id container path count size; do
            container=$(echo "$container" | xargs)
            CONTAINERS["$container"]=1
        done < <(grep -v '^#' "$PLAN_FILE" | grep -v '^$')
    fi
fi

TOTAL=${#CONTAINERS[@]}

if [ "$TOTAL" -eq 0 ]; then
    echo "  [FAIL] 未找到任何容器"
    exit 1
fi

# ─── 增量同步计数 ───
if [ -f "$SYNC_COUNT_FILE" ]; then
    SYNC_NUM=$(cat "$SYNC_COUNT_FILE")
    SYNC_NUM=$((SYNC_NUM + 1))
else
    SYNC_NUM=1
fi
echo "$SYNC_NUM" > "$SYNC_COUNT_FILE"

SYNC_LOG_DIR="${LOG_DIR}/delta_${SYNC_NUM}"
SYNC_QUEUE="${SYNC_LOG_DIR}/queue_status.txt"
mkdir -p "$SYNC_LOG_DIR"

# 初始化队列
> "$SYNC_QUEUE"
for container in "${!CONTAINERS[@]}"; do
    echo "${container}|queued" >> "$SYNC_QUEUE"
done

echo "═══════════════════════════════════════════════════════"
echo "  增量同步 #${SYNC_NUM}"
echo "  容器数: ${TOTAL}"
echo "  最大并行: ${MAX_PARALLEL_JOBS}"
echo "  开始时间: $(date -u)"
echo "═══════════════════════════════════════════════════════"
echo ""
echo "  容器列表:"
for container in "${!CONTAINERS[@]}"; do
    echo "    ${container}"
done
echo ""

echo "增量同步 #${SYNC_NUM} 开始: $(date -u)" >> "$TIMELINE"

# ═══════════════════════════════════════════════════════════
#  后台调度器
# ═══════════════════════════════════════════════════════════

nohup bash -c '
SCRIPT_DIR="'"${SCRIPT_DIR}"'"
source "${SCRIPT_DIR}/config.env"

LOG_DIR="'"${LOG_DIR}"'"
PLAN_DIR="'"${PLAN_DIR}"'"
TIMELINE="'"${TIMELINE}"'"
DELTA_PID_FILE="'"${DELTA_PID_FILE}"'"
SRC_BASE="'"${SRC_BASE}"'"
DST_BASE="'"${DST_BASE}"'"
SRC_SAS="'"${SRC_SAS}"'"
DST_SAS="'"${DST_SAS}"'"
STORAGE_TYPE="'"${STORAGE_TYPE}"'"
AZCOPY_CONCURRENCY="'"${AZCOPY_CONCURRENCY}"'"
MAX_PARALLEL_JOBS="'"${MAX_PARALLEL_JOBS}"'"
SYNC_NUM="'"${SYNC_NUM}"'"
SYNC_LOG_DIR="'"${SYNC_LOG_DIR}"'"
SYNC_QUEUE="'"${SYNC_QUEUE}"'"

export AZCOPY_CONCURRENCY_VALUE=${AZCOPY_CONCURRENCY}
export AZCOPY_LOG_LOCATION=${LOG_DIR}
export AZCOPY_JOB_PLAN_LOCATION=${PLAN_DIR}

echo $$ > "$DELTA_PID_FILE"
echo "[$(date -u)] 增量同步 #${SYNC_NUM} 调度器启动 (PID: $$)" >> "$TIMELINE"

# 读取容器列表
mapfile -t CONTAINER_LIST < <(awk -F"|" "{print \$1}" "$SYNC_QUEUE" | xargs -I{} echo {})
TOTAL=${#CONTAINER_LIST[@]}

# ─── 启动一个容器的同步 ───
start_sync() {
    local container="$1"
    local log_file="${SYNC_LOG_DIR}/${container}.log"
    local src_url="${SRC_BASE}/${container}?${SRC_SAS}"
    local dst_url="${DST_BASE}/${container}?${DST_SAS}"

    sed -i "s/^${container}|queued$/${container}|running/" "$SYNC_QUEUE"

    echo "[$(date -u)] 同步 ${container}"

    nohup bash -c "
        export AZCOPY_CONCURRENCY_VALUE=${AZCOPY_CONCURRENCY}
        export AZCOPY_LOG_LOCATION=${LOG_DIR}
        export AZCOPY_JOB_PLAN_LOCATION=${PLAN_DIR}

        azcopy copy \
            \"${src_url}\" \
            \"${dst_url}\" \
            --recursive \
            --overwrite ifSourceNewer \
            --s2s-detect-source-changed \
            --log-level=ERROR

        exit_code=\$?
        if [ \$exit_code -eq 0 ]; then
            sed -i \"s/^${container}|running$/${container}|completed/\" \"${SYNC_QUEUE}\"
            echo \"[\$(date -u)] [OK] delta#${SYNC_NUM} ${container} 完成\" >> \"${TIMELINE}\"
        else
            sed -i \"s/^${container}|running$/${container}|failed/\" \"${SYNC_QUEUE}\"
            echo \"[\$(date -u)] [FAIL] delta#${SYNC_NUM} ${container} 失败 (exit code: \$exit_code)\" >> \"${TIMELINE}\"
        fi
    " > "${log_file}" 2>&1 &

    echo $! > "${SYNC_LOG_DIR}/${container}.pid"
}

# ─── 调度循环 ───
container_index=0

while true; do
    running=$(grep -c "|running" "$SYNC_QUEUE" 2>/dev/null) || true
    running=${running:-0}
    queued=$(grep -c "|queued" "$SYNC_QUEUE" 2>/dev/null) || true
    queued=${queued:-0}

    if [ "$running" -eq 0 ] && [ "$queued" -eq 0 ]; then
        break
    fi

    while [ "$running" -lt "$MAX_PARALLEL_JOBS" ] && [ "$container_index" -lt "$TOTAL" ]; do
        current_container="${CONTAINER_LIST[$container_index]}"
        current_status=$(grep "^${current_container}|" "$SYNC_QUEUE" | awk -F"|" "{print \$2}")

        if [ "$current_status" = "queued" ]; then
            start_sync "$current_container"
            running=$((running + 1))
            sleep 2
        fi
        container_index=$((container_index + 1))
    done

    sleep 15
done

# ─── 汇总 ───
completed=$(grep -c "|completed" "$SYNC_QUEUE" 2>/dev/null) || true
completed=${completed:-0}
failed=$(grep -c "|failed" "$SYNC_QUEUE" 2>/dev/null) || true
failed=${failed:-0}

echo ""
echo "增量同步 #${SYNC_NUM} 结束: $(date -u) — 成功: ${completed}/${TOTAL} 失败: ${failed}/${TOTAL}"
echo "增量同步 #${SYNC_NUM} 结束: $(date -u) — 成功: ${completed}/${TOTAL} 失败: ${failed}/${TOTAL}" >> "$TIMELINE"

rm -f "$DELTA_PID_FILE"

' > "${DELTA_LOG}" 2>&1 &

DELTA_PID=$!
echo "$DELTA_PID" > "$DELTA_PID_FILE"

echo "  [OK] 增量同步调度器已在后台启动 (PID: ${DELTA_PID})"
echo ""
echo "  可以安全关闭终端或断开 SSH，同步不受影响。"
echo ""
echo "  常用命令:"
echo "    bash 4-delta-sync.sh status          # 查看进度"
echo "    bash 4-delta-sync.sh stop            # 停止同步"
echo "    tail -f ${DELTA_LOG}                 # 查看调度器日志"
echo ""
echo "  下一步:"
echo "    - 源端还有写入 → 稍后再次运行: bash 4-delta-sync.sh"
echo "    - 源端已停写入 → 运行最后一次 delta，然后: bash 5-validate.sh"
echo "═══════════════════════════════════════════════════════"
