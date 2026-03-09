#!/bin/bash
# ═══════════════════════════════════════════════════════════
#  3-migrate.sh — 全量迁移（自动分批 + 并发控制 + 排队调度）
#  读取 batch_plan.txt，按 MAX_PARALLEL_JOBS 控制并行数
#  前一个 Job 完成后自动启动排队的下一个 batch
#
#  用法:
#    bash 3-migrate.sh          # 启动迁移
#    bash 3-migrate.sh stop     # 停止调度器 + 所有 azcopy
#    bash 3-migrate.sh status   # 等同于 bash status.sh
#
#  特性:
#  - 调度器自动后台运行，Ctrl+C / SSH 断连不影响
#  - 防止重复启动
#  - 用 bash status.sh 查看进度
# ═══════════════════════════════════════════════════════════

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/config.env"

PLAN_FILE="${SCRIPT_DIR}/batch_plan.txt"
QUEUE_FILE="${LOG_DIR}/queue_status.txt"
TIMELINE="${LOG_DIR}/timeline.log"
SCHEDULER_PID_FILE="${LOG_DIR}/scheduler.pid"
SCHEDULER_LOG="${LOG_DIR}/scheduler.log"

mkdir -p "$LOG_DIR" "$PLAN_DIR"

# ─── 子命令: stop ───
if [ "${1:-}" = "stop" ]; then
    echo "正在停止迁移..."

    # 停调度器
    if [ -f "$SCHEDULER_PID_FILE" ]; then
        scheduler_pid=$(cat "$SCHEDULER_PID_FILE")
        if kill -0 "$scheduler_pid" 2>/dev/null; then
            kill "$scheduler_pid" 2>/dev/null || true
            echo "  [OK] 调度器已停止 (PID: ${scheduler_pid})"
        else
            echo "  [WARN] 调度器未在运行"
        fi
        rm -f "$SCHEDULER_PID_FILE"
    else
        echo "  [WARN] 未找到调度器 PID 文件"
    fi

    # 通过 PID 文件停掉每个 batch 的 azcopy 进程
    killed=0
    for pid_file in "${LOG_DIR}"/batch_*.pid; do
        [ -f "$pid_file" ] || continue
        batch_pid=$(cat "$pid_file")
        if kill -0 "$batch_pid" 2>/dev/null; then
            # 杀 nohup bash 及其子进程 azcopy
            pkill -P "$batch_pid" 2>/dev/null || true
            kill "$batch_pid" 2>/dev/null || true
            killed=$((killed + 1))
        fi
        rm -f "$pid_file"
    done
    if [ "$killed" -gt 0 ]; then
        echo "  [OK] 已停止 ${killed} 个 batch 进程"
    fi

    # 把 running 状态标记回 queued，方便重新启动
    if [ -f "$QUEUE_FILE" ]; then
        sed -i 's/|running$/|stopped/' "$QUEUE_FILE"
        echo ""
        echo "  运行中的批次已标记为 stopped"
        echo "  重新启动迁移: bash 3-migrate.sh"
    fi

    echo "[$(date -u)] 迁移手动停止" >> "$TIMELINE"
    exit 0
fi

# ─── 子命令: status ───
if [ "${1:-}" = "status" ]; then
    if [ ! -f "$QUEUE_FILE" ]; then
        echo "[FAIL] 找不到 queue_status.txt，迁移尚未启动"
        echo "请先运行: bash 3-migrate.sh"
        exit 1
    fi

    echo "═══════════════════════════════════════════════════════"
    echo "  迁移进度总览  $(date -u)"
    echo "  源端: ${SRC_ACCOUNT} → 目标: ${DST_ACCOUNT}"
    echo "  并行上限: ${MAX_PARALLEL_JOBS}"
    echo "═══════════════════════════════════════════════════════"
    echo ""

    while IFS='|' read -r batch_id status; do
        batch_id=$(echo "$batch_id" | xargs)
        status=$(echo "$status" | xargs)
        log="${LOG_DIR}/${batch_id}.log"

        case "$status" in
            completed) icon="[OK] 完成" ;;
            running)   icon="[RUNNING] 运行中" ;;
            failed)    icon="[FAIL] 失败" ;;
            queued)    icon="[QUEUED] 排队中" ;;
            stopped)   icon="[STOPPED] 已停止" ;;
            *)         icon="[?] ${status}" ;;
        esac

        echo "  ${batch_id}  ${icon}"

        # 显示运行中或已完成的 batch 的进度
        if [ -f "$log" ] && [ "$status" != "queued" ]; then
            progress=$(grep -oP '\d+\.\d+ %.*Total' "$log" 2>/dev/null | tail -1)
            if [ -n "$progress" ]; then
                echo "    └── ${progress}"
            fi

            # 如果已完成，显示最终统计
            if [ "$status" = "completed" ]; then
                final=$(grep -oP 'Number of File Transfers Completed: \K\d+' "$log" 2>/dev/null | tail -1)
                bytes=$(grep -oP 'Total Number of Bytes Transferred: \K\d+' "$log" 2>/dev/null | tail -1)
                if [ -n "$final" ]; then
                    echo "    └── 传输文件数: ${final}, 数据量: $(echo "scale=1; ${bytes:-0}/1024/1024/1024" | bc) GB"
                fi
            fi
        fi
        echo ""
    done < "$QUEUE_FILE"

    # 汇总
    running=$(grep -c '|running' "$QUEUE_FILE" 2>/dev/null) || true
    running=${running:-0}
    completed=$(grep -c '|completed' "$QUEUE_FILE" 2>/dev/null) || true
    completed=${completed:-0}
    failed=$(grep -c '|failed' "$QUEUE_FILE" 2>/dev/null) || true
    failed=${failed:-0}
    queued=$(grep -c '|queued' "$QUEUE_FILE" 2>/dev/null) || true
    queued=${queued:-0}
    stopped=$(grep -c '|stopped' "$QUEUE_FILE" 2>/dev/null) || true
    stopped=${stopped:-0}
    total=$((completed + failed + running + queued + stopped))

    # 调度器状态
    if [ -f "$SCHEDULER_PID_FILE" ] && kill -0 "$(cat "$SCHEDULER_PID_FILE")" 2>/dev/null; then
        sched_status="[OK] 运行中 (PID: $(cat "$SCHEDULER_PID_FILE"))"
    else
        sched_status="[STOPPED] 未运行"
    fi

    azcopy_procs=$(pgrep -c azcopy 2>/dev/null) || true
    azcopy_procs=${azcopy_procs:-0}

    echo "═══════════════════════════════════════════════════════"
    echo "  [OK] 完成: ${completed}  [RUNNING] 运行中: ${running}  [QUEUED] 排队: ${queued}  [FAIL] 失败: ${failed}  [STOPPED] 停止: ${stopped}  共: ${total}"
    echo "  调度器: ${sched_status}"
    echo "  azcopy 进程: ${azcopy_procs}"
    echo "═══════════════════════════════════════════════════════"
    exit 0
fi

# ─── 构造端点 ───
if [ "$STORAGE_TYPE" = "blob" ]; then
    SRC_BASE="https://${SRC_ACCOUNT}.blob.core.chinacloudapi.cn"
    DST_BASE="https://${DST_ACCOUNT}.blob.core.chinacloudapi.cn"
else
    SRC_BASE="https://${SRC_ACCOUNT}.file.core.chinacloudapi.cn"
    DST_BASE="https://${DST_ACCOUNT}.file.core.chinacloudapi.cn"
fi

# ─── 读取 batch plan ───
if [ ! -f "$PLAN_FILE" ]; then
    echo "[FAIL] 找不到 batch_plan.txt，请先运行 python3 2-inventory.py"
    exit 1
fi

mapfile -t BATCHES < <(grep -v '^#' "$PLAN_FILE" | grep -v '^$')
TOTAL=${#BATCHES[@]}

if [ "$TOTAL" -eq 0 ]; then
    echo "[FAIL] batch_plan.txt 中没有批次数据"
    exit 1
fi

# ─── 防止重复启动 ───
if [ -f "$SCHEDULER_PID_FILE" ]; then
    old_pid=$(cat "$SCHEDULER_PID_FILE")
    if kill -0 "$old_pid" 2>/dev/null; then
        echo "[WARN] 调度器已在运行中 (PID: ${old_pid})"
        echo "   查看进度: bash 3-migrate.sh status"
        echo "   查看调度日志: tail -f ${SCHEDULER_LOG}"
        echo ""
        echo "   如需重新启动，请先终止: bash 3-migrate.sh stop"
        exit 1
    else
        # 旧 PID 已不存在，清理
        rm -f "$SCHEDULER_PID_FILE"
    fi
fi

# ─── 恢复或初始化队列状态 ───
RESUME_MODE=false

if [ -f "$QUEUE_FILE" ]; then
    # 已有队列文件 — 检查是否有已完成的 batch（说明之前运行过）
    prev_completed=$(grep -c '|completed' "$QUEUE_FILE" 2>/dev/null) || true
    prev_completed=${prev_completed:-0}
    prev_total=$(wc -l < "$QUEUE_FILE" 2>/dev/null) || true
    prev_total=${prev_total:-0}

    if [ "$prev_completed" -gt 0 ] && [ "$prev_completed" -lt "$prev_total" ]; then
        # 有部分完成 — 进入恢复模式
        RESUME_MODE=true
        # 把 running / stopped 状态重置为 queued（VM 重启后这些 batch 需要重跑）
        sed -i 's/|running$/|queued/' "$QUEUE_FILE"
        sed -i 's/|stopped$/|queued/' "$QUEUE_FILE"
        sed -i 's/|failed$/|queued/' "$QUEUE_FILE"

        resume_completed=$(grep -c '|completed' "$QUEUE_FILE" 2>/dev/null) || true
        resume_completed=${resume_completed:-0}
        resume_queued=$(grep -c '|queued' "$QUEUE_FILE" 2>/dev/null) || true
        resume_queued=${resume_queued:-0}

        echo "═══════════════════════════════════════════════════════"
        echo "  [RESUME] 恢复模式 — 检测到之前的迁移进度"
        echo "  已完成: ${resume_completed}/${prev_total}  待恢复: ${resume_queued}"
        echo "  最大并行: ${MAX_PARALLEL_JOBS}"
        echo "  恢复时间: $(date -u)"
        echo "═══════════════════════════════════════════════════════"
        echo ""
        echo "[$(date -u)] 恢复迁移（已完成 ${resume_completed}，待恢复 ${resume_queued}）" >> "$TIMELINE"

    elif [ "$prev_completed" -eq "$prev_total" ] && [ "$prev_total" -gt 0 ]; then
        # 全部完成
        echo "[OK] 所有 ${prev_total} 个批次已完成，无需重新迁移"
        echo "   如需重新全量迁移，请先删除: rm ${QUEUE_FILE}"
        echo "   如需增量同步: bash 4-delta-sync.sh"
        exit 0
    else
        # 没有已完成的（全部 queued 或文件损坏），重新初始化
        RESUME_MODE=false
    fi
fi

if [ "$RESUME_MODE" = false ]; then
    # 全新初始化
    > "$QUEUE_FILE"
    for line in "${BATCHES[@]}"; do
        batch_id=$(echo "$line" | awk -F'|' '{print $1}' | xargs)
        echo "${batch_id}|queued" >> "$QUEUE_FILE"
    done

    echo "═══════════════════════════════════════════════════════"
    echo "  全量迁移启动"
    echo "  批次数: ${TOTAL}"
    echo "  最大并行: ${MAX_PARALLEL_JOBS}"
    echo "  开始时间: $(date -u)"
    echo "═══════════════════════════════════════════════════════"
    echo ""
    echo "全量迁移开始: $(date -u)" >> "$TIMELINE"
fi

echo "  批次列表:"
for line in "${BATCHES[@]}"; do
    bid=$(echo "$line" | awk -F'|' '{print $1}' | xargs)
    ctn=$(echo "$line" | awk -F'|' '{print $2}' | xargs)
    pth=$(echo "$line" | awk -F'|' '{print $3}' | xargs)
    cnt=$(echo "$line" | awk -F'|' '{print $4}' | xargs)
    # 显示当前状态（恢复模式下标记已完成的）
    batch_status=$(grep "^${bid}|" "$QUEUE_FILE" | awk -F'|' '{print $2}')
    if [ "$batch_status" = "completed" ]; then
        echo "    ${bid}: ${ctn}/${pth:-'(全部)'} (${cnt} 文件) [OK] 已完成，跳过"
    else
        echo "    ${bid}: ${ctn}/${pth:-'(全部)'} (${cnt} 文件)"
    fi
done
echo ""

# ═══════════════════════════════════════════════════════════
#  以下为后台调度器 — 通过 nohup 启动，SSH 断连不影响
# ═══════════════════════════════════════════════════════════

nohup bash -c '
SCRIPT_DIR="'"${SCRIPT_DIR}"'"
source "${SCRIPT_DIR}/config.env"

LOG_DIR="'"${LOG_DIR}"'"
PLAN_DIR="'"${PLAN_DIR}"'"
QUEUE_FILE="'"${QUEUE_FILE}"'"
TIMELINE="'"${TIMELINE}"'"
SCHEDULER_PID_FILE="'"${SCHEDULER_PID_FILE}"'"
SRC_BASE="'"${SRC_BASE}"'"
DST_BASE="'"${DST_BASE}"'"
SRC_SAS="'"${SRC_SAS}"'"
DST_SAS="'"${DST_SAS}"'"
STORAGE_TYPE="'"${STORAGE_TYPE}"'"
AZCOPY_CONCURRENCY="'"${AZCOPY_CONCURRENCY}"'"
MAX_PARALLEL_JOBS="'"${MAX_PARALLEL_JOBS}"'"

export AZCOPY_CONCURRENCY_VALUE=${AZCOPY_CONCURRENCY}
export AZCOPY_LOG_LOCATION=${LOG_DIR}
export AZCOPY_JOB_PLAN_LOCATION=${PLAN_DIR}

# 写入调度器 PID
echo $$ > "$SCHEDULER_PID_FILE"

echo "[$(date -u)] 调度器启动 (PID: $$)" >> "$TIMELINE"

# 读取 batch plan
mapfile -t BATCHES < <(grep -v "^#" "'"${PLAN_FILE}"'" | grep -v "^$")
TOTAL=${#BATCHES[@]}

# ─── 启动一个 batch ───
start_batch() {
    local line="$1"
    local batch_id=$(echo "$line" | awk -F"|" "{print \$1}" | xargs)
    local container=$(echo "$line" | awk -F"|" "{print \$2}" | xargs)
    local path=$(echo "$line" | awk -F"|" "{print \$3}" | xargs)
    local log_file="${LOG_DIR}/${batch_id}.log"

    # 构造 URL
    local extra_args="--recursive"
    local src_url dst_url

    if [ -z "$path" ] || [ "$path" = "(root_files)" ]; then
        src_url="${SRC_BASE}/${container}?${SRC_SAS}"
        dst_url="${DST_BASE}/${container}?${DST_SAS}"
        if [ "$path" = "(root_files)" ]; then
            extra_args="--recursive=false"
        fi
    else
        src_url="${SRC_BASE}/${container}/${path}?${SRC_SAS}"
        dst_url="${DST_BASE}/${container}/${path}?${DST_SAS}"
    fi

    # 确保目标容器存在
    if [ "$STORAGE_TYPE" = "blob" ]; then
        azcopy make "${DST_BASE}/${container}?${DST_SAS}" 2>/dev/null || true
    fi

    # 更新状态
    sed -i "s/^${batch_id}|queued$/${batch_id}|running/" "$QUEUE_FILE"

    echo "[$(date -u)] 启动 ${batch_id}: ${container}/${path}"

    # nohup 启动 azcopy，完成后自动更新状态
    nohup bash -c "
        export AZCOPY_CONCURRENCY_VALUE=${AZCOPY_CONCURRENCY}
        export AZCOPY_LOG_LOCATION=${LOG_DIR}
        export AZCOPY_JOB_PLAN_LOCATION=${PLAN_DIR}

        azcopy copy \
            \"${src_url}\" \
            \"${dst_url}\" \
            ${extra_args} \
            --check-length=true \
            --s2s-detect-source-changed \
            --log-level=ERROR

        exit_code=\$?
        if [ \$exit_code -eq 0 ]; then
            sed -i \"s/^${batch_id}|running$/${batch_id}|completed/\" \"${QUEUE_FILE}\"
            echo \"[\$(date -u)] [OK] ${batch_id} 完成\" >> \"${TIMELINE}\"
        else
            sed -i \"s/^${batch_id}|running$/${batch_id}|failed/\" \"${QUEUE_FILE}\"
            echo \"[\$(date -u)] [FAIL] ${batch_id} 失败 (exit code: \$exit_code)\" >> \"${TIMELINE}\"
        fi
    " > "${log_file}" 2>&1 &

    echo $! > "${LOG_DIR}/${batch_id}.pid"
}

# ─── 获取当前运行中的 batch 数量 ───
get_running_count() {
    local count
    count=$(grep -c "|running" "$QUEUE_FILE" 2>/dev/null) || true
    echo "${count:-0}"
}

# ─── 主调度循环 ───
batch_index=0

while true; do
    running=$(get_running_count)
    queued=$(grep -c "|queued" "$QUEUE_FILE" 2>/dev/null) || true
    queued=${queued:-0}

    # 所有 batch 都处理完
    if [ "$running" -eq 0 ] && [ "$queued" -eq 0 ]; then
        break
    fi

    # 有空闲槽位 + 还有排队的 batch → 启动
    while [ "$running" -lt "$MAX_PARALLEL_JOBS" ] && [ "$batch_index" -lt "$TOTAL" ]; do
        current_batch_id=$(echo "${BATCHES[$batch_index]}" | awk -F"|" "{print \$1}" | xargs)
        current_status=$(grep "^${current_batch_id}|" "$QUEUE_FILE" | awk -F"|" "{print \$2}")

        if [ "$current_status" = "queued" ]; then
            start_batch "${BATCHES[$batch_index]}"
            running=$((running + 1))
            sleep 2
        fi
        batch_index=$((batch_index + 1))
    done

    sleep 30
done

# ─── 汇总 ───
completed=$(grep -c "|completed" "$QUEUE_FILE" 2>/dev/null) || true
completed=${completed:-0}
failed=$(grep -c "|failed" "$QUEUE_FILE" 2>/dev/null) || true
failed=${failed:-0}

echo ""
echo "全量迁移结束: $(date -u) — 完成: ${completed}/${TOTAL} 失败: ${failed}/${TOTAL}"
echo "全量迁移结束: $(date -u) — 完成: ${completed}/${TOTAL} 失败: ${failed}/${TOTAL}" >> "$TIMELINE"

# 清理调度器 PID 文件
rm -f "$SCHEDULER_PID_FILE"

' > "${SCHEDULER_LOG}" 2>&1 &

SCHEDULER_PID=$!
echo "$SCHEDULER_PID" > "$SCHEDULER_PID_FILE"

echo "  [OK] 调度器已在后台启动 (PID: ${SCHEDULER_PID})"
echo ""
echo "  可以安全关闭终端或断开 SSH，迁移不受影响。"
echo ""
echo "  常用命令:"
echo "    bash 3-migrate.sh status           # 查看各 batch 进度"
echo "    bash 3-migrate.sh stop             # 停止迁移"
echo "    watch -n 60 bash 3-migrate.sh status  # 每 60 秒自动刷新"
echo "    tail -f ${SCHEDULER_LOG}           # 查看调度器日志"
echo "    tail -f ${LOG_DIR}/batch_001.log   # 查看某个 batch 日志"
echo ""
echo "═══════════════════════════════════════════════════════"
