#!/bin/bash
# ═══════════════════════════════════════════════════════════
#  6-report.sh — 生成迁移报告
#  汇总所有步骤的日志，输出一页 summary
# ═══════════════════════════════════════════════════════════

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/config.env"

PLAN_FILE="${SCRIPT_DIR}/batch_plan.txt"
QUEUE_FILE="${LOG_DIR}/queue_status.txt"
TIMELINE="${LOG_DIR}/timeline.log"
VALIDATE_RESULT="${LOG_DIR}/validate/result.txt"
REPORT_FILE="${LOG_DIR}/migration_report.txt"

# ─── 从 batch_plan.txt 读取总计 ───
total_info=$(grep '^# 总计:' "$PLAN_FILE" 2>/dev/null || echo "# 总计: N/A")
total_batches=$(grep -c -v '^#' "$PLAN_FILE" 2>/dev/null | grep -v '^0$' || echo "0")

# ─── 从 queue_status.txt 读取批次结果 ───
completed=$(grep -c '|completed' "$QUEUE_FILE" 2>/dev/null) || true
completed=${completed:-0}
failed=$(grep -c '|failed' "$QUEUE_FILE" 2>/dev/null) || true
failed=${failed:-0}

# ─── 从 timeline 读取时间线 ───
migrate_start=$(grep '全量迁移开始' "$TIMELINE" 2>/dev/null | head -1 || echo "N/A")
migrate_end=$(grep '全量迁移结束' "$TIMELINE" 2>/dev/null | tail -1 || echo "N/A")
delta_count=$(grep -c '增量同步.*开始' "$TIMELINE" 2>/dev/null) || true
delta_count=${delta_count:-0}

# ─── 从验证结果读取 ───
if [ -f "$VALIDATE_RESULT" ]; then
    validate_pass=$(grep -c '\[OK\] PASS' "$VALIDATE_RESULT" 2>/dev/null) || true
    validate_pass=${validate_pass:-0}
    validate_fail=$(grep -c '\[FAIL\] FAIL' "$VALIDATE_RESULT" 2>/dev/null) || true
    validate_fail=${validate_fail:-0}
    if [ "$validate_fail" -eq 0 ]; then
        validate_status="[OK] PASS"
    else
        validate_status="[FAIL] FAIL (${validate_fail} 项不通过)"
    fi
else
    validate_status="[WARN] 未执行"
fi

# ─── 生成报告 ───
cat > "$REPORT_FILE" << REPORT
═══════════════════════════════════════════════════════════
  Azure Storage 跨区域迁移报告
  生成时间: $(date -u)
═══════════════════════════════════════════════════════════

  源端 SA:    ${SRC_ACCOUNT} (${SRC_REGION})
  目标端 SA:  ${DST_ACCOUNT} (${DST_REGION})
  存储类型:   ${STORAGE_TYPE}
  容器范围:   ${CONTAINER_NAME:-"(全部)"}

───────────────────────────────────────────────────────────
  数据概况
───────────────────────────────────────────────────────────
  ${total_info#\# }
  迁移批次:   ${total_batches} 个
  并行上限:   ${MAX_PARALLEL_JOBS}

───────────────────────────────────────────────────────────
  全量迁移
───────────────────────────────────────────────────────────
  ${migrate_start}
  ${migrate_end}
  批次结果:   完成 ${completed} / 失败 ${failed} / 共 ${total_batches}

───────────────────────────────────────────────────────────
  增量同步
───────────────────────────────────────────────────────────
  执行次数:   ${delta_count}

REPORT

# 添加增量同步详情
if [ "$delta_count" -gt 0 ]; then
    grep '增量同步' "$TIMELINE" 2>/dev/null >> "$REPORT_FILE"
fi

cat >> "$REPORT_FILE" << REPORT

───────────────────────────────────────────────────────────
  验证结果
───────────────────────────────────────────────────────────
  总体结果:   ${validate_status}

REPORT

# 添加验证详情
if [ -f "$VALIDATE_RESULT" ]; then
    echo "  详情:" >> "$REPORT_FILE"
    while IFS= read -r line; do
        echo "    ${line}" >> "$REPORT_FILE"
    done < "$VALIDATE_RESULT"
fi

cat >> "$REPORT_FILE" << REPORT

───────────────────────────────────────────────────────────
  时间线
───────────────────────────────────────────────────────────
REPORT

if [ -f "$TIMELINE" ]; then
    while IFS= read -r line; do
        echo "  ${line}" >> "$REPORT_FILE"
    done < "$TIMELINE"
fi

cat >> "$REPORT_FILE" << REPORT

═══════════════════════════════════════════════════════════
  报告结束
═══════════════════════════════════════════════════════════
REPORT

# ─── 输出到终端 ───
cat "$REPORT_FILE"

echo ""
echo "报告已保存到: ${REPORT_FILE}"
