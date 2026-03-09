#!/bin/bash
# ═══════════════════════════════════════════════════════════
#  copy-rbac.sh — 复制源 SA 的 RBAC 角色分配到目标 SA
#
#  仅复制直接分配在 SA 上的角色，不复制从资源组/订阅继承的。
#
#  用法:
#    bash copy-rbac.sh              # 预览模式（只显示，不执行）
#    bash copy-rbac.sh apply        # 执行模式（创建角色分配）
#
#  前提:
#    - 已安装 Azure CLI (az) 并已登录 (az login / az cloud set)
#    - 登录账号需有:
#        源端: Reader + RBAC 读取权限
#        目标端: User Access Administrator 或 Owner
#    - 如果源端和目标端在不同订阅，可设置环境变量:
#        SRC_SUB="<源端订阅ID>" DST_SUB="<目标订阅ID>" bash copy-rbac.sh
# ═══════════════════════════════════════════════════════════

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/config.env"

MODE="${1:-preview}"
TIMELINE="${LOG_DIR}/timeline.log"

# 可选: 跨订阅场景通过环境变量指定
SRC_SUB="${SRC_SUB:-}"
DST_SUB="${DST_SUB:-}"

echo "═══════════════════════════════════════════════════════"
echo "  RBAC 角色分配复制"
echo "  源端: ${SRC_ACCOUNT} → 目标: ${DST_ACCOUNT}"
echo "  模式: $( [ "$MODE" = "apply" ] && echo "执行" || echo "预览" )"
echo "  $(date -u)"
echo "═══════════════════════════════════════════════════════"
echo ""

# ─── 1. 检查 Azure CLI ───
echo "[1/4] 检查 Azure CLI..."
if ! command -v az &>/dev/null; then
    echo "  [FAIL] 未安装 Azure CLI"
    echo "         安装: https://learn.microsoft.com/cli/azure/install-azure-cli"
    exit 1
fi

if ! az account show &>/dev/null 2>&1; then
    echo "  [FAIL] 未登录 Azure CLI"
    echo "         请先运行: az cloud set --name AzureChinaCloud && az login"
    exit 1
fi

CURRENT_USER=$(az account show --query user.name -o tsv 2>/dev/null)
CURRENT_SUB_NAME=$(az account show --query name -o tsv 2>/dev/null)
echo "  [OK] 已登录: ${CURRENT_USER}"
echo "  [OK] 当前订阅: ${CURRENT_SUB_NAME}"
echo ""

# ─── 2. 获取 SA Resource ID ───
echo "[2/4] 查找 Storage Account..."

# 构造查询命令（支持跨订阅）
src_sub_arg=""
dst_sub_arg=""
[ -n "$SRC_SUB" ] && src_sub_arg="--subscription ${SRC_SUB}"
[ -n "$DST_SUB" ] && dst_sub_arg="--subscription ${DST_SUB}"

SRC_ID=$(az storage account list $src_sub_arg \
    --query "[?name=='${SRC_ACCOUNT}'].id" -o tsv 2>/dev/null)
if [ -z "$SRC_ID" ]; then
    echo "  [FAIL] 找不到源端 SA: ${SRC_ACCOUNT}"
    echo "         请确认订阅正确: az account set -s <subscription-id>"
    echo "         或指定: SRC_SUB=\"<订阅ID>\" bash copy-rbac.sh"
    exit 1
fi
echo "  [OK] 源端: ${SRC_ID}"

DST_ID=$(az storage account list $dst_sub_arg \
    --query "[?name=='${DST_ACCOUNT}'].id" -o tsv 2>/dev/null)
if [ -z "$DST_ID" ]; then
    echo "  [FAIL] 找不到目标端 SA: ${DST_ACCOUNT}"
    echo "         或指定: DST_SUB=\"<订阅ID>\" bash copy-rbac.sh"
    exit 1
fi
echo "  [OK] 目标: ${DST_ID}"
echo ""

# ─── 3. 查询源端直接角色分配 ───
echo "[3/4] 查询源端角色分配..."

# --scope 返回该资源上的所有分配（含继承），用 JMESPath 过滤只保留直接分配的
ASSIGNMENTS_JSON=$(az role assignment list \
    --scope "$SRC_ID" \
    --query "[?scope=='${SRC_ID}']" \
    -o json $src_sub_arg 2>/dev/null)

# 解析为 TSV: principalId \t principalType \t roleDefinitionName \t principalName
TMPFILE=$(mktemp)
trap "rm -f $TMPFILE" EXIT

python3 -c "
import sys, json
data = json.load(sys.stdin)
for a in data:
    pid   = a.get('principalId', '')
    ptype = a.get('principalType', '')
    role  = a.get('roleDefinitionName', '')
    pname = a.get('principalName', '') or '(unknown)'
    if not pid or not role:
        continue
    print(f'{pid}\t{ptype}\t{role}\t{pname}')
" <<< "$ASSIGNMENTS_JSON" > "$TMPFILE"

TOTAL=$(wc -l < "$TMPFILE" | xargs)

if [ "$TOTAL" -eq 0 ]; then
    echo "  [INFO] 源端 SA 没有直接的角色分配"
    echo "         从资源组/订阅继承的分配不需要手动复制"
    exit 0
fi

echo "  [OK] 找到 ${TOTAL} 个直接角色分配:"
echo ""
echo "  ───────────────────────────────────────────────────"
idx=0
while IFS=$'\t' read -r pid ptype role pname; do
    idx=$((idx + 1))
    echo "  ${idx}. ${role}"
    echo "     主体: ${pname} (${ptype})"
    echo ""
done < "$TMPFILE"

# ─── 4. 执行或预览 ───
if [ "$MODE" != "apply" ]; then
    echo "═══════════════════════════════════════════════════════"
    echo "  [预览模式] 以上 ${TOTAL} 个角色分配将被复制到目标 SA"
    echo "  确认后执行: bash copy-rbac.sh apply"
    echo "═══════════════════════════════════════════════════════"
    exit 0
fi

echo "[4/4] 创建角色分配..."
echo ""

SUCCESS=0
SKIPPED=0
FAILED=0

while IFS=$'\t' read -r pid ptype role pname; do
    echo "  创建: ${role} → ${pname} (${ptype})"

    if output=$(az role assignment create \
        --assignee-object-id "$pid" \
        --assignee-principal-type "$ptype" \
        --role "$role" \
        --scope "$DST_ID" \
        $dst_sub_arg \
        -o none 2>&1); then
        echo "    [OK] 成功"
        SUCCESS=$((SUCCESS + 1))
    else
        if echo "$output" | grep -qi "already exists"; then
            echo "    [SKIP] 已存在"
            SKIPPED=$((SKIPPED + 1))
        else
            echo "    [FAIL] ${output}"
            FAILED=$((FAILED + 1))
        fi
    fi
done < "$TMPFILE"

echo ""
echo "═══════════════════════════════════════════════════════"
echo "  完成: 成功=${SUCCESS}  跳过(已存在)=${SKIPPED}  失败=${FAILED}  共=${TOTAL}"
echo "  $(date -u)"
echo "═══════════════════════════════════════════════════════"

echo "[$(date -u)] RBAC 复制完成: 成功=${SUCCESS} 跳过=${SKIPPED} 失败=${FAILED}" >> "$TIMELINE"
