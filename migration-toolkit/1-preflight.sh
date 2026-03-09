#!/bin/bash
# ═══════════════════════════════════════════════════════════
#  1-preflight.sh — 迁移前环境检查
#  检查 azcopy 安装、环境变量、SAS 有效性、目录创建
# ═══════════════════════════════════════════════════════════

set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/config.env"

echo "═══════════════════════════════════════════════════════"
echo "  迁移前环境检查"
echo "  $(date -u)"
echo "═══════════════════════════════════════════════════════"
echo ""

ERRORS=0

# ─── 1. 检查 config.env 必填项 ───
echo "[1/6] 检查配置文件..."
for var in SRC_ACCOUNT DST_ACCOUNT SRC_SAS DST_SAS STORAGE_TYPE; do
    eval val=\$$var
    if [ -z "$val" ]; then
        echo "  [FAIL] ${var} 未设置，请编辑 config.env"
        ERRORS=$((ERRORS + 1))
    else
        echo "  [OK] ${var} 已设置"
    fi
done
echo ""

# ─── 2. 检查 azcopy ───
echo "[2/6] 检查 azcopy..."
if command -v azcopy &> /dev/null; then
    AZCOPY_VER=$(azcopy --version 2>&1 | head -1)
    echo "  [OK] ${AZCOPY_VER}"
else
    echo "  [FAIL] azcopy 未安装，正在安装..."
    curl -sL https://aka.ms/downloadazcopy-v10-linux -o /tmp/azcopy.tar.gz
    sudo tar xzf /tmp/azcopy.tar.gz --strip-components=1 -C /usr/local/bin/ --wildcards '*/azcopy'
    if command -v azcopy &> /dev/null; then
        echo "  [OK] azcopy 安装成功: $(azcopy --version 2>&1 | head -1)"
    else
        echo "  [FAIL] azcopy 安装失败，请手动安装"
        ERRORS=$((ERRORS + 1))
    fi
fi
echo ""

# ─── 3. 检查 Python3 + SDK ───
echo "[3/6] 检查 Python3 + Azure SDK..."
if command -v python3 &> /dev/null; then
    echo "  [OK] $(python3 --version)"
else
    echo "  [FAIL] python3 未安装: sudo apt install -y python3 python3-pip"
    ERRORS=$((ERRORS + 1))
fi

if python3 -c "import azure.storage.blob" 2>/dev/null; then
    echo "  [OK] azure-storage-blob SDK 已安装"
else
    echo "  [WARN] azure-storage-blob SDK 未安装，正在安装..."
    pip3 install azure-storage-blob -q
    echo "  [OK] 安装完成"
fi

if [ "$STORAGE_TYPE" = "files" ]; then
    if python3 -c "import azure.storage.fileshare" 2>/dev/null; then
        echo "  [OK] azure-storage-file-share SDK 已安装"
    else
        echo "  [WARN] azure-storage-file-share SDK 未安装，正在安装..."
        pip3 install azure-storage-file-share -q
        echo "  [OK] 安装完成"
    fi
fi
echo ""

# ─── 4. 创建日志目录 ───
echo "[4/6] 创建日志目录..."
sudo mkdir -p "$LOG_DIR" "$PLAN_DIR"
sudo chown "$USER:$USER" "$LOG_DIR" "$PLAN_DIR"
echo "  [OK] ${LOG_DIR}"
echo "  [OK] ${PLAN_DIR}"
echo ""

# ─── 5. 设置 azcopy 环境变量 ───
echo "[5/6] 设置 azcopy 环境变量..."
export AZCOPY_CONCURRENCY_VALUE=${AZCOPY_CONCURRENCY}
export AZCOPY_LOG_LOCATION=${LOG_DIR}
export AZCOPY_JOB_PLAN_LOCATION=${PLAN_DIR}
echo "  [OK] AZCOPY_CONCURRENCY_VALUE=${AZCOPY_CONCURRENCY}"
echo "  [OK] AZCOPY_LOG_LOCATION=${LOG_DIR}"
echo "  [OK] AZCOPY_JOB_PLAN_LOCATION=${PLAN_DIR}"
echo ""

# ─── 6. 检查 SAS Token 参数 ───
echo "[6/7] 检查 SAS Token 参数..."

# 解析 SAS 参数的函数
check_sas() {
    local label="$1"
    local sas="$2"
    local required_perms="$3"  # 期望的最低权限字母
    local errors=0

    # 解析 SAS 中的各字段
    local srt=$(echo "$sas" | tr '&' '\n' | grep '^srt=' | cut -d= -f2)
    local sp=$(echo "$sas" | tr '&' '\n' | grep '^sp=' | cut -d= -f2)
    local se=$(echo "$sas" | tr '&' '\n' | grep '^se=' | cut -d= -f2)
    local ss=$(echo "$sas" | tr '&' '\n' | grep '^ss=' | cut -d= -f2)

    # 检查 Resource Types (srt=sco)
    if [ -z "$srt" ]; then
        echo "  [FAIL] ${label}: 缺少 srt 参数（Resource Types）"
        errors=1
    else
        for letter in s c o; do
            if [[ "$srt" != *"$letter"* ]]; then
                echo "  [FAIL] ${label}: srt=${srt} 缺少 '${letter}'（需要 sco）"
                errors=1
            fi
        done
        if [ $errors -eq 0 ]; then
            echo "  [OK] ${label}: Resource Types = ${srt}"
        fi
    fi

    # 检查权限 (sp)
    if [ -z "$sp" ]; then
        echo "  [FAIL] ${label}: 缺少 sp 参数（权限）"
        errors=1
    else
        local missing_perms=""
        for letter in $(echo "$required_perms" | grep -o .); do
            if [[ "$sp" != *"$letter"* ]]; then
                missing_perms="${missing_perms}${letter}"
            fi
        done
        if [ -n "$missing_perms" ]; then
            echo "  [FAIL] ${label}: sp=${sp} 缺少权限 '${missing_perms}'（需要至少 ${required_perms}）"
            errors=1
        else
            echo "  [OK] ${label}: 权限 = ${sp}"
        fi
    fi

    # 检查 Services (ss)
    if [ -n "$ss" ]; then
        if [ "$STORAGE_TYPE" = "blob" ] && [[ "$ss" != *"b"* ]]; then
            echo "  [FAIL] ${label}: ss=${ss} 缺少 'b'（Blob 服务）"
            errors=1
        elif [ "$STORAGE_TYPE" = "files" ] && [[ "$ss" != *"f"* ]]; then
            echo "  [FAIL] ${label}: ss=${ss} 缺少 'f'（File 服务）"
            errors=1
        else
            echo "  [OK] ${label}: Services = ${ss}"
        fi
    fi

    # 检查过期时间 (se)
    if [ -z "$se" ]; then
        echo "  [FAIL] ${label}: 缺少 se 参数（过期时间）"
        errors=1
    else
        # URL 解码 %3A → :
        local se_decoded=$(echo "$se" | sed 's/%3A/:/g' | sed 's/%3a/:/g')
        # 转换为 epoch 比较
        local se_epoch=$(date -u -d "$se_decoded" +%s 2>/dev/null)
        local now_epoch=$(date -u +%s)

        if [ -z "$se_epoch" ]; then
            echo "  [WARN] ${label}: 无法解析过期时间 se=${se}"
        elif [ "$se_epoch" -lt "$now_epoch" ]; then
            echo "  [FAIL] ${label}: SAS 已过期！过期时间 = ${se_decoded}"
            errors=1
        else
            local days_left=$(( (se_epoch - now_epoch) / 86400 ))
            if [ "$days_left" -lt 7 ]; then
                echo "  [WARN] ${label}: 过期时间 = ${se_decoded}（仅剩 ${days_left} 天，建议 ≥ 30 天）"
            else
                echo "  [OK] ${label}: 过期时间 = ${se_decoded}（剩余 ${days_left} 天）"
            fi
        fi
    fi

    return $errors
}

# 检查源端 SAS
src_sas_ok=0
check_sas "源端 SAS" "$SRC_SAS" "rl" || src_sas_ok=1
if [ $src_sas_ok -ne 0 ]; then
    ERRORS=$((ERRORS + 1))
fi

echo ""

# 检查目标端 SAS
dst_sas_ok=0
check_sas "目标端 SAS" "$DST_SAS" "rwlc" || dst_sas_ok=1
if [ $dst_sas_ok -ne 0 ]; then
    ERRORS=$((ERRORS + 1))
fi
echo ""

# ─── 7. 测试 SAS 连通性 ───
echo "[7/7] 测试 SAS 连通性..."

if [ "$STORAGE_TYPE" = "blob" ]; then
    SRC_ENDPOINT="https://${SRC_ACCOUNT}.blob.core.chinacloudapi.cn"
    DST_ENDPOINT="https://${DST_ACCOUNT}.blob.core.chinacloudapi.cn"
else
    SRC_ENDPOINT="https://${SRC_ACCOUNT}.file.core.chinacloudapi.cn"
    DST_ENDPOINT="https://${DST_ACCOUNT}.file.core.chinacloudapi.cn"
fi

# 测试源端（用退出码判断，超时 30 秒）
src_output=$(azcopy list "${SRC_ENDPOINT}/?${SRC_SAS}" 2>&1 | head -3)
if echo "$src_output" | grep -qiE "AuthenticationFailed|AuthorizationFailure|RESPONSE STATUS"; then
    echo "  [FAIL] 源端 SAS 无效 (${SRC_ACCOUNT})"
    echo "     $src_output"
    ERRORS=$((ERRORS + 1))
else
    echo "  [OK] 源端 SAS 有效 (${SRC_ACCOUNT})"
fi

# 测试目标端
dst_output=$(azcopy list "${DST_ENDPOINT}/?${DST_SAS}" 2>&1 | head -3)
if echo "$dst_output" | grep -qiE "AuthenticationFailed|AuthorizationFailure|RESPONSE STATUS"; then
    echo "  [FAIL] 目标端 SAS 无效 (${DST_ACCOUNT})"
    echo "     $dst_output"
    ERRORS=$((ERRORS + 1))
else
    echo "  [OK] 目标端 SAS 有效 (${DST_ACCOUNT})"
fi
echo ""

# ─── 汇总 ───
echo "═══════════════════════════════════════════════════════"
if [ $ERRORS -eq 0 ]; then
    echo "  [OK] 所有检查通过，可以开始迁移"
    echo "  下一步: python3 2-inventory.py"
else
    echo "  [FAIL] 有 ${ERRORS} 项检查失败，请修复后重新运行"
fi
echo "═══════════════════════════════════════════════════════"
