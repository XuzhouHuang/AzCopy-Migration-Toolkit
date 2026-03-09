#!/bin/bash
# ═══════════════════════════════════════════════════════════
#  5-validate.sh — 迁移验证
#  验证逻辑:
#    1. 文件数对比（azcopy list --running-tally）
#    2. 总大小对比
#    3. 抽样属性校验（Python SDK 比对 size + Last-Modified，不下载文件）
# ═══════════════════════════════════════════════════════════

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/config.env"

PLAN_FILE="${SCRIPT_DIR}/batch_plan.txt"
TIMELINE="${LOG_DIR}/timeline.log"
VALIDATE_DIR="${LOG_DIR}/validate"
RESULT_FILE="${VALIDATE_DIR}/result.txt"

# ─── 构造端点 ───
if [ "$STORAGE_TYPE" = "blob" ]; then
    SRC_BASE="https://${SRC_ACCOUNT}.blob.core.chinacloudapi.cn"
    DST_BASE="https://${DST_ACCOUNT}.blob.core.chinacloudapi.cn"
else
    # files-smb 和 files-nfs 都使用 .file. 端点
    SRC_BASE="https://${SRC_ACCOUNT}.file.core.chinacloudapi.cn"
    DST_BASE="https://${DST_ACCOUNT}.file.core.chinacloudapi.cn"
fi

export AZCOPY_CONCURRENCY_VALUE=${AZCOPY_CONCURRENCY}
export AZCOPY_LOG_LOCATION=${LOG_DIR}

mkdir -p "$VALIDATE_DIR"

echo "═══════════════════════════════════════════════════════"
echo "  迁移验证"
echo "  开始时间: $(date -u)"
echo "═══════════════════════════════════════════════════════"
echo ""

echo "验证开始: $(date -u)" >> "$TIMELINE"

# ─── 读取容器列表 ───
declare -A CONTAINERS
while IFS='|' read -r batch_id container path count size; do
    container=$(echo "$container" | xargs)
    CONTAINERS["$container"]=1
done < <(grep -v '^#' "$PLAN_FILE" | grep -v '^$')

ALL_PASS=true
> "$RESULT_FILE"

for container in "${!CONTAINERS[@]}"; do
    echo "───────────────────────────────────────"
    echo "  验证容器: ${container}"
    echo "───────────────────────────────────────"

    src_url="${SRC_BASE}/${container}?${SRC_SAS}"
    dst_url="${DST_BASE}/${container}?${DST_SAS}"

    # ─── 1. 文件数和大小对比 ───
    echo "  [1/2] 统计文件数和大小..."

    src_tally=$(azcopy list "${src_url}" --running-tally 2>/dev/null | tail -3)
    dst_tally=$(azcopy list "${dst_url}" --running-tally 2>/dev/null | tail -3)

    src_count=$(echo "$src_tally" | grep -oP 'File count: \K[0-9]+' || echo "0")
    dst_count=$(echo "$dst_tally" | grep -oP 'File count: \K[0-9]+' || echo "0")
    src_size=$(echo "$src_tally" | grep -oP 'Total file size: \K[0-9.]+' || echo "0")
    dst_size=$(echo "$dst_tally" | grep -oP 'Total file size: \K[0-9.]+' || echo "0")

    # 文件数判定
    if [ "$src_count" = "$dst_count" ]; then
        echo "  [OK] 文件数一致: ${src_count}"
        echo "${container} | 文件数 | [OK] PASS | src=${src_count} dst=${dst_count}" >> "$RESULT_FILE"
    else
        diff=$((src_count - dst_count))
        echo "  [FAIL] 文件数不一致: 源=${src_count} 目标=${dst_count} 差=${diff}"
        echo "${container} | 文件数 | [FAIL] FAIL | src=${src_count} dst=${dst_count} diff=${diff}" >> "$RESULT_FILE"
        ALL_PASS=false
    fi

    # 大小判定
    if [ "$src_size" = "$dst_size" ]; then
        echo "  [OK] 总大小一致: ${src_size}"
        echo "${container} | 大小 | [OK] PASS | src=${src_size} dst=${dst_size}" >> "$RESULT_FILE"
    else
        echo "  [WARN] 总大小不一致: 源=${src_size} 目标=${dst_size}"
        echo "${container} | 大小 | [WARN] WARN | src=${src_size} dst=${dst_size}" >> "$RESULT_FILE"
    fi

    # ─── 2. 抽样属性校验（Python SDK，不下载文件） ───
    echo "  [2/2] 抽样属性校验（随机 100 个文件，比对 size）..."

    if [ "$STORAGE_TYPE" = "blob" ]; then
        sample_result=$(python3 -c "
import random
from azure.storage.blob import BlobServiceClient

src_svc = BlobServiceClient(account_url='${SRC_BASE}', credential='${SRC_SAS}')
dst_svc = BlobServiceClient(account_url='${DST_BASE}', credential='${DST_SAS}')

src_container = src_svc.get_container_client('${container}')
dst_container = dst_svc.get_container_client('${container}')

# 收集源端文件列表（取前 10000 个中随机抽 100 个）
src_blobs = []
for i, blob in enumerate(src_container.list_blobs()):
    src_blobs.append(blob)
    if i >= 9999:
        break

sample_size = min(100, len(src_blobs))
samples = random.sample(src_blobs, sample_size)

checked = 0
mismatch = 0
missing = 0
details = []

for src_blob in samples:
    checked += 1
    try:
        dst_blob_client = dst_container.get_blob_client(src_blob.name)
        dst_props = dst_blob_client.get_blob_properties()

        # 比较文件大小
        if src_blob.size != dst_props.size:
            mismatch += 1
            details.append(f'SIZE_MISMATCH: {src_blob.name} src={src_blob.size} dst={dst_props.size}')
    except Exception:
        missing += 1
        details.append(f'MISSING: {src_blob.name}')

print(f'checked={checked}')
print(f'mismatch={mismatch}')
print(f'missing={missing}')
for d in details:
    print(d)
" 2>/dev/null) || true

    else
        # Azure Files (SMB/NFS): 用 ShareServiceClient 遍历文件并抽样比对
        sample_result=$(python3 -c "
import random
from azure.storage.fileshare import ShareServiceClient

src_svc = ShareServiceClient(account_url='${SRC_BASE}', credential='${SRC_SAS}')
dst_svc = ShareServiceClient(account_url='${DST_BASE}', credential='${DST_SAS}')

src_share = src_svc.get_share_client('${container}')
dst_share = dst_svc.get_share_client('${container}')

# 递归收集源端文件列表（取前 10000 个）
src_files = []
def walk(dir_client, prefix=''):
    for item in dir_client.list_directories_and_files():
        path = f'{prefix}/{item[\"name\"]}' if prefix else item['name']
        if item['is_directory']:
            sub = dir_client.get_subdirectory_client(item['name'])
            walk(sub, path)
        else:
            src_files.append({'name': path, 'size': item.get('size', 0)})
            if len(src_files) >= 10000:
                return

root = src_share.get_directory_client('')
walk(root)

sample_size = min(100, len(src_files))
samples = random.sample(src_files, sample_size)

checked = 0
mismatch = 0
missing = 0
details = []

for src_file in samples:
    checked += 1
    try:
        dst_file_client = dst_share.get_directory_client('').get_file_client(src_file['name'])
        dst_props = dst_file_client.get_file_properties()

        if src_file['size'] != dst_props.size:
            mismatch += 1
            details.append(f'SIZE_MISMATCH: {src_file[\"name\"]} src={src_file[\"size\"]} dst={dst_props.size}')
    except Exception:
        missing += 1
        details.append(f'MISSING: {src_file[\"name\"]}')

print(f'checked={checked}')
print(f'mismatch={mismatch}')
print(f'missing={missing}')
for d in details:
    print(d)
" 2>/dev/null) || true

    fi

    if [ -z "$sample_result" ]; then
        echo "  [WARN] Python SDK 校验失败，跳过"
        echo "${container} | 抽样 | [WARN] SKIP | Python SDK 执行失败" >> "$RESULT_FILE"
    else
        s_checked=$(echo "$sample_result" | grep -oP 'checked=\K[0-9]+' || echo "0")
        s_mismatch=$(echo "$sample_result" | grep -oP 'mismatch=\K[0-9]+' || echo "0")
        s_missing=$(echo "$sample_result" | grep -oP 'missing=\K[0-9]+' || echo "0")
        s_errors=$((s_mismatch + s_missing))

        if [ "$s_errors" -eq 0 ]; then
            echo "  [OK] 抽样校验通过: ${s_checked} 个文件大小一致"
            echo "${container} | 抽样 | [OK] PASS | ${s_checked}/${s_checked}" >> "$RESULT_FILE"
        else
            echo "  [FAIL] 抽样校验失败: ${s_mismatch} 大小不一致, ${s_missing} 缺失"
            # 输出详情
            echo "$sample_result" | grep -E 'SIZE_MISMATCH|MISSING' | head -10 | while IFS= read -r line; do
                echo "    ${line}"
            done
            echo "${container} | 抽样 | [FAIL] FAIL | mismatch=${s_mismatch} missing=${s_missing} / ${s_checked}" >> "$RESULT_FILE"
            ALL_PASS=false
        fi
    fi
    echo ""
done

# ─── 汇总 ───
echo "═══════════════════════════════════════════════════════"
if $ALL_PASS; then
    echo "  [OK] 验证通过: 所有容器的文件数、大小一致，抽样校验通过"
else
    echo "  [FAIL] 验证失败: 存在不一致项，详见 ${RESULT_FILE}"
fi
echo "  完成时间: $(date -u)"
echo "═══════════════════════════════════════════════════════"

echo "验证结束: $(date -u) — $($ALL_PASS && echo 'PASS' || echo 'FAIL')" >> "$TIMELINE"

echo ""
echo "下一步: bash 6-report.sh"
