#!/usr/bin/env python3
"""
2-inventory.py — 源端数据盘点 + 自动生成分批迁移计划
统计每个容器的文件数和大小，按 MAX_FILES_PER_BATCH / MAX_GB_PER_BATCH 自动拆分为多个 batch
输出 batch_plan.txt 供 3-migrate.sh 使用
"""

import os
import sys
import time
from collections import defaultdict

# ─── 读取 config.env ───
def load_config(config_path):
    config = {}
    with open(config_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' in line:
                key, _, value = line.partition('=')
                key = key.strip()
                # 去掉行内注释（不在引号内的 #）
                # 先检查是否有引号包裹
                value = value.strip()
                if (value.startswith('"') and '"' in value[1:]):
                    # 双引号包裹：取第一对引号之间的内容
                    value = value[1:value.index('"', 1)]
                elif (value.startswith("'") and "'" in value[1:]):
                    # 单引号包裹：取第一对引号之间的内容
                    value = value[1:value.index("'", 1)]
                else:
                    # 无引号：# 之后为注释
                    if '#' in value:
                        value = value[:value.index('#')]
                    value = value.strip().strip('"').strip("'")
                config[key] = value.strip()
    return config

script_dir = os.path.dirname(os.path.abspath(__file__))
config = load_config(os.path.join(script_dir, 'config.env'))

SRC_ACCOUNT = config['SRC_ACCOUNT']
SRC_SAS = config['SRC_SAS']
STORAGE_TYPE = config.get('STORAGE_TYPE', 'blob')
CONTAINER_NAME = config.get('CONTAINER_NAME', '')
MAX_FILES = int(config.get('MAX_FILES_PER_BATCH', '8000000'))
MAX_BYTES = int(config.get('MAX_GB_PER_BATCH', '10000')) * 1024**3  # GB → bytes
DELTA_SYNC_RATIO = float(config.get('DELTA_SYNC_RATIO', '0.01'))  # 每次增量同步数据比例

# ─── Azure SDK 导入 ───
if STORAGE_TYPE == 'blob':
    from azure.storage.blob import BlobServiceClient
    ENDPOINT = f"https://{SRC_ACCOUNT}.blob.core.chinacloudapi.cn"
    service = BlobServiceClient(account_url=ENDPOINT, credential=SRC_SAS)
elif STORAGE_TYPE == 'files':
    from azure.storage.fileshare import ShareServiceClient
    ENDPOINT = f"https://{SRC_ACCOUNT}.file.core.chinacloudapi.cn"
    service = ShareServiceClient(account_url=ENDPOINT, credential=SRC_SAS)
else:
    print(f"[FAIL] 不支持的 STORAGE_TYPE: {STORAGE_TYPE}")
    sys.exit(1)

# ─── 获取容器/共享列表 ───
def get_containers():
    if CONTAINER_NAME:
        return [CONTAINER_NAME]
    if STORAGE_TYPE == 'blob':
        return [c.name for c in service.list_containers()]
    else:
        return [s.name for s in service.list_shares()]

# ─── 统计 Blob 容器 ───
def inventory_blob_container(container_name):
    """返回 {顶层目录: {count, bytes}} 和 总计"""
    container = service.get_container_client(container_name)
    dir_stats = defaultdict(lambda: {'count': 0, 'bytes': 0})
    total_count = 0
    total_bytes = 0
    start = time.time()

    print(f"  正在枚举 {container_name} ...")
    for i, blob in enumerate(container.list_blobs()):
        size = blob.size
        total_count += 1
        total_bytes += size

        # 提取顶层目录
        parts = blob.name.split('/', 1)
        if len(parts) > 1:
            top_dir = parts[0]
        else:
            top_dir = '(root_files)'

        dir_stats[top_dir]['count'] += 1
        dir_stats[top_dir]['bytes'] += size

        if (i + 1) % 500000 == 0:
            elapsed = time.time() - start
            print(f"    已扫描 {i+1:,} 个文件 ({elapsed:.0f}s)")

    elapsed = time.time() - start
    print(f"    完成: {total_count:,} 文件, {total_bytes/1024**3:.1f} GB, 耗时 {elapsed:.0f}s")
    return dir_stats, total_count, total_bytes

# ─── 统计 Azure Files 共享 ───
def inventory_file_share(share_name):
    """返回 {顶层目录: {count, bytes}} 和 总计"""
    share = service.get_share_client(share_name)
    dir_stats = defaultdict(lambda: {'count': 0, 'bytes': 0})
    total_count = 0
    total_bytes = 0
    start = time.time()

    def walk_directory(dir_client, top_dir_name):
        nonlocal total_count, total_bytes
        for item in dir_client.list_directories_and_files():
            if item['is_directory']:
                sub_dir = dir_client.get_subdirectory_client(item['name'])
                walk_directory(sub_dir, top_dir_name)
            else:
                size = item.get('size', 0)
                total_count += 1
                total_bytes += size
                dir_stats[top_dir_name]['count'] += 1
                dir_stats[top_dir_name]['bytes'] += size

                if total_count % 500000 == 0:
                    elapsed = time.time() - start
                    print(f"    已扫描 {total_count:,} 个文件 ({elapsed:.0f}s)")

    print(f"  正在枚举 {share_name} ...")
    root_dir = share.get_directory_client("")
    for item in root_dir.list_directories_and_files():
        if item['is_directory']:
            sub_dir = root_dir.get_subdirectory_client(item['name'])
            walk_directory(sub_dir, item['name'])
        else:
            size = item.get('size', 0)
            total_count += 1
            total_bytes += size
            dir_stats['(root_files)']['count'] += 1
            dir_stats['(root_files)']['bytes'] += size

    elapsed = time.time() - start
    print(f"    完成: {total_count:,} 文件, {total_bytes/1024**3:.1f} GB, 耗时 {elapsed:.0f}s")
    return dir_stats, total_count, total_bytes

# ─── 生成分批计划 ───
def generate_batch_plan(containers_stats):
    """根据文件数和数据量自动分批，任一超限即按顶层目录拆分"""
    batches = []
    batch_id = 1

    for container_name, (dir_stats, total_count, total_bytes) in containers_stats.items():
        if total_count <= MAX_FILES and total_bytes <= MAX_BYTES:
            # 文件数和数据量都在限制内，整个容器作为一个 batch
            batches.append({
                'id': f"batch_{batch_id:03d}",
                'container': container_name,
                'path': '',
                'count': total_count,
                'bytes': total_bytes,
            })
            batch_id += 1
        else:
            # 文件数或数据量超限，按顶层目录拆分
            for dir_name, stats in sorted(dir_stats.items()):
                batches.append({
                    'id': f"batch_{batch_id:03d}",
                    'container': container_name,
                    'path': dir_name,
                    'count': stats['count'],
                    'bytes': stats['bytes'],
                })
                batch_id += 1

    return batches

# ─── 费用估算（Azure China 跨区域迁移）───
def estimate_cost(total_count, total_bytes):
    """
    估算 CE1/CN1 → CN3 跨区域迁移费用（人民币）
    价格来源: https://www.azure.cn/pricing/details/bandwidth/
              https://www.azure.cn/pricing/details/storage/blobs/
    注意: 价格可能变动，以 Azure China 官网为准
    """
    total_gb = total_bytes / (1024 ** 3)

    # ─── Azure China 定价（人民币）───
    # 跨区域数据传输
    BANDWIDTH_PER_GB = 0.67             # ¥/GB（跨区域出站）

    # Blob 操作费（Standard LRS Hot）
    WRITE_PER_10K = 0.36                # ¥/万次（PUT/COPY 写入操作）
    READ_PER_10K = 0.036                # ¥/万次（GET/LIST 读取操作）
    LIST_PER_10K = 0.36                 # ¥/万次（List 操作）

    # ─── 计算 ───
    # 1. 跨区域数据传输费
    bandwidth_cost = total_gb * BANDWIDTH_PER_GB

    # 2. 源端读取操作费（每个文件 1 次 GET）
    read_ops_cost = (total_count / 10000) * READ_PER_10K

    # 3. 目标端写入操作费（每个文件 1 次 PUT）
    write_ops_cost = (total_count / 10000) * WRITE_PER_10K

    # 4. 源端 List 操作费（每 5000 个文件约 1 次 List 请求）
    list_ops = total_count / 5000
    list_ops_cost = (list_ops / 10000) * LIST_PER_10K

    # 5. 增量同步（预估 3 次，每次传输 DELTA_SYNC_RATIO 比例的数据）
    delta_total_ratio = 3 * DELTA_SYNC_RATIO  # 3 次 × 每次比例
    delta_bandwidth = total_gb * delta_total_ratio * BANDWIDTH_PER_GB
    delta_ops = (total_count * delta_total_ratio / 10000) * (READ_PER_10K + WRITE_PER_10K)

    total_cost = bandwidth_cost + read_ops_cost + write_ops_cost + list_ops_cost + delta_bandwidth + delta_ops

    return {
        'bandwidth': bandwidth_cost,
        'read_ops': read_ops_cost,
        'write_ops': write_ops_cost,
        'list_ops': list_ops_cost,
        'delta_bandwidth': delta_bandwidth,
        'delta_ops': delta_ops,
        'delta_ratio': DELTA_SYNC_RATIO,
        'total': total_cost,
        'total_gb': total_gb,
        'total_count': total_count,
    }

# ─── 主流程 ───
def main():
    print("═══════════════════════════════════════════════════════")
    print("  源端数据盘点")
    print(f"  SA: {SRC_ACCOUNT}  类型: {STORAGE_TYPE}")
    print(f"  容器: {CONTAINER_NAME if CONTAINER_NAME else '(全部)'}")
    print(f"  分批上限: {MAX_FILES:,} 文件 或 {MAX_BYTES/1024**4:.0f} TB/batch")
    print("═══════════════════════════════════════════════════════")
    print("")

    containers = get_containers()
    print(f"发现 {len(containers)} 个{'容器' if STORAGE_TYPE == 'blob' else '共享'}: {', '.join(containers)}")
    print("")

    containers_stats = {}
    grand_total_count = 0
    grand_total_bytes = 0

    for c in containers:
        if STORAGE_TYPE == 'blob':
            dir_stats, count, size = inventory_blob_container(c)
        else:
            dir_stats, count, size = inventory_file_share(c)
        containers_stats[c] = (dir_stats, count, size)
        grand_total_count += count
        grand_total_bytes += size
        print("")

    # 生成分批计划
    batches = generate_batch_plan(containers_stats)

    # 写入 batch_plan.txt
    plan_path = os.path.join(script_dir, 'batch_plan.txt')
    with open(plan_path, 'w') as f:
        f.write(f"# 迁移批次计划 — 自动生成于 {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}\n")
        f.write(f"# 源端: {SRC_ACCOUNT}  类型: {STORAGE_TYPE}\n")
        f.write(f"# 总计: {grand_total_count:,} 文件, {grand_total_bytes/1024**3:.1f} GB, {len(batches)} 个批次\n")
        f.write(f"# 格式: batch_id | 容器名 | 路径 | 文件数 | 大小(GB)\n")
        f.write("#\n")
        for b in batches:
            f.write(f"{b['id']} | {b['container']} | {b['path']} | {b['count']} | {b['bytes']/1024**3:.1f}\n")

    # 输出汇总
    print("═══════════════════════════════════════════════════════")
    print("  盘点汇总")
    print("═══════════════════════════════════════════════════════")
    print(f"  {'容器/共享':<20} {'文件数':>14} {'大小':>10}")
    print(f"  {'-'*46}")
    for c, (_, count, size) in containers_stats.items():
        print(f"  {c:<20} {count:>14,} {size/1024**3:>8.1f} GB")
    print(f"  {'-'*46}")
    print(f"  {'合计':<20} {grand_total_count:>14,} {grand_total_bytes/1024**3:>8.1f} GB")
    print("")
    print(f"  分批计划: {len(batches)} 个批次 → batch_plan.txt")
    print("")

    # 输出 batch 列表
    print(f"  {'批次':<12} {'容器':<20} {'路径':<16} {'文件数':>12} {'大小':>8}")
    print(f"  {'-'*70}")
    for b in batches:
        path_display = b['path'] if b['path'] else '(全部)'
        print(f"  {b['id']:<12} {b['container']:<20} {path_display:<16} {b['count']:>12,} {b['bytes']/1024**3:>6.1f} GB")
    print("")
    print("═══════════════════════════════════════════════════════")
    print("  下一步: bash 3-migrate.sh")
    print("═══════════════════════════════════════════════════════")

    # ─── 费用估算 ───
    cost = estimate_cost(grand_total_count, grand_total_bytes)
    print("")
    print("═══════════════════════════════════════════════════════")
    print("  迁移费用估算（Azure China 跨区域 CE1/CN1 → CN3）")
    print("═══════════════════════════════════════════════════════")
    print(f"  数据量: {cost['total_gb']:.1f} GB / {cost['total_count']:,} 文件")
    print(f"")
    print(f"  {'费用项':<24} {'金额':>10}")
    print(f"  {'-'*36}")
    print(f"  {'跨区域数据传输':<24} {'¥{:,.0f}'.format(cost['bandwidth']):>10}")
    print(f"  {'源端读取操作':<24} {'¥{:,.0f}'.format(cost['read_ops']):>10}")
    print(f"  {'目标端写入操作':<24} {'¥{:,.0f}'.format(cost['write_ops']):>10}")
    print(f"  {'源端 List 操作':<24} {'¥{:,.0f}'.format(cost['list_ops']):>10}")
    delta_pct = cost['delta_ratio'] * 100
    print(f"  {f'增量同步（3次×{delta_pct:.0f}%）':<24} {'¥{:,.0f}'.format(cost['delta_bandwidth'] + cost['delta_ops']):>10}")
    print(f"  {'-'*36}")
    print(f"  {'预估总费用':<24} {'¥{:,.0f}'.format(cost['total']):>10}")
    print(f"")
    print(f"  [WARN]  以上为估算值，实际费用以 Azure 账单为准")
    print(f"  [WARN]  价格来源: azure.cn（可能有变动）")
    print(f"  [WARN]  增量比例可在 config.env 的 DELTA_SYNC_RATIO 调整（当前 {delta_pct:.0f}%/次）")
    print(f"  [WARN]  未含 VM 运行费用")
    print("═══════════════════════════════════════════════════════")

if __name__ == "__main__":
    main()
