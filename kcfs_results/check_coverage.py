import json
import pandas as pd
from collections import Counter
from datasets import load_from_disk

# --- 配置 ---
DATASET_PATH = "/home/wyq/GenKubeSec_Reproduce/kcfs_results/genkubesec_dataset"
UMI_CSV_PATH = "/home/wyq/GenKubeSec_Reproduce/unify_error_umi/policies_with_remediation.csv"

def main():
    # 1. 加载所有 UMI ID
    print("加载 UMI 规则库...")
    df = pd.read_csv(UMI_CSV_PATH, dtype=str).fillna("")
    
    all_ids = set(df['ID'].unique())
    
    # 修改点：更健壮的描述提取逻辑
    id_to_desc = {}
    for _, row in df.iterrows():
        # 依次尝试获取描述，使用 strip() 去除首尾空格
        desc = (
            str(row.get('Checkov_Policy', '')).strip() or 
            str(row.get('Kube_Linter_Policy', '')).strip() or 
            str(row.get('Terrascan_Policy', '')).strip() or 
            str(row.get('Remediation', '')).strip()
        )
        id_to_desc[row['ID']] = desc
    
    print(f"UMI 总规则数: {len(all_ids)}")

    # 2. 统计训练集里的标签
    print("扫描训练集标签分布...")
    dataset = load_from_disk(DATASET_PATH)
    train_data = dataset['train']
    
    present_ids = set()
    label_counts = Counter()

    # 遍历训练集 (这里只看 ID，忽略 ResourceKind)
    # 标签格式: "Deployment+52, Service+10"
    for item in train_data:
        labels_str = item['target']
        if not labels_str: continue
        
        for label in labels_str.split(','):
            parts = label.strip().split('+')
            if len(parts) == 2:
                uid = parts[1]
                present_ids.add(uid)
                label_counts[uid] += 1
                
    # 3. 计算缺失
    missing_ids = all_ids - present_ids
    print("\n" + "="*40)
    print(f"📊 覆盖率报告")
    print("="*40)
    print(f"✅ 已覆盖 ID 数: {len(present_ids)}")
    print(f"❌ 未覆盖 ID 数: {len(missing_ids)}")
    print(f"⚠️ 样本极少 (<10) 的 ID 数: {sum(1 for c in label_counts.values() if c < 10)}")
    print("-" * 40)
    
    # 4. 保存缺失列表，供下一步使用
    missing_report = []
    for uid in missing_ids:
        missing_report.append({
            "id": uid,
            "description": id_to_desc.get(uid, "Unknown")
        })
        
    with open("missing_ids.json", "w") as f:
        json.dump(missing_report, f, indent=2)
        
    print(f"缺失 ID 列表已保存至 missing_ids.json (共 {len(missing_ids)} 条)")
    # 打印前 5 个看看
    print("缺失示例:", json.dumps(missing_report[:5], indent=2))

if __name__ == "__main__":
    main()