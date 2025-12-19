import json
import pandas as pd
import difflib
import os

# --- 配置 ---
MAPPING_FILE = "policies_with_remediation.csv"
INPUT_FILES = {
    "checkov": "checkov_100_results.jsonl",
    "kubelinter": "kubelinter_100_results.jsonl",
    "terrascan": "terrascan_100_results.jsonl"
}
OUTPUT_FILE = "unified_100_dataset2.jsonl"

def load_mapping(filepath):
    """加载 CSV 并创建查找字典"""
    try:
        df = pd.read_csv(filepath)
        print(f"成功加载映射文件: {filepath}, 共 {len(df)} 条规则")
    except Exception as e:
        print(f"加载映射文件失败: {e}")
        return None, None, None

    # 创建查找表：描述 -> Rule_ID
    # 注意：这里我们去掉字符串前后的空格，确保匹配准确
    ckv_map = {row['Checkov_Policy'].strip(): row['ID'] for _, row in df.iterrows() if pd.notna(row['Checkov_Policy'])}
    ter_map = {row['Terrascan_Policy'].strip(): row['ID'] for _, row in df.iterrows() if pd.notna(row['Terrascan_Policy'])}
    
    # 3. KubeLinter 映射：Remediation -> ID (改为精确匹配)
    # 注意：这里读取的是 CSV 中的 'Remediation' 列
    kbl_map = {
        row['Remediation'].strip(): row['ID'] 
        for _, row in df.iterrows() 
        if pd.notna(row['Remediation'])
    }
    
    return ckv_map, ter_map, kbl_map

# def get_best_match(text, candidates, threshold=0.4):
#     """
#     为 KubeLinter 使用模糊匹配
#     text: 工具输出的错误信息
#     candidates: [(text, ID, source), ...]
#     返回: (rule_id, matched_text, source) 或 (None, None, None)
#     """
#     if not text:
#         return None, None, None
    
#     # 提取所有候选文本
#     texts = [item[0] for item in candidates]
#     matches = difflib.get_close_matches(text, texts, n=1, cutoff=threshold)
#     if matches:
#         best_text = matches[0]
#         for txt, rule_id, source in candidates:
#             if txt == best_text:
#                 return rule_id, best_text, source
#     return None, None, None

def main():
    ckv_map, ter_map, kbl_map = load_mapping(MAPPING_FILE)
    if not ckv_map: return

    # 用于存储聚合结果: {filename: {umi_ids}}
    aggregated_data = {}
    
    # 统计匹配率
    stats = {"total_findings": 0, "mapped_findings": 0}

    # --- 1. 处理 Checkov ---
    if os.path.exists(INPUT_FILES["checkov"]):
        print(f"正在处理 {INPUT_FILES['checkov']} ...")
        with open(INPUT_FILES["checkov"], 'r', encoding='utf-8') as f:
            for line in f:
                entry = json.loads(line)
                fname = entry['filename']
                if fname not in aggregated_data: aggregated_data[fname] = set()
                
                for err in entry.get('errors', []):
                    stats["total_findings"] += 1
                    # Checkov 使用 'check_name' 进行匹配
                    check_name = err.get('check_name', '').strip()
                    if check_name in ckv_map:
                        aggregated_data[fname].add(f"{ckv_map[check_name]}")
                        stats["mapped_findings"] += 1
    
    # --- 2. 处理 Terrascan ---
    if os.path.exists(INPUT_FILES["terrascan"]):
        print(f"正在处理 {INPUT_FILES['terrascan']} ...")
        with open(INPUT_FILES["terrascan"], 'r', encoding='utf-8') as f:
            for line in f:
                entry = json.loads(line)
                fname = entry['filename']
                if fname not in aggregated_data: aggregated_data[fname] = set()

                for err in entry.get('errors', []):
                    stats["total_findings"] += 1
                    # Terrascan 使用 'description' 进行匹配
                    desc = err.get('description', '').strip()
                    if desc in ter_map:
                        aggregated_data[fname].add(f"{ter_map[desc]}")
                        stats["mapped_findings"] += 1

    # --- 3. 处理 KubeLinter (使用 Remediation 精确匹配) ---
    if os.path.exists(INPUT_FILES["kubelinter"]):
        print(f"正在处理 {INPUT_FILES['kubelinter']} ...")
        with open(INPUT_FILES["kubelinter"], 'r', encoding='utf-8') as f:
            for line in f:
                entry = json.loads(line)
                fname = entry['filename']
                if fname not in aggregated_data: aggregated_data[fname] = set()

                for err in entry.get('errors', []):
                    stats["total_findings"] += 1
                    
                    # 获取工具输出中的 remediation 字段
                    remediation_text = err.get('remediation', '').strip()
                    
                    # 直接在字典中查找 (Exact Match)
                    if remediation_text in kbl_map:
                        aggregated_data[fname].add(f"{kbl_map[remediation_text]}")
                        stats["mapped_findings"] += 1

    # --- 4. 输出结果 ---
    print(f"正在写入结果到 {OUTPUT_FILE} ...")
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        for fname, umi_ids in aggregated_data.items():
            record = {
                "filename": fname,
                "umi_errors": sorted(list(umi_ids)) # 转回列表并排序
            }
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    print("-" * 30)
    print(f"处理完成！")
    print(f"总发现错误数: {stats['total_findings']}")
    print(f"成功映射 UMI 数: {stats['mapped_findings']}")
    if stats['total_findings'] > 0:
        print(f"映射覆盖率: {stats['mapped_findings']/stats['total_findings']:.2%}")
    print(f"结果已保存至 {OUTPUT_FILE}")

if __name__ == "__main__":
    main()