import json
import pandas as pd
import os
from collections import defaultdict

# --- 1. 配置路径 ---
MAPPING_FILE = "policies_with_remediation.csv"
INPUT_FILES = {
    "checkov": "/home/wyq/GenKubeSec_Reproduce/kcfs_results/RB_tool_results/checkov_full_results.jsonl",
    "kubelinter": "/home/wyq/GenKubeSec_Reproduce/kcfs_results/RB_tool_results/kubelinter_full_results.jsonl",
    "terrascan": "/home/wyq/GenKubeSec_Reproduce/kcfs_results/RB_tool_results/terrascan_full_results.jsonl"
}
OUTPUT_FILE = "/home/wyq/kcfs_results/final_labels.jsonl"

def normalize_text(text):
    """文本标准化：去除前后空格"""
    if not isinstance(text, str):
        return ""
    return text.strip()

def load_mapping(filepath):
    """加载 CSV 映射表"""
    if not os.path.exists(filepath):
        print(f"❌ 错误: 找不到 CSV 文件 {filepath}")
        return None, None, None

    df = pd.read_csv(filepath, dtype=str).fillna("")
    
    ckv_map = {}
    ter_map = {}
    kbl_map_remediation = {}

    for _, row in df.iterrows():
        umi_id = row['ID']
        
        # Checkov 匹配字典
        c_policy = normalize_text(row.get('Checkov_Policy', ''))
        if c_policy: ckv_map[c_policy] = umi_id

        # Terrascan 匹配字典
        t_policy = normalize_text(row.get('Terrascan_Policy', ''))
        if t_policy: ter_map[t_policy] = umi_id

        # KubeLinter (Remediation) 匹配字典
        rem = normalize_text(row.get('Remediation', ''))
        if rem: kbl_map_remediation[rem] = umi_id
    
    print(f"✅ 映射表加载完成。Checkov: {len(ckv_map)}, Terrascan: {len(ter_map)}, KubeLinter: {len(kbl_map_remediation)}")
    return ckv_map, ter_map, kbl_map_remediation

def process_file(filepath, tool_name, mapping, global_data):
    """
    读取单个文件，解析并更新到全局字典 global_data 中
    """
    if not os.path.exists(filepath):
        print(f"⚠️ 跳过: 文件不存在 {filepath}")
        return

    print(f"📖 正在读取 {tool_name} 结果...")
    count = 0
    matched_count = 0
    
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line: continue
            
            try:
                entry = json.loads(line)
                filename = entry.get('filename')
                if not filename: continue
                
                count += 1
                
                # 1. 收集 Resource Kind 候选 (用于后续补全 Unknown)
                # 只要该文件在任意工具中识别出了有效的 Kind，就存下来
                for err in entry.get('errors', []):
                    k = err.get('kind', 'Unknown')
                    if k and k != "Unknown":
                        global_data[filename]["kinds"].append(k)
                
                # 2. 匹配错误规则并记录 ID
                for err in entry.get('errors', []):
                    matched_id = None
                    
                    if tool_name == "checkov":
                        # Checkov: 用 check_name 匹配
                        key = normalize_text(err.get('check_name', ''))
                        if key in mapping: matched_id = mapping[key]
                            
                    elif tool_name == "terrascan":
                        # Terrascan: 用 description 匹配
                        key = normalize_text(err.get('description', ''))
                        if key in mapping: matched_id = mapping[key]
                            
                    elif tool_name == "kubelinter":
                        # KubeLinter: 用 remediation 匹配
                        key = normalize_text(err.get('remediation', ''))
                        if key in mapping: matched_id = mapping[key]
                    
                    if matched_id:
                        global_data[filename]["umi_ids"].add(matched_id)
                        matched_count += 1
                        
            except json.JSONDecodeError:
                pass
    
    print(f"   └─ 已处理 {count} 个文件记录，成功匹配 {matched_count} 个错误项。")

def main():
    # 1. 加载 CSV 映射
    ckv_map, ter_map, kbl_map_rem = load_mapping(MAPPING_FILE)
    if not ckv_map: return

    # 2. 初始化全局数据容器
    # 结构: { "file_1.yaml": { "kinds": ["Service", ...], "umi_ids": set("1", "52") } }
    # 使用 defaultdict 自动处理新文件
    global_data = defaultdict(lambda: {"kinds": [], "umi_ids": set()})

    print("🚀 开始加载数据到内存 (字典模式)...")

    # 3. 依次处理三个文件 (顺序不重要，因为是按 filename 聚合)
    process_file(INPUT_FILES["checkov"], "checkov", ckv_map, global_data)
    process_file(INPUT_FILES["terrascan"], "terrascan", ter_map, global_data)
    process_file(INPUT_FILES["kubelinter"], "kubelinter", kbl_map_rem, global_data)

    print(f"💾 内存加载完毕，共涉及 {len(global_data)} 个唯一文件。正在写入结果...")

    # 4. 生成最终结果
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f_out:
        for filename, data in global_data.items():
            
            # 确定最佳 Kind (投票机制)
            best_kind = "Unknown"
            if data["kinds"]:
                # 简单取第一个非 Unknown 的，或者你可以写更复杂的统计逻辑
                # 因为通常一个文件的 Kind 是唯一的
                best_kind = data["kinds"][0]
            
            # 如果没有匹配到任何 UMI ID，则跳过 (或者视情况保留空列表)
            if not data["umi_ids"]:
                continue

            # 生成标签: Kind+ID
            final_labels = [f"{best_kind}+{uid}" for uid in data["umi_ids"]]
            
            record = {
                "filename": filename,
                "misconfig_labels": sorted(list(set(final_labels))),
                "error_count": len(final_labels)
            }
            f_out.write(json.dumps(record, ensure_ascii=False) + "\n")

    print(f"🎉 全部完成！结果已保存至: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()