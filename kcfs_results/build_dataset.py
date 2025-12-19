import json
import os
import yaml
import random

# --- 配置 ---
LABEL_FILE = "./RB_tool_results/unified_dataset2.jsonl"
YAML_DIR = "/home/wyq/GenKubeSec_Reproduce/raw_10_yaml_files"
OUTPUT_TRAIN = "train_data.json"
OUTPUT_VAL = "val_data.json"
SPLIT_RATIO = 0.9  # 90% 训练，10% 验证

# 论文中的 Prompt 模板 (参考 System Prompt)
INSTRUCTION_TEMPLATE = (
    "You are a Kubernetes security expert. Detect misconfigurations in the following Kubernetes manifest. "
    "Return the detected issues as a list of encoded labels in the format 'ResourceName+UMI_ID'."
)

def get_resource_name(content):
    """
    从 YAML 内容中提取 metadata.name，用于构建论文提到的 'Encoded Label'
    例如: pulsar-admin
    """
    try:
        docs = list(yaml.safe_load_all(content))
        # 通常只取第一个文档的名称，或者你需要处理多文档 YAML
        for doc in docs:
            if doc and 'metadata' in doc and 'name' in doc['metadata']:
                return doc['metadata']['name']
    except:
        pass
    return "unknown"

def main():
    dataset = []
    
    print(f"正在读取 {LABEL_FILE}...")
    with open(LABEL_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            entry = json.loads(line)
            filename = entry['filename']
            umi_ids = entry['umi_errors']
            
            yaml_path = os.path.join(YAML_DIR, filename)
            
            # 1. 读取 YAML 内容
            if not os.path.exists(yaml_path):
                print(f"警告: 找不到文件 {yaml_path}，跳过")
                continue
                
            with open(yaml_path, 'r', encoding='utf-8') as yf:
                content = yf.read()
            
            # 2. 构建 Encoded Labels (资源名 + ID)
            # GenKubeSec 论文核心技巧: 将 resource name 和 error ID 绑定
            resource_name = get_resource_name(content)
            
            # 如果没有错误，输出 "No misconfigurations found" 或者空
            if not umi_ids:
                output_str = "safe" # 或者 "No misconfigurations detected"
            else:
                # 格式示例: "pulsar-admin+10, pulsar-admin+11"
                encoded_labels = [f"{resource_name}+{uid}" for uid in umi_ids]
                output_str = ", ".join(encoded_labels)
            
            # 3. 构建微调样本
            sample = {
                "instruction": INSTRUCTION_TEMPLATE,
                "input": content,
                "output": output_str
            }
            dataset.append(sample)

    # 4. 划分数据集 (Train/Val Split)
    random.shuffle(dataset)
    split_idx = int(len(dataset) * SPLIT_RATIO)
    train_set = dataset[:split_idx]
    val_set = dataset[split_idx:]
    
    # 5. 保存
    with open(OUTPUT_TRAIN, 'w', encoding='utf-8') as f:
        json.dump(train_set, f, indent=2, ensure_ascii=False)
        
    with open(OUTPUT_VAL, 'w', encoding='utf-8') as f:
        json.dump(val_set, f, indent=2, ensure_ascii=False)

    print(f"构建完成！")
    print(f"训练集: {len(train_set)} 条 -> {OUTPUT_TRAIN}")
    print(f"验证集: {len(val_set)} 条 -> {OUTPUT_VAL}")
    
    # 打印一个样本看看
    if train_set:
        print("\n--- 样本预览 ---")
        print(json.dumps(train_set[0], indent=2))

if __name__ == "__main__":
    main()