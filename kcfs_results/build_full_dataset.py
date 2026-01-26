import json
import os
import random
# 设置 HF 缓存路径 (保持和你之前的一致)
os.environ["HF_DATASETS_CACHE"] = "/ssd_2t_1/wyq_workspace/hf_cache"
from datasets import load_dataset, Dataset, DatasetDict

# --- 配置路径 ---
# 1. 你的标签文件
LABEL_FILE = "/home/wyq/GenKubeSec_Reproduce/kcfs_results/RB_tool_results/final_labels.jsonl"
# 2. 原始数据集名称
HF_DATASET_NAME = "substratusai/the-stack-yaml-k8s"
# 3. 最终保存的 Hugging Face 格式数据集路径
OUTPUT_DIR = "/home/wyq/GenKubeSec_Reproduce/kcfs_results/genkubesec_dataset"

def load_labels(filepath):
    """加载标签文件，返回字典 {filename: labels_string}"""
    print(f"正在加载标签文件: {filepath} ...")
    label_map = {}
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                data = json.loads(line)
                filename = data['filename']
                labels = data['misconfig_labels']
                
                # 将列表转换为逗号分隔的字符串，作为模型的训练目标
                # 例如: ["Deployment+10", "Deployment+15"] -> "Deployment+10, Deployment+15"
                label_str = ", ".join(labels)
                label_map[filename] = label_str
            except json.JSONDecodeError:
                pass
    print(f"✅ 加载完成，共 {len(label_map)} 个已标注文件。")
    return label_map

def main():
    # 1. 加载标签
    label_map = load_labels(LABEL_FILE)
    
    # 2. 加载原始数据集
    print(f"正在加载原始数据集: {HF_DATASET_NAME} ...")
    raw_ds = load_dataset(HF_DATASET_NAME, split="train", streaming=False)
    
    # 3. 构建训练数据列表
    data_entries = []
    
    print("正在合并 YAML 内容与标签...")
    # 遍历原始数据集，根据 file_{i}.yaml 的规则进行匹配
    for i, item in enumerate(raw_ds):
        pseudo_filename = f"file_{i}.yaml"
        
        # 只有当该文件有对应的错误标签时，才纳入训练集
        # (GenKubeSec 论文主要关注有缺陷的样本进行检测训练，
        #  如果你也想让模型学会识别“无错误”文件，可以保留 label_map 中没有的文件并标记为 "Safe")
        if pseudo_filename in label_map:
            content = item['content']
            target = label_map[pseudo_filename]
            
            # 过滤过长的文件 (CodeT5p 限制 512 token，太长的 YAML 效果不好)
            # 这里简单用字符数粗略过滤，后续 Tokenizer 处理时会截断
            if len(content) > 10000: 
                continue

            data_entries.append({
                "source": content,   # 输入: YAML 内容
                "target": target,    # 输出: 错误标签字符串
                "filename": pseudo_filename
            })
            
        if (i + 1) % 50000 == 0:
            print(f"   已扫描 {i + 1} 个原始文件...")

    print(f"✅ 合并完成。有效训练样本数: {len(data_entries)}")

    # 4. 创建 Hugging Face Dataset 对象
    full_dataset = Dataset.from_list(data_entries)

    # 5. 划分数据集 (80% 训练, 10% 验证, 10% 测试)
    # 首先分出 Train 和 (Test + Validation)
    train_testvalid = full_dataset.train_test_split(test_size=0.2, seed=42)
    # 再将 (Test + Validation) 分为 Test 和 Validation
    test_valid = train_testvalid['test'].train_test_split(test_size=0.5, seed=42)

    # 组合成 DatasetDict
    final_dataset = DatasetDict({
        'train': train_testvalid['train'],
        'validation': test_valid['train'], # 这里名字叫 train 但其实是 split 出来的一半
        'test': test_valid['test']
    })

    print("\n数据集划分详情:")
    print(f"   Train: {len(final_dataset['train'])}")
    print(f"   Validation: {len(final_dataset['validation'])}")
    print(f"   Test: {len(final_dataset['test'])}")

    # 6. 保存到磁盘
    print(f"\n正在保存数据集到 {OUTPUT_DIR} ...")
    final_dataset.save_to_disk(OUTPUT_DIR)
    print("🎉 恭喜！训练数据准备就绪。")

if __name__ == "__main__":
    main()