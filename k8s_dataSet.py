import os
from datasets import load_dataset

# 配置
DATASET_NAME = "substratusai/the-stack-yaml-k8s"
OUTPUT_DIR = "./raw_100_yaml_files"  # 文件保存目录
LIMIT = 100  # 下载数量限制

# 1. 创建保存目录
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)
    print(f"已创建目录: {OUTPUT_DIR}")

# 2. 以流式 (Streaming) 方式加载数据集
# streaming=True 意味着不下载几十 GB 的全量数据，而是像读文件流一样一条条读
print(f"开始连接 Hugging Face 数据集: {DATASET_NAME}...")
dataset = load_dataset(DATASET_NAME, split="train", streaming=True)

# 3. 循环保存文件
print(f"正在下载前 {LIMIT} 个文件到本地...")

count = 0
for i, sample in enumerate(dataset):
    if count >= LIMIT:
        break

    try:
        # 获取文件内容
        content = sample.get("content", "")
        
        # 获取唯一标识符用于文件名
        # 该数据集通常包含 'id' 或 'hexsha' 字段，如果都没有就用序号
        file_id = f"file_{count + 1}"
        
        # 构造保存路径
        file_path = os.path.join(OUTPUT_DIR, f"{file_id}.yaml")
        
        # 写入文件
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)
            
        count += 1
        
        # 每10个打印一次进度
        if count % 10 == 0:
            print(f"已保存: {count}/{LIMIT}")
            
    except Exception as e:
        print(f"保存第 {i} 个文件时出错: {e}")

print(f"\n下载完成！共保存 {count} 个文件在 '{OUTPUT_DIR}' 目录下。")
print("你现在可以运行之前的 RB 工具扫描脚本来处理这些文件了。")
