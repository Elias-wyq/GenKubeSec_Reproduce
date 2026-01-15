import os
import json
import tempfile
import subprocess
from concurrent.futures import ProcessPoolExecutor, as_completed

# --- 关键配置：设置 Hugging Face 缓存路径 ---
# 必须在导入 datasets 之前设置
os.environ["HF_DATASETS_CACHE"] = "/ssd_2t_1/wyq_workspace/hf_cache"
from datasets import load_dataset
from tqdm import tqdm

# --- 配置区域 ---
DATASET_NAME = "substratusai/the-stack-yaml-k8s"
OUTPUT_FILE = "/home/wyq/kcfs_results/terrascan_full_results.jsonl"
# 并行进程数 (建议设置为 CPU 核数 - 2)
MAX_WORKERS = 16 

def scan_content_with_terrascan(args):
    """
    工作进程函数：接收文件内容，创建临时文件，运行 Terrascan，提取关键数据
    args: (index, content, pseudo_filename)
    """
    idx, content, filename = args
    
    extracted_data = None
    
    try:
        # 创建临时文件 (扫描完自动删除)
        # suffix='.yaml' 
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=True) as tmp_file:
            tmp_file.write(content)
            tmp_file.flush() # 确保内容写入磁盘
            
            # 构造命令
            # -i k8s: 指定输入类型为 Kubernetes
            # -f: 指定文件路径
            # -o json: 输出 JSON 格式
            cmd = f"terrascan scan -i k8s -f {tmp_file.name} -o json"
            
            # 运行 Terrascan
            # check=False: Terrascan 发现违规时退出码通常为 3，不应报错中断
            result = subprocess.run(
                cmd, 
                shell=True, 
                capture_output=True, 
                text=True, 
                check=False
            )
            
            # Terrascan 的 JSON 结果在 stdout 中
            if result.stdout:
                try:
                    data = json.loads(result.stdout)
                    
                    # 提取 violations 列表
                    # 结构通常是: {"results": {"violations": [...]}}
                    # 防御性编程：确保 results 存在且 violations 不为 None
                    violations = data.get("results", {}).get("violations", [])
                    if violations is None:
                        violations = []
                    
                    # 只有当确实有 violations 时我们才提取
                    if len(violations) > 0:
                        extracted_errors = []
                        for v in violations:
                            # 提取用于构建 UMI 的关键字段 (复用你原来的逻辑)
                            extracted_errors.append({
                                "rule_id": v.get("rule_id"),       # 如 AC_K8S_0062
                                "description": v.get("description"), 
                                "severity": v.get("severity"),     
                                "category": v.get("category"),     
                                "line": v.get("line")              
                            })
                        
                        extracted_data = {
                            "filename": filename,
                            "scan_tool": "terrascan",
                            "error_count": len(extracted_errors),
                            "errors": extracted_errors
                        }
                    
                except json.JSONDecodeError:
                    # Terrascan 偶尔可能输出非 JSON 的 panic 信息，直接忽略
                    pass

    except Exception as e:
        return {"error": str(e), "filename": filename}

    return extracted_data

def main():
    print(f"1. 正在设置缓存路径: {os.environ['HF_DATASETS_CACHE']}")
    print(f"2. 正在加载数据集: {DATASET_NAME} ...")
    
    try:
        # split="train" 加载所有数据
        # streaming=False 利用缓存，无需重复下载
        ds = load_dataset(DATASET_NAME, split="train", streaming=False)
    except Exception as e:
        print(f"数据集加载失败: {e}")
        return

    total_files = len(ds)
    print(f"   数据集加载成功！共包含 {total_files} 个文件。")
    print(f"3. 准备开始并行扫描 (进程数: {MAX_WORKERS})...")

    # 准备任务列表
    tasks = []
    for i in range(total_files):
        item = ds[i]
        content = item['content']
        
        # === 文件名逻辑修改：只保留 file_{i}.yaml ===
        pseudo_filename = f"file_{i}.yaml"
        
        tasks.append((i, content, pseudo_filename))

    # 确保输出目录存在
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    # 并行执行
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f_out:
        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # 提交任务
            futures = [executor.submit(scan_content_with_terrascan, task) for task in tasks]
            
            # 使用 tqdm 显示进度条
            for future in tqdm(as_completed(futures), total=total_files, desc="Scanning with Terrascan"):
                result = future.result()
                
                # 如果有有效结果 (非 None 且包含 errors)
                if result and "errors" in result:
                    f_out.write(json.dumps(result, ensure_ascii=False) + "\n")
                    f_out.flush()

    print(f"\n扫描完成！结果已保存至: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()