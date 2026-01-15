import os
import json
import tempfile
import subprocess
from concurrent.futures import ProcessPoolExecutor, as_completed
# 必须在导入 datasets 之前设置缓存路径
os.environ["HF_DATASETS_CACHE"] = "/ssd_2t_1/wyq_workspace/hf_cache"
from datasets import load_dataset
from tqdm import tqdm

# --- 配置区域 ---
DATASET_NAME = "substratusai/the-stack-yaml-k8s"
OUTPUT_FILE = "/home/wyq/kcfs_results/checkov_full_results.jsonl"
# 并行进程数：建议设置为 CPU 核心数 - 2，防止卡死机器
MAX_WORKERS = 16 

def scan_content_with_checkov(args):
    """
    工作进程函数：接收文件内容，创建临时文件，运行 Checkov，提取关键数据
    args: (index, content, pseudo_filename)
    """
    idx, content, filename = args
    
    # 构造 Checkov 命令
    # --quiet: 减少无关输出
    # --framework kubernetes: 加速扫描
    # --check: 如果你有特定的规则列表(UMI)，可以在这里加 --check CKV_K8S_1,CKV_K8S_2... 进一步加速
    base_cmd = "checkov --output json --quiet --framework kubernetes --file"
    
    extracted_data = None
    
    try:
        # 创建临时文件 (扫描完自动删除)
        # suffix='.yaml' 非常重要，Checkov 依赖后缀识别文件类型
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=True) as tmp_file:
            tmp_file.write(content)
            tmp_file.flush() # 确保内容写入磁盘
            
            cmd = f"{base_cmd} {tmp_file.name}"
            
            # 运行 Checkov
            # check=False: Checkov 发现漏洞会返回非0状态码，不应抛出异常
            result = subprocess.run(
                cmd, 
                shell=True, 
                capture_output=True, 
                text=True, 
                check=False
            )
            
            # 解析 JSON 输出
            if result.stdout:
                try:
                    data = json.loads(result.stdout)
                    
                    # Checkov 可能返回字典(单文件)或列表(多文件/目录)
                    result_dict = None
                    if isinstance(data, dict):
                        result_dict = data
                    elif isinstance(data, list) and len(data) > 0:
                        result_dict = data[0]
                    
                    # 提取关键字段 (复用你原来的逻辑)
                    if result_dict and "results" in result_dict:
                        failed_checks = result_dict["results"].get("failed_checks", [])
                        
                        errors = []
                        for check in failed_checks:
                            errors.append({
                                "check_id": check.get("check_id"),     # 如 CKV_K8S_20
                                "check_name": check.get("check_name"), # 如 Containers should not run...
                                "file_line_range": check.get("file_line_range"), # 如 [20, 44]
                                # "guideline": check.get("guideline")  # 可选：修复指南
                            })
                        
                        # 只有当发现错误时才返回数据，或者如果你需要统计“无错误文件”，也可以返回空列表
                        extracted_data = {
                            "filename": filename,
                            "scan_tool": "checkov",
                            "error_count": len(errors),
                            "errors": errors
                        }
                        
                except json.JSONDecodeError:
                    # JSON 解析失败通常意味着 Checkov 崩溃或输出了非 JSON 文本
                    pass

    except Exception as e:
        # 捕捉如 IO 错误等异常
        return {"error": str(e), "filename": filename}

    return extracted_data

def main():
    print(f"1. 正在设置缓存路径: {os.environ['HF_DATASETS_CACHE']}")
    print(f"2. 正在加载数据集: {DATASET_NAME} (首次运行需要下载，请耐心等待)...")
    
    # split="train" 加载所有数据
    # streaming=False 会将数据下载并解压到上面的 cache 目录，方便多次读取
    try:
        ds = load_dataset(DATASET_NAME, split="train", streaming=False)
    except Exception as e:
        print(f"数据集加载失败: {e}")
        return

    total_files = len(ds)
    print(f"   数据集加载成功！共包含 {total_files} 个文件。")
    print(f"3. 准备开始并行扫描 (进程数: {MAX_WORKERS})...")

    # 准备任务列表
    # 我们只提取需要的字段传给子进程，减少内存开销
    tasks = []
    for i in range(total_files):
        item = ds[i]
        content = item['content']
        # 构造一个伪文件名，结合仓库名和路径，方便后续追踪
        # 格式: repo_owner_repo_name_filepath.yaml
        repo = item.get('repository_name', 'unknown_repo').replace('/', '_')
        path = item.get('path', f'file_{i}.yaml').replace('/', '_')
        pseudo_filename = f"{repo}_{path}"
        
        tasks.append((i, content, pseudo_filename))

    # 确保输出目录存在
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    # 并行执行
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f_out:
        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # 提交任务
            futures = [executor.submit(scan_content_with_checkov, task) for task in tasks]
            
            # 使用 tqdm 显示进度条
            for future in tqdm(as_completed(futures), total=total_files, desc="Scanning with Checkov"):
                result = future.result()
                
                # 如果有有效结果（result 非 None 且不是报错信息）
                if result and "errors" in result:
                    # 写入 JSONL
                    f_out.write(json.dumps(result, ensure_ascii=False) + "\n")
                    # 立即刷新缓冲区，防止程序中断丢失数据
                    f_out.flush() 

    print(f"\n扫描完成！结果已保存至: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()