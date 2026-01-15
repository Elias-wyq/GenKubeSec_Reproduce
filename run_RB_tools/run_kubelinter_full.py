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
OUTPUT_FILE = "/home/wyq/GenKubeSec_Reproduce/kcfs_results/RB_tool_results/kubelinter_full_results.jsonl"
# KubeLinter 二进制文件的路径 (请确保在包含 .gobin 的目录下运行此脚本)
KUBELINTER_BIN = "/home/wyq/kube-linter/.gobin/kube-linter" 
# 并行进程数
MAX_WORKERS = 16

def scan_content_with_kubelinter(args):
    """
    工作进程函数：接收文件内容，创建临时文件，运行 KubeLinter，提取关键数据
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
            
            # 构造命令: 添加 --format json
            cmd = f"{KUBELINTER_BIN} lint {tmp_file.name} --format json"
            
            # 运行 KubeLinter
            # check=False: 即使发现错误也不报错退出
            result = subprocess.run(
                cmd, 
                shell=True, 
                capture_output=True, 
                text=True, 
                check=False
            )
            
            # KubeLinter 的结果在 stdout 中
            if result.stdout:
                try:
                    data = json.loads(result.stdout)
                    
                    # 提取 Reports 列表
                    # KubeLinter 如果没有发现问题，Reports 可能是 None 或空列表
                    reports = data.get("Reports", [])
                    if reports is None:
                        reports = []
                    
                    # 只有当确实有 Reports 时我们才提取，节省存储空间
                    if len(reports) > 0:
                        extracted_errors = []
                        for report in reports:
                            # 提取关键信息用于数据集构建 (复用你原来的逻辑)
                            extracted_errors.append({
                                "check_id": report.get("Check"),           # 如: latest-tag
                                "remediation": report.get("Remediation"),  # 如: Use a container image...
                                "object_kind": report.get("Object", {}).get("Kind"),
                                "object_name": report.get("Object", {}).get("Name"),
                                "message": report.get("Diagnostic", {}).get("Message")
                            })
                        
                        extracted_data = {
                            "filename": filename,
                            "scan_tool": "kubelinter",
                            "error_count": len(extracted_errors),
                            "errors": extracted_errors
                        }
                    
                except json.JSONDecodeError:
                    # KubeLinter 有时会输出非 JSON 的日志信息，忽略即可
                    pass

    except Exception as e:
        return {"error": str(e), "filename": filename}

    return extracted_data

def main():
    print(f"1. 正在设置缓存路径: {os.environ['HF_DATASETS_CACHE']}")
    
    # 检查 KubeLinter 二进制文件是否存在
    if not os.path.exists(KUBELINTER_BIN) and not KUBELINTER_BIN.startswith("kube-linter"):
        # 如果是相对路径且找不到，给个警告（除非它是全局命令）
        print(f"警告: 在当前路径下未找到 {KUBELINTER_BIN}，请确认路径是否正确。")

    print(f"2. 正在加载数据集: {DATASET_NAME} ...")
    
    try:
        # split="train" 加载所有数据
        # streaming=False 利用缓存
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
        # 构造伪文件名: repo_owner_repo_name_filepath.yaml
        pseudo_filename = f"file_{i}.yaml"
        
        tasks.append((i, content, pseudo_filename))

    # 确保输出目录存在
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    # 并行执行
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f_out:
        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # 提交任务
            futures = [executor.submit(scan_content_with_kubelinter, task) for task in tasks]
            
            # 使用 tqdm 显示进度条
            for future in tqdm(as_completed(futures), total=total_files, desc="Scanning with KubeLinter"):
                result = future.result()
                
                # 如果有有效结果 (非 None 且包含 errors)
                if result and "errors" in result:
                    f_out.write(json.dumps(result, ensure_ascii=False) + "\n")
                    f_out.flush()

    print(f"\n扫描完成！结果已保存至: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()