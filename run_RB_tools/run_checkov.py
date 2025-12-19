import os
import glob
import subprocess
import json

# --- 配置部分 ---
INPUT_DIR = "/home/wyq/GenKubeSec_Reproduce/raw_100_yaml_files"       # 你的 YAML 文件存放目录
OUTPUT_FILE = "/home/wyq/kcfs_results/checkov_100_results.jsonl" # 结果保存文件
SCAN_LIMIT = 100                      # 只扫描前 10 个

def scan_file_with_checkov(filepath):
    """
    对单个文件运行 Checkov 并返回解析后的结果
    """
    # 构造命令
    # --file: 指定文件
    # --output json: 输出 JSON 格式，方便代码解析
    # --quiet: 不输出 Checkov 的 Logo 和进度条
    # --framework kubernetes: 只启用 K8s 扫描，加快速度
    cmd = f"checkov --file {filepath} --output json --quiet --framework kubernetes"
    
    try:
        # 运行命令
        # check=False 是因为 Checkov 发现错误时会返回非 0 状态码，我们不希望脚本因此中断
        result = subprocess.run(
            cmd, 
            shell=True, 
            capture_output=True, 
            text=True, 
            check=False 
        )
        
        # Checkov 的 JSON 输出通常在 stdout 中
        if result.stdout:
            try:
                data = json.loads(result.stdout)
                
                # Checkov 对单个文件的输出是一个字典（如果扫描文件夹则是列表）
                # 但有时如果出错可能返回空，需要防御性编程
                if isinstance(data, dict):
                    return data
                elif isinstance(data, list) and len(data) > 0:
                    return data[0]
            except json.JSONDecodeError:
                print(f"解析 JSON 失败: {filepath}")
                return None
    except Exception as e:
        print(f"运行出错 {filepath}: {str(e)}")
        return None
    
    return None

def main():
    # 1. 获取前 10 个文件
    yaml_files = sorted(glob.glob(os.path.join(INPUT_DIR, "*.yaml")))
    target_files = yaml_files[:SCAN_LIMIT]
    
    print(f"找到 {len(yaml_files)} 个文件，准备扫描前 {len(target_files)} 个...")
    
    # 2. 准备写入文件
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f_out:
        for idx, filepath in enumerate(target_files):
            filename = os.path.basename(filepath)
            print(f"[{idx+1}/{len(target_files)}] 正在扫描: {filename}")
            
            raw_result = scan_file_with_checkov(filepath)
            
            if raw_result and "results" in raw_result:
                failed_checks = raw_result["results"].get("failed_checks", [])
                
                # 提取我们关心的精简信息，用于数据集标签
                extracted_errors = []
                for check in failed_checks:
                    extracted_errors.append({
                        "check_id": check.get("check_id"),     # 例如 CKV_K8S_20
                        "check_name": check.get("check_name"), # 例如 Containers should not run with allowPrivilegeEscalation
                        "file_line_range": check.get("file_line_range"), # 例如 [20, 44]
                        # "guideline": check.get("guideline")    # 官方修复文档链接
                    })
                
                # 构造最终的一条训练数据记录
                record = {
                    "filename": filename,
                    # 为了训练 LLM，你需要同时保存原始 YAML 内容和错误标签
                    # 这里我们暂时不读取 content，如果你需要可以加上
                    "scan_tool": "checkov",
                    "error_count": len(extracted_errors),
                    "errors": extracted_errors
                }
                
                # 写入一行 JSON
                f_out.write(json.dumps(record, ensure_ascii=False) + "\n")
            else:
                print(f"  警告: {filename} 没有返回有效的 JSON 结果")

    print(f"\n扫描完成！结果已保存至 {OUTPUT_FILE}")

if __name__ == "__main__":
    main()