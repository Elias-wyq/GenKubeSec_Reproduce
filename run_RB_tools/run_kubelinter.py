import os
import glob
import subprocess
import json

# --- 配置区域 ---
# 你的 YAML 文件目录（使用绝对路径最稳妥）
INPUT_DIR = "/home/wyq/GenKubeSec_Reproduce/raw_100_yaml_files" 
# 结果保存文件
OUTPUT_FILE = "/home/wyq/kcfs_results/kubelinter_100_results.jsonl"
# KubeLinter 二进制文件的路径
# 根据你的提示，你是在 ~/kube-linter 目录下运行的，所以路径如下：
KUBELINTER_BIN = "./.gobin/kube-linter" 
# 扫描数量限制
SCAN_LIMIT = 100

def scan_file_with_kubelinter(filepath):
    """
    运行 KubeLinter 并提取结构化数据
    """
    # 构造命令: 添加 --format json
    cmd = f"{KUBELINTER_BIN} lint {filepath} --format json"
    
    try:
        # capture_output=True: 捕获输出
        # check=False: 即使 KubeLinter 发现错误（返回非0状态码）也不报错退出
        result = subprocess.run(
            cmd, 
            shell=True, 
            capture_output=True, 
            text=True, 
            check=False
        )
        
        # KubeLinter 的 JSON 结果在 stdout 中
        if result.stdout:
            try:
                data = json.loads(result.stdout)
                
                # 提取 Reports 列表
                # 如果没有错误，Reports 可能是 None 或空列表
                reports = data.get("Reports", [])
                if reports is None: 
                    reports = []
                    
                extracted_errors = []
                for report in reports:
                    # 提取关键信息用于数据集
                    extracted_errors.append({
                        "check_id": report.get("Check"),           # 例如: latest-tag
                        "remediation": report.get("Remediation"),  # 例如: Use a container image...
                        "object_kind": report.get("Object", {}).get("Kind"),
                        "object_name": report.get("Object", {}).get("Name"),
                        "message": report.get("Diagnostic", {}).get("Message")
                    })
                
                return extracted_errors
                
            except json.JSONDecodeError:
                print(f"  [Error] JSON 解析失败: {filepath}")
                # 打印出原始内容方便调试
                # print(result.stdout[:100]) 
                return None
    except Exception as e:
        print(f"  [Error] 执行出错 {filepath}: {str(e)}")
        return None
        
    return []

def main():
    # 1. 获取前 10 个文件
    yaml_files = sorted(glob.glob(os.path.join(INPUT_DIR, "*.yaml")))
    if not yaml_files:
        print(f"错误: 在 {INPUT_DIR} 没有找到 .yaml 文件")
        return

    target_files = yaml_files[:SCAN_LIMIT]
    print(f"找到 {len(yaml_files)} 个文件，准备扫描前 {len(target_files)} 个...")
    
    # 2. 扫描并写入结果
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f_out:
        for idx, filepath in enumerate(target_files):
            filename = os.path.basename(filepath)
            print(f"[{idx+1}/{len(target_files)}] 正在扫描: {filename}")
            
            errors = scan_file_with_kubelinter(filepath)
            
            if errors is not None:
                # 构造训练数据格式
                record = {
                    "filename": filename,
                    "scan_tool": "kubelinter",
                    "error_count": len(errors),
                    "errors": errors
                }
                # 写入 JSONL
                f_out.write(json.dumps(record, ensure_ascii=False) + "\n")
            else:
                print(f"  跳过: {filename} (处理失败)")

    print(f"\n扫描完成！结果已保存至: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()