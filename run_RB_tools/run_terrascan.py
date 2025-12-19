import os
import glob
import subprocess
import json

# --- 配置区域 ---
# 你的 YAML 文件目录
INPUT_DIR = "/home/wyq/GenKubeSec_Reproduce/raw_100_yaml_files"
# 结果保存文件
OUTPUT_FILE = "/home/wyq/kcfs_results/terrascan_100_results.jsonl"
# 扫描限制
SCAN_LIMIT = 100

def scan_file_with_terrascan(filepath):
    """
    运行 Terrascan 并提取结构化数据
    -i k8s: 指定输入类型为 Kubernetes
    -f: 指定单个文件
    -o json: 输出 JSON 格式
    """
    cmd = f"terrascan scan -i k8s -f {filepath} -o json"
    
    try:
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
                violations = data.get("results", {}).get("violations", [])
                if violations is None:
                    violations = []
                
                extracted_errors = []
                for v in violations:
                    # 提取用于构建 UMI 的关键字段
                    extracted_errors.append({
                        "rule_id": v.get("rule_id"),       # 关键ID，如 AC_K8S_0062
                        "description": v.get("description"), # 错误描述
                        "severity": v.get("severity"),     # 严重程度
                        "category": v.get("category"),     # 分类 (如 Security)
                        "line": v.get("line")              # 行号
                    })
                
                return extracted_errors

            except json.JSONDecodeError:
                print(f"  [Error] JSON 解析失败: {filepath}")
                return None
    except Exception as e:
        print(f"  [Error] 执行出错 {filepath}: {str(e)}")
        return None
    
    return []

def main():
    # 1. 获取前 10 个文件
    yaml_files = sorted(glob.glob(os.path.join(INPUT_DIR, "*.yaml")))
    target_files = yaml_files[:SCAN_LIMIT]
    
    print(f"找到 {len(yaml_files)} 个文件，准备扫描前 {len(target_files)} 个...")
    
    # 2. 扫描并写入结果
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f_out:
        for idx, filepath in enumerate(target_files):
            filename = os.path.basename(filepath)
            print(f"[{idx+1}/{len(target_files)}] 正在扫描: {filename}")
            
            errors = scan_file_with_terrascan(filepath)
            
            if errors is not None:
                # 构造训练数据格式
                record = {
                    "filename": filename,
                    "scan_tool": "terrascan",
                    "error_count": len(errors),
                    "errors": errors
                }
                f_out.write(json.dumps(record, ensure_ascii=False) + "\n")
            else:
                print(f"  跳过: {filename}")

    print(f"\n扫描完成！结果已保存至: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()