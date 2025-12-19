import os
import json
import pandas as pd

def parse_terrascan_policies(repo_path):
    """
    遍历本地 Terrascan 仓库目录，提取 k8s 策略的元数据
    """
    # 拼接出 k8s 策略所在的绝对路径
    base_path = os.path.join(repo_path, 'pkg', 'policies', 'opa', 'rego', 'k8s')
    
    policies_list = []

    # 检查路径是否存在
    if not os.path.exists(base_path):
        print(f"错误：路径 {base_path} 不存在，请确认您的 repo_path 设置正确。")
        return pd.DataFrame()

    print(f"正在扫描目录: {base_path} ...")

    # os.walk 遍历目录
    for root, dirs, files in os.walk(base_path):
        for file in files:
            # 我们只关心 .json 结尾的元数据文件
            # 注意：有时目录下可能有测试用的json，通常元数据文件不带 'mock' 或 'test'，
            # 或者我们可以通过读取内容来判断是否包含 'reference_id' 等字段
            if file.endswith('.json'):
                file_path = os.path.join(root, file)
                
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        
                    # 检查是否是策略元数据 (通常包含 category, version 等字段)
                    # Terrascan 的 json 结构通常比较扁平，或者在 properties 里
                    
                    # 提取 UMI 核心字段
                    # 注意：根据实际 JSON 结构，字段名可能需要微调，以下是通用结构
                    if 'reference_id' in data or 'id' in data:
                        policy_item = {
                            # 核心 ID (如 AC_K8S_0001) - 这是您 UMI 的主要映射键
                            "Reference_ID": data.get("id") or data.get("reference_id"),
                            
                            # 策略名称
                            "Name": data.get("name") or data.get("title"),
                            
                            # 严重程度 (Low/Medium/High)
                            "Severity": data.get("severity"),
                            
                            # 类别 (Security, Best Practice 等)
                            "Category": data.get("category"),
                            
                            # 资源类型 (如 Kubernetes Pod, Service)
                            "Resource_Type": data.get("resource_type"),
                            
                            # 详细描述
                            "Description": data.get("description"),
                            
                            # 来源文件路径 (方便后续核对)
                            "File_Path": os.path.relpath(file_path, repo_path)
                        }
                        policies_list.append(policy_item)
                        
                except Exception as e:
                    print(f"解析文件出错 {file_path}: {e}")

    # 转换为 DataFrame
    df = pd.read_json(json.dumps(policies_list))
    return df

# ================= 使用说明 =================
# 1. 修改下方的路径为您本地 terrascan 仓库的实际路径
#    例如: "C:/Users/YourName/Code/terrascan" 或 "/Users/YourName/terrascan"
local_repo_path = "./NCCL/terrascan"  

# 2. 执行解析
df_umi = parse_terrascan_policies(local_repo_path)

# 3. 查看与保存
if not df_umi.empty:
    print(f"成功提取了 {len(df_umi)} 条策略！")
    print(df_umi[['Reference_ID', 'Name', 'Severity']].head())
    
    # 保存为 CSV 方便您做后续的 UMI 映射工作
    df_umi.to_csv("./NCCL/Terrascan_K8s_Policies_UMI.csv", index=False, encoding='utf-8-sig')
    print("结果已保存为 Terrascan_K8s_Policies_UMI.csv")
else:
    print("未提取到数据，请检查路径或文件结构。")