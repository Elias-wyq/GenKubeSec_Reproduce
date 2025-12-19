import requests
import pandas as pd
import re

def fetch_kubelinter_policies():
    # 1. KubeLinter checks.md 的 Raw 地址
    url = "https://raw.githubusercontent.com/stackrox/kube-linter/main/docs/generated/checks.md"
    
    print(f"正在下载文档: {url} ...")
    try:
        response = requests.get(url)
        response.raise_for_status()
        content = response.text
    except Exception as e:
        print(f"下载失败: {e}")
        return pd.DataFrame()

    print("下载成功，开始解析...")

    policies = []
    
    # 当前正在处理的策略对象
    current_policy = {}
    
    lines = content.split('\n')
    
    for line in lines:
        line = line.strip()
        
        # 2. 识别策略名称 (Markdown 二级标题 ## )
        # 文档结构通常是: ## access-to-create-pods
        if line.startswith('## '):
            # 如果之前已经有一个策略在处理中，先把它保存下来
            if current_policy:
                policies.append(current_policy)
            
            # 开始新策略
            policy_name = line.replace('## ', '').strip()
            current_policy = {
                "Name": policy_name,
                "Enabled_by_default": "", # 初始化为空
                "Description": "",
                "Remediation": ""
            }
            continue

        # 3. 提取具体字段
        # 只有当我们处于某个策略块内部时才提取
        if current_policy:
            
            # 提取 Enabled by default
            # 格式: **Enabled by default**: No
            if "Enabled by default" in line:
                # 去除 Markdown 加粗符号 ** 和前缀
                # 正则查找: 冒号后面的内容
                match = re.search(r'Enabled by default\*\*:\s*(.+)', line)
                if match:
                    current_policy["Enabled_by_default"] = match.group(1).strip()
            
            # 提取 Description
            # 格式: **Description**: ...
            elif line.startswith('**Description**:') or line.startswith('Description:'):
                # 提取冒号后的内容
                desc_text = line.split(':', 1)[1].strip()
                current_policy["Description"] = desc_text
            
            # 提取 Remediation
            # 格式: **Remediation**: ...
            elif line.startswith('**Remediation**:') or line.startswith('Remediation:'):
                rem_text = line.split(':', 1)[1].strip()
                current_policy["Remediation"] = rem_text

    # 循环结束后，别忘了保存最后一个策略
    if current_policy:
        policies.append(current_policy)

    # 4. 转为 DataFrame
    df = pd.DataFrame(policies)
    return df

# ================= 执行 =================
df_kubelinter = fetch_kubelinter_policies()

if not df_kubelinter.empty:
    print("-" * 60)
    print(f"🎉 成功提取 {len(df_kubelinter)} 条 KubeLinter 策略！")
    print(df_kubelinter.head())
    
    # 保存为 CSV
    output_file = "./NCCL/KubeLinter_Policies_UMI.csv"
    df_kubelinter.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"\n💾 文件已保存为: {output_file}")
else:
    print("❌ 未提取到数据，请检查文档结构是否变更。")