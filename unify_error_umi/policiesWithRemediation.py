import pandas as pd

# 1. 读取 CSV 文件
# 请确保文件名与您的实际文件名一致
policies_df = pd.read_csv('./NCCL/policies3.csv')
umi_df = pd.read_csv('./NCCL/KubeLinter_Policies_UMI.csv')

# 2. 创建映射字典
# 将 KubeLinter_Policies_UMI.csv 中的 Description 映射到 Remediation
# drop_duplicates 确保如果 Description 有重复，只保留一个（防止映射出错）
umi_unique = umi_df.drop_duplicates(subset=['Description'])
description_to_remediation = dict(zip(umi_unique['Description'], umi_unique['Remediation']))

# 3. 定义处理函数
def get_remediation(policy_value):
    # 检查是否为空值 (NaN, 空字符串, 或字符串 "null")
    if pd.isna(policy_value) or policy_value == "" or str(policy_value).lower() == "null":
        return "null"
    
    # 去除首尾空格，防止匹配失败
    policy_str = str(policy_value).strip()
    
    # 在字典中查找对应的 Remediation
    if policy_str in description_to_remediation:
        return description_to_remediation[policy_str]
    else:
        # 如果不为空但在另一个表中找不到对应描述，根据逻辑也设为 "null"
        return "null"

# 4. 应用函数生成新列
policies_df['Remediation'] = policies_df['Kube_Linter_Policy'].apply(get_remediation)

# 5. 保存结果到新文件
output_file = './NCCL/policies_with_remediation.csv'
policies_df.to_csv(output_file, index=False)

print(f"处理完成，文件已保存为: {output_file}")