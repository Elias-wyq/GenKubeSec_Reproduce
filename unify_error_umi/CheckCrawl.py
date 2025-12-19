import requests
import pandas as pd
import re
import os

def fetch_unique_checkov_policies():
    # 1. GitHub Raw 地址
    url = "https://raw.githubusercontent.com/bridgecrewio/checkov/main/docs/5.Policy%20Index/kubernetes.md"
    
    print(f"正在下载文档: {url} ...")
    try:
        response = requests.get(url)
        response.raise_for_status()
        content = response.text
    except Exception as e:
        print(f"下载失败: {e}")
        return None

    print("下载成功，正在解析...")

    # 2. 清洗函数
    def clean_markdown_cell(text):
        if not isinstance(text, str): return text
        text = text.strip()
        # 去除 Markdown 链接: [CKV_K8S_1](...) -> CKV_K8S_1
        text = re.sub(r'\[([^\]]+)\]\([^\)]+\)', r'\1', text)
        # 去除代码反引号: `Pod` -> Pod
        text = re.sub(r'`([^`]+)`', r'\1', text)
        return text

    # 3. 解析表格
    lines = content.split('\n')
    data = []
    headers = []
    
    for line in lines:
        stripped = line.strip()
        # 必须包含管道符才是表格
        if "|" not in stripped: continue
        # 跳过分隔行 (---|---)
        if set(stripped.replace('|', '').replace(' ', '')) == {'-'}: continue

        cells = [c.strip() for c in stripped.strip('|').split('|')]
        
        # 识别表头
        if not headers:
            potential_headers = [h.lower() for h in cells]
            # 只要包含 id 和 policy 就可以开始提取
            if "id" in potential_headers and "policy" in potential_headers:
                headers = potential_headers
                print(f"锁定表头结构: {headers}")
            continue

        # 提取数据行
        if headers and len(cells) == len(headers):
            row = {}
            for i, h in enumerate(headers):
                row[h] = clean_markdown_cell(cells[i])
            data.append(row)

    # 4. 数据处理
    df = pd.DataFrame(data)
    
    if df.empty:
        return df

    # 标准化列名 (将 id -> Id, policy -> Policy)
    col_mapping = {}
    if 'id' in df.columns: col_mapping['id'] = 'Id'
    if 'policy' in df.columns: col_mapping['policy'] = 'Policy'
    
    df.rename(columns=col_mapping, inplace=True)

    # --- 关键修改 1: 只保留 Id 和 Policy 列 ---
    required_cols = ['Id', 'Policy']
    # 确保这两列都存在
    existing_cols = [c for c in required_cols if c in df.columns]
    df = df[existing_cols]

    # --- 关键修改 2: 根据 Id 去重 ---
    if 'Id' in df.columns:
        initial_count = len(df)
        # keep='first' 表示保留第一次出现的，删除后续重复的
        df.drop_duplicates(subset=['Id'], keep='first', inplace=True)
        final_count = len(df)
        print(f"去重处理: 删除了 {initial_count - final_count} 条重复 ID 的记录。")

    return df

# ================= 执行 =================
df_result = fetch_unique_checkov_policies()

if df_result is not None and not df_result.empty:
    print("-" * 60)
    print(f"最终有效规则数: {len(df_result)}")
    print(df_result.head())
    
    # --- 关键修改 3: 确保输出目录存在 ---
    output_dir = "./NCCL"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"创建目录: {output_dir}")

    output_csv = f"{output_dir}/Checkov_K8s.csv"
    df_result.to_csv(output_csv, index=False, encoding='utf-8-sig')
    print(f"\n文件已保存至: {output_csv}")
else:
    print("警告: 未提取到有效数据。")