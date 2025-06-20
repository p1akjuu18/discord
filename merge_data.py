import pandas as pd
import os

# 获取桌面路径
desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")

def check_file_exists(file_path):
    if not os.path.exists(file_path):
        print(f"错误：找不到文件 '{file_path}'")
        print(f"请确保文件位于以下目录：{desktop_path}")
        return False
    return True

try:
    # 构建完整的文件路径
    file_1 = os.path.join(desktop_path, "1.xlsx")
    file_history = os.path.join(desktop_path, "history.csv")

    # 检查文件是否存在
    if not check_file_exists(file_1):
        exit(1)
    if not check_file_exists(file_history):
        exit(1)

    # 读取中文文件（1文件）
    print("正在读取1.xlsx...")
    df_cn = pd.read_excel(file_1)

    # 读取英文文件（history文件）
    print("正在读取history.csv...")
    df_en = pd.read_csv(file_history)

    # 创建映射字典
    column_mapping = {
        '委托时间': 'Time(UTC)',
        '合约': 'Symbol',
        '买卖': 'Side',
        '委托价格': 'Price',
        '委托数量': 'Amount',
        '成交额': 'Executed Quote Amount',
        '成交均价': 'Average Price',
        '成交量': 'Executed Amount'
    }

    # 重命名中文文件的列名
    print("正在处理数据...")
    df_cn = df_cn.rename(columns=column_mapping)

    # 将买卖列的值转换为英文
    df_cn['Side'] = df_cn['Side'].map({'买入': 'buy', '卖出': 'sell'})

    # 合并数据
    merged_df = pd.concat([df_en, df_cn], ignore_index=True)

    # 保存合并后的文件到桌面
    output_file = os.path.join(desktop_path, "merged_history.csv")
    print(f"正在保存合并后的文件到 {output_file}...")
    merged_df.to_csv(output_file, index=False)

    print("数据合并完成！新文件已保存到桌面：merged_history.csv")

except Exception as e:
    print(f"发生错误：{str(e)}")
    print("请确保：")
    print("1. 1.xlsx和history.csv文件都在桌面上")
    print("2. 文件没有被其他程序占用")
    print("3. 您有足够的权限读写文件") 