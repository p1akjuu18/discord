import pandas as pd
import os

def process_excel():
    # 获取桌面路径
    desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
    file_path = os.path.join(desktop_path, "result3.xlsx")
    
    # 读取Excel文件
    df = pd.read_excel(file_path)
    
    # 打印所有列名
    print("Excel文件中的列名：")
    print(df.columns.tolist())
    
    # 将duration_hours转换为分钟
    if 'duration_hours' in df.columns:
        df['duration_hours'] = df['duration_hours'] * 60
    else:
        print("警告：找不到 'duration_hours' 列")
    
    # 标准化方向列
    if 'analysis_方向' in df.columns:
        df['analysis_方向'] = df['analysis_方向'].replace({
            '多单': '多',
            '多头': '多',
            '空单': '空',
            '空头': '空'
        })
    else:
        print("警告：找不到 'analysis_方向' 列")
    
    # 保存处理后的文件到桌面
    output_path = os.path.join(desktop_path, "result3_processed.xlsx")
    df.to_excel(output_path, index=False)
    print(f"文件处理完成，已保存为 {output_path}")

if __name__ == "__main__":
    process_excel() 