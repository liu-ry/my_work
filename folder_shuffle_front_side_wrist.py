import os
import shutil
import random
import argparse
import json
from pathlib import Path
import re
import pandas as pd

def parse_arguments():
    parser = argparse.ArgumentParser(description='混合并随机打乱两个文件夹中的文件，保持匹配顺序')
    parser.add_argument('--source1', required=True, help='第一个源文件夹路径 (A)')
    parser.add_argument('--source2', required=True, help='第二个源文件夹路径 (B)')
    parser.add_argument('--target', required=True, help='目标文件夹路径')
    parser.add_argument('--json1', required=True, help='第一个JSON文件路径')
    parser.add_argument('--json2', required=True, help='第二个JSON文件路径')
    parser.add_argument('--seed', type=int, default=42, help='随机种子，用于控制打乱顺序')
    return parser.parse_args()

def natural_sort_key(s):
    """用于自然排序的键函数"""
    return [int(text) if text.isdigit() else text.lower() 
            for text in re.split('([0-9]+)', s)]

def get_files(folder_path, extension):
    """获取指定文件夹下的所有特定扩展名文件，并按自然顺序排序"""
    if not os.path.exists(folder_path):
        print(f"警告: 文件夹不存在 - {folder_path}")
        return []
    
    files = [f for f in os.listdir(folder_path) 
             if os.path.isfile(os.path.join(folder_path, f)) and f.endswith(extension)]
    
    # 按自然顺序排序
    sorted_files = sorted(files, key=natural_sort_key)
    print(f"在 {folder_path} 中找到 {len(sorted_files)} 个{extension}文件")
    return sorted_files

def load_json_lines(file_path):
    """加载JSON行格式文件"""
    with open(file_path, 'r') as f:
        return [json.loads(line) for line in f if line.strip()]

def save_json_lines(data, file_path):
    """保存为JSON行格式文件"""
    with open(file_path, 'w') as f:
        for item in data:
            f.write(json.dumps(item) + '\n')

def process_parquet_file(source_path, target_path, episode_index, start_index):
    """处理Parquet文件，设置episode_index并从start_index开始累加index"""
    try:
        # 读取Parquet文件
        df = pd.read_parquet(source_path)
        
        # 设置episode_index为文件名中的索引
        df['episode_index'] = episode_index
        
        # 从start_index开始累加index
        df['index'] = [i + start_index for i in range(len(df))]
        
        # 保存修改后的文件
        df.to_parquet(target_path, index=False)
        
        # 返回新的start_index（下一个文件的起始值）
        return start_index + len(df)
    
    except Exception as e:
        print(f"处理Parquet文件时出错 - {source_path}: {str(e)}")
        return start_index

def main():
    args = parse_arguments()
    
    # 确保源文件夹存在
    for source in [args.source1, args.source2]:
        if not os.path.exists(source):
            print(f"错误: 源文件夹 '{source}' 不存在")
            return
    
    # 确保JSON文件存在
    for json_file in [args.json1, args.json2]:
        if not os.path.exists(json_file):
            print(f"错误: JSON文件 '{json_file}' 不存在")
            return
    
    # 创建目标文件夹结构
    os.makedirs(args.target, exist_ok=True)
    target_data = os.path.join(args.target, "data", "chunk-000")
    target_front = os.path.join(args.target, "videos", "chunk-000", "observation.images.front_view")
    target_side = os.path.join(args.target, "videos", "chunk-000", "observation.images.side_view")
    target_wrist = os.path.join(args.target, "videos", "chunk-000", "observation.images.wrist_view")
    
    for path in [target_data, target_front, target_side, target_wrist]:
        os.makedirs(path, exist_ok=True)
    
    # 定义源文件夹结构
    source1_groups = {
        "data": os.path.join(args.source1, "data", "chunk-000"),
        "front": os.path.join(args.source1, "videos", "chunk-000", "observation.images.front_view"),
        "side": os.path.join(args.source1, "videos", "chunk-000", "observation.images.side_view"),
        "wrist": os.path.join(args.source1, "videos", "chunk-000", "observation.images.wrist_view")
    }
    
    source2_groups = {
        "data": os.path.join(args.source2, "data", "chunk-000"),
        "front": os.path.join(args.source2, "videos", "chunk-000", "observation.images.front_view"),
        "side": os.path.join(args.source2, "videos", "chunk-000", "observation.images.side_view"),
        "wrist": os.path.join(args.source2, "videos", "chunk-000", "observation.images.wrist_view")
    }
    
    # 验证源文件夹结构是否存在
    for key, group in source1_groups.items():
        if not os.path.exists(group):
            print(f"错误: 源文件夹结构不完整，找不到 {group}")
            alt_path = group.replace("front_view", "front_view")
            if os.path.exists(alt_path):
                print(f"警告: 使用替代路径 {alt_path}")
                source1_groups[key] = alt_path
            else:
                return
    
    for key, group in source2_groups.items():
        if not os.path.exists(group):
            print(f"错误: 源文件夹结构不完整，找不到 {group}")
            alt_path = group.replace("front_view", "front_view")
            if os.path.exists(alt_path):
                print(f"警告: 使用替代路径 {alt_path}")
                source2_groups[key] = alt_path
            else:
                return
    
    # 加载JSON数据
    json_data1 = load_json_lines(args.json1)
    json_data2 = load_json_lines(args.json2)
    
    # 获取各分组的文件并排序
    files1 = {
        "data": get_files(source1_groups["data"], ".parquet"),
        "front": get_files(source1_groups["front"], ".mp4"),
        "side": get_files(source1_groups["side"], ".mp4"),
        "wrist": get_files(source1_groups["wrist"], ".mp4")
    }
    
    files2 = {
        "data": get_files(source2_groups["data"], ".parquet"),
        "front": get_files(source2_groups["front"], ".mp4"),
        "side": get_files(source2_groups["side"], ".mp4"),
        "wrist": get_files(source2_groups["wrist"], ".mp4")
    }
    
    # 创建混合列表 (50+200=250个文件和JSON条目)
    all_files = {
        "data": files1["data"] + files2["data"],
        "front": files1["front"] + files2["front"],
        "side": files1["side"] + files2["side"],
        "wrist": files1["wrist"] + files2["wrist"]
    }
    
    all_json_data = json_data1 + json_data2
    
    # 生成随机排列顺序（使用相同的随机种子以保持匹配）
    random.seed(args.seed)
    random_order = list(range(len(all_files["data"])))
    random.shuffle(random_order)
    
    # 用于跟踪index累加值
    current_index = 0
    
    # 按照随机顺序复制文件和重新排序JSON数据
    shuffled_json_data = []
    
    for j, original_index in enumerate(random_order):
        # 更新episode_index以匹配新顺序
        if original_index < len(json_data1):
            json_entry = json_data1[original_index]
        else:
            json_entry = json_data2[original_index - len(json_data1)]
        
        json_entry['episode_index'] = j
        shuffled_json_data.append(json_entry)
        
        # 处理每个组的文件
        for group_key in ["data", "front", "side", "wrist"]:
            # 获取当前组的所有文件（先files1后files2）
            all_group_files = files1[group_key] + files2[group_key]
            
            # 确保索引在有效范围内
            if original_index >= len(all_group_files):
                print(f"错误: 索引超出范围 - 索引 {original_index}, 文件总数 {len(all_group_files)}")
                continue
            
            # 确定源文件来自哪个源文件夹
            if original_index < len(files1[group_key]):
                # 来自第一个源文件夹
                source_file = files1[group_key][original_index]
                source_path = os.path.join(source1_groups[group_key], source_file)
            else:
                # 来自第二个源文件夹，计算在files2中的正确索引
                files2_index = original_index - len(files1[group_key])
                source_file = files2[group_key][files2_index]
                source_path = os.path.join(source2_groups[group_key], source_file)
            
            # 检查源文件是否存在
            if not os.path.exists(source_path):
                print(f"错误: 文件不存在 - {source_path}")
                print(f"原始索引: {original_index}, 组: {group_key}, 文件: {source_file}")
                continue
            
            # 创建目标文件路径
            target_folder = {
                "data": target_data,
                "front": target_front,
                "side": target_side,
                "wrist": target_wrist
            }[group_key]
            
            # 获取文件扩展名
            file_ext = os.path.splitext(source_file)[1]
            
            # 构建目标文件名
            target_file = f"episode_{j:06d}{file_ext}"
            target_path = os.path.join(target_folder, target_file)
            
            # 处理Parquet文件（只处理data组）
            if group_key == "data" and file_ext == ".parquet":
                current_index = process_parquet_file(
                    source_path, target_path, j, current_index)
                print(f"已处理 {group_key} 组, 文件 {j+1}/{len(random_order)}: {source_file} -> {target_file} (index从{current_index-len(pd.read_parquet(source_path))}到{current_index-1})")
            else:
                # 普通文件复制
                try:
                    shutil.copy2(source_path, target_path)
                    print(f"已处理 {group_key} 组, 文件 {j+1}/{len(random_order)}: {source_file} -> {target_file}")
                except Exception as e:
                    print(f"复制文件时出错 - {source_path} -> {target_path}: {str(e)}")
        
        if (j + 1) % 50 == 0:
            print(f"进度: {j+1}/{len(random_order)}")
    
    # 保存打乱后的JSON数据（合并为一个文件）
    json_output_path = os.path.join(target_data, "episodes.jsonl")
    save_json_lines(shuffled_json_data, json_output_path)
    print(f"合并后的JSON数据已保存到: {json_output_path}")
    print(f"index累加值最终为: {current_index}")
    
    print(f"操作完成! 共处理 {len(random_order)} 个episode")
    print(f"结果保存在: {args.target}")

if __name__ == "__main__":
    main()    
