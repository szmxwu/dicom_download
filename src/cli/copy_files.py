import os
import shutil
import logging
from PIL import Image
import numpy as np
from scipy.stats import entropy

logger = logging.getLogger('DICOMApp')


def is_low_quality_png(png_path, unique_ratio_threshold=0.001, entropy_threshold=2.0):
    """
    Check if a PNG image is low quality (lacks grayscale diversity, nearly binary).

    Low quality images typically have:
    - Very few unique pixel values (low unique_ratio)
    - Low entropy (concentrated histogram)
    - Often appear as mostly black/white with little gradation

    Args:
        png_path (str): Path to the PNG file
        unique_ratio_threshold (float): Minimum ratio of unique pixels to total pixels
        entropy_threshold (float): Minimum entropy value for acceptable quality

    Returns:
        bool: True if the image is low quality, False otherwise
    """
    try:
        img = Image.open(png_path).convert('L')
        img_array = np.array(img)

        # Calculate metrics
        total_pixels = img_array.size
        unique_pixels = len(np.unique(img_array))
        unique_ratio = unique_pixels / total_pixels

        # Calculate histogram and entropy
        hist, _ = np.histogram(img_array.flatten(), bins=256, range=[0, 256])
        hist_normalized = hist / hist.sum()
        img_entropy = entropy(hist_normalized, base=2)

        # Determine if low quality
        is_low = (unique_ratio < unique_ratio_threshold) or (img_entropy < entropy_threshold)

        if is_low:
            logger.debug(
                f"Low quality PNG detected: {png_path} "
                f"(unique_ratio={unique_ratio:.6f}, entropy={img_entropy:.4f})"
            )

        return is_low

    except Exception as e:
        logger.warning(f"Failed to analyze PNG quality: {png_path}, error: {e}")
        return False  # Copy the file if we can't analyze it


def copy_png_structure(source_dir, destination_dir, skip_low_quality=True, 
                       unique_ratio_threshold=0.001, entropy_threshold=2.0):
    """
    复制指定目录下的子文件夹结构，只保留 PNG 文件。

    参数:
        source_dir (str): 源目录路径
        destination_dir (str): 目标目录路径
        skip_low_quality (bool): 是否跳过低质量 PNG 图像（灰度多样性低，接近黑白两色）
        unique_ratio_threshold (float): 低质量判定的唯一像素比例阈值
        entropy_threshold (float): 低质量判定的熵阈值
    """
    # 确保目标目录存在
    os.makedirs(destination_dir, exist_ok=True)

    copied_count = 0
    skipped_count = 0

    # 遍历源目录
    for root, dirs, files in os.walk(source_dir):
        # 获取相对路径
        relative_path = os.path.relpath(root, source_dir)
        # 构建目标路径
        dest_path = os.path.join(destination_dir, relative_path)

        # 创建目标子目录（如果不存在）
        os.makedirs(dest_path, exist_ok=True)

        # 处理每个文件
        for file in files:
            if file.lower().endswith('.png'):
                src_file = os.path.join(root, file)
                dest_file = os.path.join(dest_path, file)

                # Check if low quality
                if skip_low_quality and is_low_quality_png(
                    src_file, unique_ratio_threshold, entropy_threshold
                ):
                    skipped_count += 1
                    logger.info(f"Skipped low quality PNG: {file}")
                    continue

                shutil.copy2(src_file, dest_file)  # 保留文件元数据
                copied_count += 1

    logger.info(f"PNG 文件夹结构复制完成。已复制：{copied_count} 个文件，跳过：{skipped_count} 个低质量文件")
    print(f"PNG 文件夹结构复制完成。已复制：{copied_count} 个文件，跳过：{skipped_count} 个低质量文件")

# 示例用法
if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    
    source = "downloads"  # 替换为你的源目录
    destination = "data"  # 替换为你的目标目录
    
    # Copy with low-quality filtering enabled (default)
    copy_png_structure(source, destination)
    
    # To disable low-quality filtering:
    # copy_png_structure(source, destination, skip_low_quality=False)
    
    # To customize thresholds:
    # copy_png_structure(source, destination, unique_ratio_threshold=0.0005, entropy_threshold=1.5)