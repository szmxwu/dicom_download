import os
import shutil
import logging
from PIL import Image
import numpy as np
from scipy.stats import entropy
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import queue

logger = logging.getLogger('DICOMApp')


def is_low_quality_png(png_path, bright_threshold=0.05, max_components_threshold=10):
    """
    Check if a PNG image is low quality (segmented/masked image with pure white regions).

    Low quality images (like segmentation masks) typically have:
    - Large continuous areas of pure white pixels (value = 255)
    - Only 1 or very few connected components in the 255 pixel mask
    - This is because they are binary masks saved as grayscale

    Normal medical images (even overexposed) have:
    - White pixels scattered as many small dots across the image
    - Hundreds or thousands of connected components
    - Even bright areas don't form single large continuous regions

    Args:
        png_path (str): Path to the PNG file
        bright_threshold (float): Minimum ratio of bright pixels (>250) to check further,
                                  default 0.05 (5%)
        max_components_threshold (int): Maximum number of 255-pixel connected components
                                        for an image to be considered segmented,
                                        default 10 (segmented images usually have 1-5)

    Returns:
        bool: True if the image is low quality, False otherwise
    """
    try:
        img = Image.open(png_path).convert('L')
        img_array = np.array(img)

        # Calculate metrics
        total_pixels = img_array.size

        # Check for bright pixels (value > 250)
        bright_mask = img_array > 250
        bright_pixels = np.sum(bright_mask)
        bright_ratio = bright_pixels / total_pixels

        if bright_pixels == 0:
            return False

        # Count connected components of pure white pixels (255)
        # This is the key differentiator:
        # - Segmented images: 1 large continuous white region
        # - Normal X-rays: hundreds of scattered white pixels
        from scipy import ndimage
        pure_white_mask = img_array == 255
        labeled_array, num_components = ndimage.label(pure_white_mask)

        # A segmented image must have:
        # 1. Enough bright pixels (> bright_threshold)
        # 2. Very few connected components (continuous white regions)
        has_bright_area = bright_ratio > bright_threshold
        is_continuous = num_components <= max_components_threshold

        is_low = has_bright_area and is_continuous

        if is_low:
            logger.info(
                f"Low quality PNG detected: {png_path} "
                f"(bright_ratio={bright_ratio:.4f}, num_components={num_components})"
            )

        return is_low

    except Exception as e:
        logger.warning(f"Failed to analyze PNG quality: {png_path}, error: {e}")
        return False  # Copy the file if we can't analyze it


def _copy_file_worker(task, skip_low_quality, bright_threshold, max_components_threshold,
                     copied_count, skipped_count, lock, low_quality_set):
    """
    Worker function to copy a single file.

    Returns:
        tuple: (success, is_low_quality, message)
    """
    src_file, dest_file, dest_path, low_path = task

    try:
        # Ensure destination directory exists (thread-safe)
        os.makedirs(dest_path, exist_ok=True)

        # Check if low quality
        is_low = False
        if skip_low_quality:
            is_low = is_low_quality_png(src_file, bright_threshold, max_components_threshold)

        if skip_low_quality and is_low:
            with lock:
                skipped_count[0] += 1
                # Track low quality directory for this file's parent
                low_quality_set.add(low_path)
            dest_file = os.path.join(low_path, os.path.basename(src_file))
            os.makedirs(low_path, exist_ok=True)
            shutil.copy2(src_file, dest_file)
            return True, True, f"Skipped low quality PNG: {src_file}"
        else:
            shutil.copy2(src_file, dest_file)
            with lock:
                copied_count[0] += 1
            return True, False, None

    except Exception as e:
        return False, False, f"Failed to copy {src_file}: {e}"


def copy_png_structure(source_dir, destination_dir, skip_low_quality=True,
                       bright_threshold=0.05, max_components_threshold=10, max_workers=8):
    """
    复制指定目录下的子文件夹结构，只保留 PNG 文件。使用多线程加速拷贝。

    参数:
        source_dir (str): 源目录路径
        destination_dir (str): 目标目录路径
        skip_low_quality (bool): 是否跳过低质量 PNG 图像（分割/掩码图像，有大量纯白区域）
        bright_threshold (float): 高亮像素(>250)比例阈值，默认0.05(5%)
        max_components_threshold (int): 255像素连通区域数阈值，默认10
                                         分割图通常只有1-5个连续白区，正常X光有数百个分散亮点
        max_workers (int): 最大线程数，默认为8，可根据SSD性能调整
    """
    # 确保目标目录存在
    os.makedirs(destination_dir, exist_ok=True)
    low_quality_root = os.path.join(destination_dir, "low_quality")
    os.makedirs(low_quality_root, exist_ok=True)

    # 第一阶段：收集所有PNG文件任务
    logger.info("正在扫描PNG文件...")
    tasks = []

    for root, dirs, files in os.walk(source_dir):
        relative_path = os.path.relpath(root, source_dir)
        dest_path = os.path.join(destination_dir, relative_path)

        for file in files:
            if file.lower().endswith('.png'):
                src_file = os.path.join(root, file)
                dest_file = os.path.join(dest_path, file)
                low_path = os.path.join(low_quality_root, relative_path)
                tasks.append((src_file, dest_file, dest_path, low_path))

    if not tasks:
        logger.info("未找到PNG文件")
        print("未找到PNG文件")
        return

    total_files = len(tasks)
    logger.info(f"找到 {total_files} 个PNG文件，开始拷贝...")

    # 使用线程安全的计数器
    copied_count = [0]
    skipped_count = [0]
    lock = Lock()
    low_quality_set = set()

    # 第二阶段：多线程拷贝文件
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务
        future_to_task = {
            executor.submit(
                _copy_file_worker,
                task,
                skip_low_quality,
                bright_threshold,
                max_components_threshold,
                copied_count,
                skipped_count,
                lock,
                low_quality_set
            ): task for task in tasks
        }

        # 使用tqdm显示进度
        last_logged_progress = 0
        log_interval = max(1, total_files // 20)  # 每5%记录一次日志

        with tqdm(total=total_files, desc="拷贝进度", unit="文件") as pbar:
            for future in as_completed(future_to_task):
                success, is_low, message = future.result()

                if not success:
                    logger.error(message)
                    tqdm.write(message)
                elif is_low and message:
                    logger.info(message)  # 记录低质量文件到日志
                    tqdm.write(message)

                pbar.update(1)
                pbar.set_postfix({
                    '已拷贝': copied_count[0],
                    '已跳过': skipped_count[0]
                })

                # 定期记录进度到日志
                processed = copied_count[0] + skipped_count[0]
                if processed - last_logged_progress >= log_interval:
                    logger.info(f"拷贝进度: {processed}/{total_files} ({processed*100//total_files}%) - "
                               f"已拷贝: {copied_count[0]}, 已跳过: {skipped_count[0]}")
                    last_logged_progress = processed

    # 输出统计信息
    actual_copied = copied_count[0]
    actual_skipped = skipped_count[0]
    total_processed = actual_copied + actual_skipped

    if total_processed > 0:
        skip_percentage = (actual_skipped / total_processed) * 100
    else:
        skip_percentage = 0

    summary_msg = (
        f"PNG 文件夹结构复制完成。\n"
        f"  - 总文件数: {total_files}\n"
        f"  - 正常拷贝: {actual_copied} 个\n"
        f"  - 低质量跳过: {actual_skipped} 个 ({skip_percentage:.1f}%)\n"
        f"  - 线程数: {max_workers}"
    )

    logger.info(summary_msg)
    print(summary_msg)


def setup_logging(log_file="logs/copy_files.log", level=logging.INFO):
    """
    配置日志输出到文件和控制台。

    参数:
        log_file (str): 日志文件路径，默认为 logs/copy_files.log
        level (int): 日志级别，默认为 INFO
    """
    # 确保日志目录存在
    log_dir = os.path.dirname(log_file)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    # 配置日志格式
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 文件处理器
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(formatter)

    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # 配置根日志记录器
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    return log_file


# 示例用法
if __name__ == "__main__":
    # 配置日志（同时输出到文件和控制台）
    log_path = setup_logging(log_file="logs/copy_files.log", level=logging.INFO)
    print(f"日志文件: {os.path.abspath(log_path)}")

    source = "downloads"  # 替换为你的源目录
    destination = "data"  # 替换为你的目标目录

    # Copy with low-quality filtering enabled (default)
    # 根据SSD性能调整max_workers，通常8-16个线程效果较好
    copy_png_structure(source, destination, max_workers=8)

    # To disable low-quality filtering:
    # copy_png_structure(source, destination, skip_low_quality=False)

    # To customize threshold:
    # copy_png_structure(source, destination, bright_threshold=0.03)
