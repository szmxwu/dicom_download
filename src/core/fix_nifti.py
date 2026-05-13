# -*- coding: utf-8 -*-
"""
NIfTI 文件自动修复模块

当 QC 检测到以下问题时，自动修复 NIfTI 文件：
1. NIFTI_ORIENTATION_ERROR: Y轴翻转问题（dcm2niix bug）
2. PHOTOMETRIC_MISMATCH: 灰度反相问题（dcm2niix bug）

修复后的文件会覆盖原文件，并在 QC 报告中标记修复状态。
"""

import os
import logging
import tempfile
import shutil
from typing import Optional, Dict, Any

import numpy as np
import nibabel as nib

logger = logging.getLogger('DICOMApp')


class NiftiFixResult:
    """NIfTI 修复结果"""
    def __init__(
        self,
        success: bool,
        fixes_applied: list,
        original_shape: tuple,
        fixed_shape: tuple,
        error_message: Optional[str] = None
    ):
        self.success = success
        self.fixes_applied = fixes_applied  # 应用的修复列表
        self.original_shape = original_shape
        self.fixed_shape = fixed_shape
        self.error_message = error_message
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'success': self.success,
            'fixes_applied': self.fixes_applied,
            'original_shape': self.original_shape,
            'fixed_shape': self.fixed_shape,
            'error_message': self.error_message
        }


def fix_nifti_orientation_error(nifti_img: nib.Nifti1Image) -> nib.Nifti1Image:
    """
    修复 NIfTI 方向错误（X轴翻转/左右翻转）
    
    问题：dcm2niix 在缺少 ImageOrientationPatient 时会生成 X 轴翻转的 NIfTI
    修复：水平翻转数据（翻转第二维/列方向）
    
    注意：这里假设数据已经经过 nib.as_closest_canonical 处理，
    所以只需要简单的水平翻转即可恢复正确的方向。
    
    参数:
        nifti_img: 输入的 NIfTI 图像
        
    返回:
        修复后的 NIfTI 图像
    """
    data = nifti_img.get_fdata()
    affine = nifti_img.affine.copy()
    header = nifti_img.header.copy()
    
    # 水平翻转（翻转第二维/列方向 - X轴方向）
    if data.ndim == 2:
        fixed_data = data[:, ::-1]
    elif data.ndim == 3:
        if data.shape[2] == 1:
            # 2D 图像（伪 3D）
            fixed_data = data[:, ::-1, :]
        else:
            # 真正的 3D 体积 - 翻转第二维（列方向）
            fixed_data = data[:, ::-1, :]
    else:
        # 4D 或更高维，只翻转空间维度的第二维
        fixed_data = data[:, ::-1, ...]
    
    # 创建新的 NIfTI 图像
    fixed_img = nib.Nifti1Image(fixed_data.astype(np.float32), affine, header)
    
    return fixed_img


def fix_nifti_photometric_inversion(nifti_img: nib.Nifti1Image) -> nib.Nifti1Image:
    """
    修复 NIfTI 灰度反相问题
    
    问题：dcm2niix 有时不能正确处理 PhotometricInterpretation，
          导致 MONOCHROME1 图像灰度反相
    修复：对数据进行灰度反转（max - value）
    
    参数:
        nifti_img: 输入的 NIfTI 图像
        
    返回:
        修复后的 NIfTI 图像
    """
    data = nifti_img.get_fdata()
    affine = nifti_img.affine.copy()
    header = nifti_img.header.copy()
    
    # 灰度反转：新值 = 最大值 - 原值
    # 注意：这里使用数据范围而不是 255，因为 NIfTI 存储的是原始像素值
    data_min = data.min()
    data_max = data.max()
    
    # 反转灰度
    fixed_data = data_max - (data - data_min)
    
    # 创建新的 NIfTI 图像
    fixed_img = nib.Nifti1Image(fixed_data.astype(np.float32), affine, header)
    
    return fixed_img


def fix_nifti_file(
    filepath: str,
    fix_orientation: bool = False,
    fix_photometric: bool = False,
    backup: bool = False,
    debug: bool = False  # 调试模式：保存 .original 和 .fixed 对比文件
) -> NiftiFixResult:
    """
    修复单个 NIfTI 文件
    
    参数:
        filepath: NIfTI 文件路径
        fix_orientation: 是否修复方向错误
        fix_photometric: 是否修复灰度反相
        backup: 是否备份原文件（添加 .backup 后缀）
        debug: 是否开启调试模式（保存 .original.nii.gz 和 .fixed.nii.gz 供对比）
        
    返回:
        NiftiFixResult: 修复结果
    """
    temp_path = None
    try:
        # 加载原始文件
        img = nib.load(filepath)
        original_data = img.get_fdata().copy()  # 复制数据，避免文件句柄问题
        original_shape = original_data.shape
        affine = img.affine.copy()
        header = img.header.copy()
        
        # 调试模式：保存原始文件副本（修复前）
        if debug and (fix_orientation or fix_photometric):
            original_save_path = filepath.replace('.nii.gz', '.original.nii.gz')
            if not os.path.exists(original_save_path):
                original_img = nib.Nifti1Image(original_data.astype(np.float32), affine, header)
                nib.save(original_img, original_save_path)
                print(f"  [Debug] Saved original file: {os.path.basename(original_save_path)}")
        
        # 创建新的图像对象（不依赖原文件）
        fixed_img = nib.Nifti1Image(original_data.astype(np.float32), affine, header)
        
        fixes_applied = []
        
        # 应用方向修复
        if fix_orientation:
            logger.info("  [Fix NIfTI] Fixing orientation error for %s", os.path.basename(filepath))
            fixed_img = fix_nifti_orientation_error(fixed_img)
            fixes_applied.append('orientation_flip_x')
        
        # 应用灰度修复
        if fix_photometric:
            logger.info("  [Fix NIfTI] Fixing photometric inversion for %s", os.path.basename(filepath))
            fixed_img = fix_nifti_photometric_inversion(fixed_img)
            fixes_applied.append('photometric_inversion')
        
        fixed_data = fixed_img.get_fdata()
        fixed_shape = fixed_data.shape
        
        # 调试模式：保存修复后的文件副本（不覆盖原文件，单独保存供对比）
        if debug and (fix_orientation or fix_photometric):
            fixed_save_path = filepath.replace('.nii.gz', '.fixed.nii.gz')
            nib.save(fixed_img, fixed_save_path)
            print(f"  [Debug] Saved fixed file: {os.path.basename(fixed_save_path)}")
        
        # 保存修复后的文件（使用临时文件+重命名，避免覆盖问题）
        temp_dir = os.path.dirname(filepath)
        fd, temp_path = tempfile.mkstemp(suffix='.nii.gz', dir=temp_dir)
        try:
            os.close(fd)  # Close the file descriptor
            nib.save(fixed_img, temp_path)
            
            # Verify the temp file was created successfully
            if not os.path.exists(temp_path):
                raise IOError(f"Temp file {temp_path} was not created")
            
            # Get original file stats
            original_stat = os.stat(filepath)
            
            # Replace original file with temp file
            shutil.move(temp_path, filepath)
            
            # Restore original file permissions
            os.chmod(filepath, original_stat.st_mode)
            
            logger.info(
                "  [Fix NIfTI] Fixed %s: %s",
                os.path.basename(filepath),
                ', '.join(fixes_applied) if fixes_applied else 'no changes'
            )
            print("✅ NIFTI file fixed:fix_orientation=%s, fix_photometric=%s" % (fix_orientation, fix_photometric))
        except Exception as e:
            raise e
        finally:
            # Clean up temp file if it exists (in case of exception)
            if temp_path and os.path.exists(temp_path):
                try:
                    os.unlink(temp_path)
                except:
                    pass
        
        return NiftiFixResult(
            success=True,
            fixes_applied=fixes_applied,
            original_shape=original_shape,
            fixed_shape=fixed_shape
        )
        
    except Exception as e:
        logger.error("  [Fix NIfTI] Failed to fix %s: %s", filepath, e)
        # Clean up temp file if it exists
        if temp_path and os.path.exists(temp_path):
            try:
                os.unlink(temp_path)
            except:
                pass
        return NiftiFixResult(
            success=False,
            fixes_applied=[],
            original_shape=(),
            fixed_shape=(),
            error_message=str(e)
        )



