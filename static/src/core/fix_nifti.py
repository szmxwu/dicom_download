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


def _fix_orientation_data(data: np.ndarray) -> np.ndarray:
    """对 numpy 数组执行方向修复（水平翻转第二维/列方向）。"""
    if data.ndim == 2:
        return data[:, ::-1]
    elif data.ndim >= 3:
        return data[:, ::-1, ...]
    return data


def fix_nifti_orientation_error(nifti_img: nib.Nifti1Image) -> nib.Nifti1Image:
    """
    修复 NIfTI 方向错误（X轴翻转/左右翻转）
    
    问题：dcm2niix 在缺少 ImageOrientationPatient 时会生成 X 轴翻转的 NIfTI
    修复：水平翻转数据（翻转第二维/列方向）
    
    参数:
        nifti_img: 输入的 NIfTI 图像
        
    返回:
        修复后的 NIfTI 图像
    """
    data = nifti_img.get_fdata()
    affine = nifti_img.affine.copy()
    header = nifti_img.header.copy()
    fixed_data = _fix_orientation_data(data)
    return nib.Nifti1Image(fixed_data.astype(np.float32), affine, header)


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
    fixed_data = _fix_photometric_data(data)
    return nib.Nifti1Image(fixed_data.astype(np.float32), affine, header)


def _fix_photometric_data(data: np.ndarray) -> np.ndarray:
    """对 numpy 数组执行灰度反相修复。"""
    data_min = data.min()
    data_max = data.max()
    return data_max - (data - data_min)


def fix_nifti_file(
    filepath: str,
    fix_orientation: bool = False,
    fix_photometric: bool = False,
    backup: bool = False,
    debug: bool = False,  # 调试模式：保存 .original 和 .fixed 对比文件
    img: Optional[nib.Nifti1Image] = None  # 可选：传入已加载的 NIfTI 图像，避免重复 nib.load()
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
        # 加载原始文件（如果调用者已提供 img，则复用，避免重复 nib.load()）
        if img is None:
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
        
        # 直接在 numpy 数组上修复，避免修复函数内部重复 get_fdata()。
        # 原本流程：get_fdata() → 创建 NIfTI → 修复函数 get_fdata() → 创建 NIfTI → 再 get_fdata()
        # 优化后：get_fdata() 一次 → 直接修改数组 → 创建最终 NIfTI。
        fixed_data = original_data
        fixes_applied = []
        
        # 应用方向修复
        if fix_orientation:
            logger.info("  [Fix NIfTI] Fixing orientation error for %s", os.path.basename(filepath))
            fixed_data = _fix_orientation_data(fixed_data)
            fixes_applied.append('orientation_flip_x')
        
        # 应用灰度修复
        if fix_photometric:
            logger.info("  [Fix NIfTI] Fixing photometric inversion for %s", os.path.basename(filepath))
            fixed_data = _fix_photometric_data(fixed_data)
            fixes_applied.append('photometric_inversion')
        
        fixed_img = nib.Nifti1Image(fixed_data.astype(np.float32), affine, header)
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



