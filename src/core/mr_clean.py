# -*- coding: utf-8 -*-
"""
MRI 数据清洗与智能分类模块

提供 MRI 序列的数据清洗、特征提取、序列分类和动态增强分析功能。

主要功能：
- 原子特征提取（方位、维度、脂肪抑制等）
- 硬件特征标准化（场强、厂商、型号）
- 序列智能分类（T1/T2/FLAIR/DWI 等）
- 动态增强时相分析
"""

import pandas as pd
import numpy as np
import ast
import os
import json
import warnings
from time import time
# 忽略Pandas在进行apply操作时可能产生的性能警告
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)


# 默认配置文件路径（相对于项目根目录）
DEFAULT_CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    'mr_clean_config.json'
)


def load_mr_clean_config(config_path: str | None = None) -> dict:
    """
    加载 MR_clean 规则配置

    从 JSON 文件加载规则配置，包括关键词、阈值、正则表达式等。

    Args:
        config_path: 配置文件路径，默认使用 mr_clean_config.json

    Returns:
        dict: 配置字典
    """
    path = config_path or DEFAULT_CONFIG_PATH
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def _get_cfg(cfg: dict | None, config_path: str | None = None) -> dict:
    return cfg if cfg is not None else load_mr_clean_config(config_path)

# ==============================================================================
# Part 1: 辅助函数 (Helper Functions)
# ==============================================================================


def safe_to_numeric(value):
    """
    安全地将输入值转换为浮点数。

    如果转换失败（例如，值为空或非数值字符串），则返回np.nan，
    这比返回0能更清晰地表示数据缺失或无效。

    Args:
        value: 需要转换的任意值。

    Returns:
        float or np.nan: 转换后的浮点数或np.nan。
    """
    try:
        # 首先尝试转换为float，可以处理整数和浮点数形式的字符串
        return float(value)
    except (ValueError, TypeError):
        # 如果转换失败，返回NaN
        return np.nan


def get_orientation(row, cfg: dict, iop_col='ImageOrientationPatient', fallback_col='protocolName_lower'):
    """
    通过物理参数计算或从协议名回退来获取扫描方位。

    优先通过DICOM标签ImageOrientationPatient(IOP)计算法向量来确定方位。
    此方法能够精确区分轴位(AX)，矢状位(SAG)，冠状位(COR)，并能识别斜位(OBL)。
    当IOP数据无效或缺失时，则从协议名称中搜索关键词作为备用方案。

    Args:
        row (pd.Series): DataFrame的一行。
        iop_col (str): 包含IOP数据的列名。
        fallback_col (str): 用于关键词搜索的回退列名。

    Returns:
        str: 标准化的方位名称 ('AX', 'SAG', 'COR', 'OBL', 'UNKNOWN')。
    """
    orientation_cfg = cfg.get('orientation', {})
    oblique_ratio = float(orientation_cfg.get('oblique_dominance_ratio', 0.9))

    # 1. 优先从ImageOrientationPatient计算
    iop_val = row.get(iop_col)
    if pd.notnull(iop_val):
        try:
            # 安全地将字符串 '[-1.0, 0.0, ...]' 转换为数值列表
            iop = ast.literal_eval(str(iop_val))
            if isinstance(iop, list) and len(iop) == 6:
                row_vec = np.array(iop[0:3])
                col_vec = np.array(iop[3:6])
                normal = np.cross(row_vec, col_vec)

                # 检查是否为斜位：如果没有一个轴占绝对主导，则为斜位
                # 判断依据：主轴分量的平方是否小于向量模长平方的 oblique_ratio
                if np.max(np.abs(normal))**2 < oblique_ratio * np.sum(normal**2):
                    return 'OBL'

                main_axis = np.argmax(np.abs(normal))
                if main_axis == 0:
                    return 'SAG'  # 法向量主轴为X
                elif main_axis == 1:
                    return 'COR'  # 法向量主轴为Y
                elif main_axis == 2:
                    return 'AX'  # 法向量主轴为Z
        except (ValueError, SyntaxError, TypeError):
            pass  # 解析失败则继续执行回退逻辑

    # 2. 回退逻辑：从协议名搜索
    protocol_name = str(row.get(fallback_col, '')).lower()

    fallback_keywords = orientation_cfg.get('fallback_keywords', {})
    for orientation, keywords in fallback_keywords.items():
        if any(k in protocol_name for k in keywords):
            return str(orientation)

    return 'UNKNOWN'


def detect_fat_suppression(row, cfg: dict):
    """
    通过层级化规则判断序列是否应用了脂肪抑制技术。

    优先级从高到低：STIR物理参数 -> Dixon技术标签 -> ScanOptions标签 -> 协议名关键词。

    Args:
        row (pd.Series): DataFrame的一行数据。

    Returns:
        bool: 如果是脂肪抑制序列，则返回True，否则返回False。
    """
    fs_cfg = cfg.get('fat_suppression', {})

    # 方法一：基于TI识别STIR序列 (最高优先级)
    if str(fs_cfg.get('ir_token', 'IR')) in str(row.get('ScanningSequence', '')):
        ti = safe_to_numeric(row.get('InversionTime'))
        # STIR的典型TI范围
        stir_ti_min = safe_to_numeric(fs_cfg.get('stir_ti_min', 100))
        stir_ti_max = safe_to_numeric(fs_cfg.get('stir_ti_max', 250))
        if stir_ti_min <= ti <= stir_ti_max:
            return True

    # 方法二：识别Dixon（水脂分离）技术的“纯水像”
    image_type_str = str(row.get('ImageType', '')).upper()
    # 更鲁棒地解析多值字符串
    image_type_parts = image_type_str.split('\\')
    dixon_tokens = [str(x).upper() for x in fs_cfg.get('dixon_water_tokens', ['W', 'WATER'])]
    if any(t in image_type_parts for t in dixon_tokens):
        return True

    # 方法三：解析专用的扫描选项（ScanOptions）标签
    fs_token = str(fs_cfg.get('scan_options_fs_token', 'FS')).upper()
    if fs_token and fs_token in str(row.get('ScanOptions', '')).upper():
        return True

    # 方法四：关键词匹配（作为补充和回退）
    protocol_name = str(row.get('protocolName_lower', '')).lower()
    fat_sat_keywords = [str(x).lower() for x in fs_cfg.get('protocol_keywords', ['fs', 'fatsat', 'spair', 'stir', 'fat sep', 'dixon'])]
    if any(keyword in protocol_name for keyword in fat_sat_keywords):
        return True

    return False

# ==============================================================================
# Part 2: 阶段一 - 提取原子特征 (Extract Atomic Features)
# ==============================================================================


def extract_atomic_features(df, cfg: dict, progress_callback=None):
    """
    从原始DataFrame中派生出一系列标准化的“原子特征”列。
    这些特征是后续进行序列分类的基础。

    Args:
        df (pd.DataFrame): 包含原始DICOM信息的DataFrame。

    Returns:
        pd.DataFrame: 增加了标准化特征列的DataFrame。
    """
    if progress_callback:
        progress_callback("Stage 1: extracting atomic features...", "extract_atomic_features")
    else:
        print("Stage 1: extracting atomic features...")

    # -- 预处理 --
    # 为关键词匹配准备小写、无空值的列
    df['protocolName_lower'] = df['ProtocolName'].astype(
        str).str.lower().fillna('')
    # ImageType是权威的DICOM标签，应优先使用
    df['imageType_lower'] = df.get('ImageType', pd.Series(
        index=df.index)).astype(str).str.lower().fillna('')

    # -- 特征提取 --
    atomic_cfg = cfg.get('atomic_features', {})

    # 1. 方位 (Orientation)
    df['standardOrientation'] = df.apply(lambda r: get_orientation(r, cfg), axis=1)

    # 2. 维度 (Dimension)
    df['standardDimension'] = df.get('MRAcquisitionType', pd.Series(
        index=df.index)).astype(str).fillna('UNKNOWN')

    # 3. 附加技术特征 (布尔型)
    df['isFatSuppressed'] = df.apply(lambda r: detect_fat_suppression(r, cfg), axis=1)

    contrast_regex = str(atomic_cfg.get('contrast_protocol_regex', r'\+c|post|gd|enh|contrast|增强|dyn'))
    df['isContrastEnhanced'] = df['protocolName_lower'].str.contains(contrast_regex, na=False, regex=True)

    motion_regex = str(atomic_cfg.get('motion_correction_protocol_regex', 'propeller|blade|radial|star'))
    df['hasMotionCorrection'] = df['protocolName_lower'].str.contains(motion_regex, na=False, regex=True)

    # 4. 图像类型 (Refined ImageType)
    # 优先从权威的'ImageType'字段判断，若无则尝试从协议名猜测
    def get_refined_type(row):
        img_type = row['imageType_lower']
        protocol_name = row['protocolName_lower']
        if 'derived' in img_type or 'secondary' in img_type:
            return 'DERIVED'
        if 'localizer' in img_type or 'localizer' in protocol_name or 'survey' in protocol_name or 'scout' in protocol_name:
            return 'LOCALIZER'
        if 'original' in img_type and 'primary' in img_type:
            return 'ORIGINAL'
        return 'OTHER'
    df['refinedImageType'] = df.apply(get_refined_type, axis=1)

    if progress_callback:
        progress_callback("Done. Added columns: standardOrientation, standardDimension, isFatSuppressed, etc.", "extract_atomic_features_done")
    else:
        print("Done. Added columns: standardOrientation, standardDimension, isFatSuppressed, etc.")
    return df

# ==============================================================================
# Part 3: 阶段二 - 应用规则进行序列分类 (Classify Sequence)
# ==============================================================================


def get_subtype_suffix(row, cfg: dict):
    """
    获取序列的亚型后缀，用于对Dixon等多输出序列进行精细区分。

    该函数检查SeriesDescription和ImageType，寻找特定的关键词，
    并返回一个标准化的后缀字符串。

    Args:
        row (pd.Series): 包含序列信息的DataFrame行。

    Returns:
        str: 标准化的亚型后缀 (如 '_WATER', '_FAT')，如果没有找到则返回空字符串。
    """
    # 准备待检查的文本，优先使用更规范的ImageType
    # SeriesDescription作为补充
    desc = str(row.get('protocolName_lower', '') + ' ' +
               row.get('SeriesDescription', '')).lower()
    img_type_parts = str(row.get('ImageType', '')).upper().split('\\')

    subtype_cfg = cfg.get('subtype_suffix', {})

    # --- 识别Dixon序列的输出类型 ---
    # 使用if/elif确保一个序列只被赋予一个亚型
    water_tokens = [str(x) for x in subtype_cfg.get('water_tokens', ['WATER', ' W ', 'water'])]
    fat_tokens = [str(x) for x in subtype_cfg.get('fat_tokens', ['FAT', ' F ', 'fat'])]
    inphase_tokens = [str(x) for x in subtype_cfg.get('inphase_tokens', ['INPHASE', ' IP ', 'in_phase', 'inphase'])]
    outphase_tokens = [str(x) for x in subtype_cfg.get('outphase_tokens', ['OUTPHASE', ' OP ', 'out_phase', 'outphase'])]

    if 'WATER' in img_type_parts or any(t in desc for t in water_tokens):
        return '_WATER'
    elif 'FAT' in img_type_parts or any(t in desc for t in fat_tokens):
        return '_FAT'
    elif 'INPHASE' in img_type_parts or any(t in desc for t in inphase_tokens):
        return '_INPHASE'
    elif 'OUTPHASE' in img_type_parts or any(t in desc for t in outphase_tokens):
        return '_OUTPHASE'

    # --- 识别其他可能的多回波/多参数输出 ---
    # 示例：识别不同回波时间的T2*序列
    t2_star_marker = str(subtype_cfg.get('t2_star_echo_marker', 't2_star_echo'))
    split_token = str(subtype_cfg.get('t2_star_echo_split_token', 'echo'))
    if t2_star_marker in desc:  # 假设有T2*序列描述为 "t2_star_echo_2"
        try:
            echo_num = ''.join(filter(str.isdigit, desc.split(split_token)[-1]))
            if echo_num:
                return f'_ECHO{echo_num}'
        except:
            pass  # 解析失败则忽略

    # --- 如果未找到任何亚型关键词，返回空字符串 ---
    return ''


def classify_sequence(row, cfg: dict):
    """
    应用层级规则，对每个序列进行分类，确定其核心名称。(版本 v3)

    此版本特性：
    - 根据磁场强度动态调整TR/TE/TI阈值。
    - 优先识别T1/T2 Map等特殊序列。
    - 强化了当物理参数无效或超出范围时的备用（兜底）分类逻辑。
    - 将'blade'等运动校正技术作为后缀处理。

    Args:
        row (pd.Series): 包含原子特征的一行数据。

    Returns:
        str: 序列的分类名称。
    """
    # --- 1. 参数提取与准备 ---
    name = row.get('protocolName_lower', '')
    SeriesDescription= str(row.get('SeriesDescription', '')).lower()
    scan_seq = str(row.get('ScanningSequence', '')).lower()
    seq_variant = str(row.get('SequenceVariant', '')).lower()
    img_type = row.get('refinedImageType', '')
    field_strength = row.get('standardFieldStrength', 'default')
    standardDimension= row.get('standardDimension', '')

    tr = safe_to_numeric(row.get('RepetitionTime'))
    te = safe_to_numeric(row.get('EchoTime'))
    ti = safe_to_numeric(row.get('InversionTime'))
    fa = safe_to_numeric(row.get('FlipAngle'))
    b_val = safe_to_numeric(row.get('b_value'))
    etl = safe_to_numeric(row.get('EchoTrainLength'))
    
    base_class = 'UNKNOWN'
    seq_family = 'UNKNOWN'

    classification_cfg = cfg.get('classification', {})
    thresholds_cfg = cfg.get('thresholds', {})

    field_strength_thresholds = thresholds_cfg.get('field_strength', {})
    P = field_strength_thresholds.get(field_strength, field_strength_thresholds.get('default', {}))


    # --- 3. 分类规则引擎 (按优先级) ---

    # --- 规则A: 优先处理基于名称的、明确的特殊序列 ---
    ruleA = classification_cfg.get('ruleA', {})

    localizer_rule = ruleA.get('LOCALIZER', {})
    localizer_keywords = [str(x).lower() for x in localizer_rule.get('protocol_keywords', ['localizer', 'survey', 'scout'])]
    localizer_img_type = str(localizer_rule.get('refinedImageType', 'LOCALIZER'))
    if any(k in name for k in localizer_keywords) or img_type == localizer_img_type:
        base_class = 'LOCALIZER'
    elif any(k in name for k in [str(x).lower() for x in ruleA.get('T1_MAP', {}).get('protocol_keywords', ['t1_map', 't1map'])]):
        base_class = 'T1_MAP'
    elif any(k in name for k in [str(x).lower() for x in ruleA.get('T2_MAP', {}).get('protocol_keywords', ['t2_map', 't2map'])]):
        base_class = 'T2_MAP'
    elif any(k in name for k in [str(x).lower() for x in ruleA.get('ADC', {}).get('protocol_keywords', ['adc'])]):
        base_class = 'ADC'
    elif any(k in name for k in [str(x).lower() for x in ruleA.get('FA_MAP', {}).get('protocol_keywords', ['fa_map'])]):
        base_class = 'FA_MAP'
    elif any(k in name for k in [str(x).lower() for x in ruleA.get('SUBTRACTION', {}).get('protocol_keywords', ['sub', 'subtract'])]):
        base_class = 'SUBTRACTION'
    elif any(k in name for k in [str(x).lower() for x in ruleA.get('MRA', {}).get('protocol_keywords', ['mra', 'mrv', 'tof'])]):
        base_class = 'MRA'
    elif any(k in name for k in [str(x).lower() for x in ruleA.get('SWI', {}).get('protocol_keywords', ['swi', 'swan'])]):
        base_class = 'SWI'
    elif any(k in name for k in [str(x).lower() for x in ruleA.get('PWI', {}).get('protocol_keywords', ['pwi', 'perf', 'dsc'])]):
        base_class = 'PWI'
    elif any(k in name for k in [str(x).lower() for x in ruleA.get('MRS', {}).get('protocol_keywords', ['mrs', 'svs', 'csi', 'spectro'])]):
        base_class = 'MRS'
    elif any(k in SeriesDescription for k in [str(x).lower() for x in ruleA.get('BREATH MOVEMENT', {}).get('series_description_keywords', ['resp'])]):
        base_class = 'BREATH MOVEMENT'
    elif any(k in SeriesDescription for k in [str(x).lower() for x in ruleA.get('MIP', {}).get('series_description_keywords', ['mip'])]):
        base_class = 'MIP'
    # --- 规则B: 基于物理参数的核心分类 (仅当规则A未命中时执行) ---
    if base_class == 'UNKNOWN':
        # 物理规则1: 功能成像 (DWI, fMRI)
        dwi_min = safe_to_numeric(classification_cfg.get('dwi_b_value_min', 50))
        if b_val > dwi_min:
            base_class = 'DTI' if 'dti' in name else 'DWI'
        else:
            fmri_cfg = classification_cfg.get('fmri', {})
            fmri_seq_token = str(fmri_cfg.get('scan_seq_token', 'ep')).lower()
            fmri_name_keywords = [str(x).lower() for x in fmri_cfg.get('protocol_keywords', ['fmri', 'bold'])]
            fmri_class = str(fmri_cfg.get('class', 'fMRI_BOLD'))
            if fmri_seq_token in scan_seq and any(k in name for k in fmri_name_keywords):
                base_class = fmri_class

        # 物理规则2/3: 仅当仍未知时，继续形态学判断
        if base_class == 'UNKNOWN':
            # 物理规则2: 反转恢复 (FLAIR, STIR)
            fat_cfg = cfg.get('fat_suppression', {})
            stir_ti_min = safe_to_numeric(fat_cfg.get('stir_ti_min', 90))
            flair_ti_min = safe_to_numeric(P.get('flair_ti_min'))
            stir_ti_max = safe_to_numeric(P.get('stir_ti_max'))
            if pd.notnull(ti) and ti >= flair_ti_min:
                base_class = 'T2_FLAIR'
            elif pd.notnull(ti) and stir_ti_min <= ti <= stir_ti_max:
                base_class = 'T2_STIR'
            else:
                # 物理规则3: 形态学成像 (T1, T2, PD)
                # 3a. 判断序列家族
                fam_cfg = classification_cfg.get('sequence_family', {})
                gre_token = str(fam_cfg.get('gre_token', 'gr')).lower()
                se_token = str(fam_cfg.get('se_token', 'se')).lower()
                ss_token = str(fam_cfg.get('steady_state_seq_variant_token', 'ss')).lower()
                sp_token = str(fam_cfg.get('spoiled_seq_variant_token', 'sp')).lower()

                if gre_token in scan_seq:
                    if ss_token in seq_variant:
                        seq_family = 'GRE_STEADY_STATE'
                    elif sp_token in seq_variant:
                        seq_family = 'GRE_SPOILED'
                    else:
                        seq_family = 'GRE'
                elif se_token in scan_seq:
                    single_shot_keywords = [str(x).lower() for x in fam_cfg.get('single_shot_protocol_keywords', ['haste', 'ssfse'])]
                    single_shot_etl_min = safe_to_numeric(fam_cfg.get('single_shot_etl_min', 128))
                    if any(k in name for k in single_shot_keywords) or etl > single_shot_etl_min:
                        seq_family = 'SE_SingleShot'
                    elif etl > 1:
                        seq_family = 'TSE'
                    else:
                        seq_family = 'SE'

                # 3b. 根据家族和TR/TE判断对比度
                if seq_family == 'SE_SingleShot':
                    base_class = 'T2_SE_SingleShot'  # HASTE/SSFSE本质是T2加权
                elif seq_family != 'UNKNOWN':
                    if te > safe_to_numeric(P.get('t2_te_min')):
                        base_class = 'T2_' + seq_family
                    elif tr < safe_to_numeric(P.get('t1_tr_max')) and te < safe_to_numeric(P.get('t1_te_max')):
                        base_class = 'T1_' + seq_family
                    elif tr > safe_to_numeric(P.get('t2_tr_min')) and te < safe_to_numeric(P.get('pd_te_max')) and 'pd' in name:
                        base_class = 'PD_' + seq_family

    # --- 规则C: 兜底方案 - 基于名称的最终猜测 (仅当以上规则全部失败) ---
    if base_class == 'UNKNOWN':
        fallback_cfg = classification_cfg.get('fallback', {})
        if 't2' in name:
            if seq_family == 'UNKNOWN':
                tse_tokens = [str(x).lower() for x in fallback_cfg.get('tse_tokens', ['tse', 'fse'])]
                if any(t in name for t in tse_tokens):
                    base_class = 'T2_TSE'
                elif str(fallback_cfg.get('se_token', 'se')).lower() in name:
                    base_class = 'T2_SE'
                else:
                    base_class = 'T2_NAME_BASED'
            else:
                base_class = 'T2_' + seq_family

        if str(fallback_cfg.get('tse_dark_fluid_to_flair', 'tse_dark_fluid')).lower() in name:
            base_class = 'T2_FLAIR'
        elif 't1' in name:
            if seq_family == 'UNKNOWN':
                tse_tokens = [str(x).lower() for x in fallback_cfg.get('tse_tokens', ['tse', 'fse'])]
                if any(t in name for t in tse_tokens):
                    base_class = 'T1_TSE'
                elif str(fallback_cfg.get('se_token', 'se')).lower() in name:
                    base_class = 'T1_SE'
                elif (
                    all(t in name for t in [str(x).lower() for x in fallback_cfg.get('mpr_iso_tokens', ['mpr', 'iso'])])
                    and standardDimension == str(fallback_cfg.get('requires_dimension_for_flash3d', '3D'))
                ):
                    base_class = 'T1_GRE_FLASH3D'
                else:
                    base_class = 'T1_NAME_BASED'
            else:
                base_class = 'T1_' + seq_family
        elif 'pd' in name:
            base_class = 'PD_' + seq_family
        elif 'flair' in name:
            base_class = 'T2_FLAIR'
        elif 'stir' in name:
            base_class = 'T2_STIR'
        elif 'dwi' in name or 'diff' in name:
            base_class = 'DWI'

    # --- 4. 后处理：附加属性后缀 ---
    if base_class != 'UNKNOWN':
        # 附加Dixon等多输出序列的亚型后缀
        final_class = base_class + get_subtype_suffix(row, cfg)

        # 附加运动校正技术后缀
        mc_cfg = classification_cfg.get('motion_correction', {})
        mc_keywords = [str(x).lower() for x in mc_cfg.get('protocol_keywords', ['blade', 'propeller'])]
        mc_suffix = str(mc_cfg.get('suffix', '_MC'))
        if any(k in name for k in mc_keywords) or row.get('hasMotionCorrection'):
            final_class += mc_suffix

        return final_class
    return 'UNKNOWN'


def extract_hardware_features(df, progress_callback=None):
    """
    阶段三：提取硬件环境与高级参数特征。

    此函数对硬件相关字段进行标准化处理，为最精细粒度的图像质量比较提供依据。
    它处理磁场强度、设备制造商和设备型号，将多样化的原始输入统一为标准格式。

    Args:
        df (pd.DataFrame): 已完成原子特征提取的DataFrame。

    Returns:
        pd.DataFrame: 增加了标准化硬件特征列的DataFrame。
    """
    if progress_callback:
        progress_callback("Stage 2: extracting hardware features and advanced parameters...", "extract_hardware_features")
    else:
        print("Stage 2: extracting hardware features and advanced parameters...")

    # --- 1. 标准化磁场强度 (MagneticFieldStrength) ---
    # 原始数据可能是1.5, 3.0, 或接近的浮点数如1.4999。
    # 我们将其归类到标准的分类中，以增强鲁棒性。

    # 首先确保字段为数值类型，无效值转为NaN
    field_strength_num = df.get('MagneticFieldStrength', pd.Series(
        index=df.index)).apply(safe_to_numeric)

    # 定义分类边界和标签
    bins = [-np.inf, 1.0, 2.0, 4.0, np.inf]
    labels = ['Low-Field', '1.5T', '3.0T', 'High-Field']

    df['standardFieldStrength'] = pd.cut(
        field_strength_num, bins=bins, labels=labels, right=False)
    # 将结果转为字符串，并填充未知值
    df['standardFieldStrength'] = df['standardFieldStrength'].astype(
        str).fillna('UNKNOWN')

    # --- 2. 标准化设备制造商 (Manufacturer) ---
    # 原始数据可能包含'SIEMENS', 'Philips Medical Systems', 'GE MEDICAL SYSTEMS'等不同写法。

    # 使用一个条件列表进行映射，np.select比多层if/else更清晰高效
    m_lower = df.get('Manufacturer', pd.Series(index=df.index)
                     ).astype(str).str.lower().fillna('')

    conditions = [
        m_lower.str.contains('siemens', na=False),
        m_lower.str.contains('philips', na=False),
        m_lower.str.contains('ge medical|ge healthcare', na=False),
        m_lower.str.contains('uih|united imaging', na=False),
        m_lower.str.contains('anke', na=False),
        m_lower.str.contains('canon', na=False),
        m_lower.str.contains('fujifilm', na=False),
        m_lower.str.contains('hitachi', na=False),
        m_lower.str.contains('mindray', na=False),
        m_lower.str.contains('shimadzu', na=False),
    ]

    choices = ['Siemens', 'Philips', 'GE', 'UIH', 'Anke',
               'Canon', "Fujifilm", "Hitachi", "Mindray", "Shimadzu"]

    df['standardManufacturer'] = np.select(
        conditions, choices, default='Other')

    # --- 3. 清理设备型号 (ManufacturerModelName) ---
    # 型号通常比较具体，我们主要做一些基础的清理，如转小写、去首尾空格。
    df['cleanedModelName'] = df.get('ManufacturerModelName', pd.Series(index=df.index))\
        .astype(str).str.lower().str.strip().fillna('unknown')

    if progress_callback:
        progress_callback("Done. Added columns: standardFieldStrength, standardManufacturer, cleanedModelName.", "extract_hardware_features_done")
    else:
        print("Done. Added columns: standardFieldStrength, standardManufacturer, cleanedModelName.")
    return df


def analyze_dynamic_series(df, cfg: dict, progress_callback=None):
    """
    分析时空关系以识别动态序列。

    功能：
    - 自动推断增强时相，不再依赖关键词标签。
    - 指纹中包含ImagePositionPatient和ImageOrientationPatient，确保空间位置精确匹配。
    - 稳健地处理空间标签的列表字符串格式及空值。
    """
    if progress_callback:
        progress_callback("Stage 4: analyzing dynamic series...", "analyze_dynamic_series")
    else:
        print("Stage 4: analyzing dynamic series...")
    df['dynamicGroup'] = np.nan
    df['dynamicPhase'] = ''
    dynamic_cfg = cfg.get('dynamic', {})
    exclude_from_dynamics = dynamic_cfg.get('exclude_sequence_classes', [])
    # --- 1. 稳健的数据类型和格式转换 ---

    # 转换数值型列
    numeric_cols = dynamic_cfg.get('numeric_cols', ['RepetitionTime', 'EchoTime', 'FlipAngle', 'SliceThickness', 'SeriesTime'])
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        else:
            if progress_callback:
                progress_callback(f"Warning: key column '{col}' is missing, some features may be limited.", "warning_missing_col")
            else:
                print(f"Warning: key column '{col}' is missing, some features may be limited.")

    # 定义一个函数来安全地解析和规范化列表格式的字符串
    def normalize_list_string(val, decimals=2):
        if pd.isnull(val):
            return 'NA'
        try:
            # 安全地将字符串 '[-125.0, -125.0, 80.0]' 转换为实际的列表
            list_val = ast.literal_eval(str(val))
            # 对列表中的每个数字四舍五入，以处理微小的浮点差异
            rounded_list = [round(float(v), decimals) for v in list_val]
            return str(rounded_list)
        except (ValueError, SyntaxError):
            # 如果值不是一个合法的列表字符串（例如，普通文本），则返回 'INVALID'
            return 'INVALID'

    # 为指纹创建规范化的空间特征列
    spatial_cols = dynamic_cfg.get('spatial_cols', ['ImagePositionPatient', 'ImageOrientationPatient'])
    list_round_decimals = int(dynamic_cfg.get('list_round_decimals', 2))
    for col in spatial_cols:
        if col in df.columns:
            df[f'{col}_str'] = df[col].apply(lambda v: normalize_list_string(v, decimals=list_round_decimals))
        else:
            if progress_callback:
                progress_callback(f"Warning: spatial column '{col}' is missing; fingerprint precision may be reduced.", "warning_missing_spatial_col")
            else:
                print(f"Warning: spatial column '{col}' is missing; fingerprint precision may be reduced.")
            df[f'{col}_str'] = 'NA'  # 如果列不存在，创建一个默认列

    # --- 2. 排除不适合进行动态分析的序列类型 ---
    # exclude_from_dynamics = ['DWI', 'DTI', 'ADC', 'FA', 'MRS', 'PWI', 'ASL', 'LOCALIZER']
    eligible_mask = ~df['sequenceClass'].isin(exclude_from_dynamics)
    df_eligible = df[eligible_mask].copy()

    # --- 3. 创建包含精确空间定位的“终极指纹” ---
    fingerprint_cols = dynamic_cfg.get(
        'fingerprint_cols',
        ['ImagePositionPatient_str', 'ImageOrientationPatient_str', 'sequenceClass', 'SliceThickness', 'RepetitionTime', 'EchoTime', 'FlipAngle'],
    )

    # 临时填充空值以确保它们能被包含在指纹中
    temp_fp_df = df_eligible[fingerprint_cols].copy()
    numeric_round_decimals = int(dynamic_cfg.get('numeric_round_decimals', 1))
    for col in fingerprint_cols:
        if temp_fp_df[col].dtype in ['float64', 'float32']:
            temp_fp_df[col] = temp_fp_df[col].round(numeric_round_decimals).fillna('NA')
        else:
            temp_fp_df[col] = temp_fp_df[col].fillna('NA')

    df_eligible['fingerprint'] = temp_fp_df.astype(str).agg('_'.join, axis=1)

    # --- 4. 按研究（Study）分组并识别动态集合 ---
    grouped = df_eligible.groupby('StudyInstanceUID')
    next_dynamic_group_id = 1

    for study_id, group in grouped:
        fingerprint_counts = group['fingerprint'].value_counts()
        dynamic_fingerprints = fingerprint_counts[fingerprint_counts > 1].index

        for fp in dynamic_fingerprints:
            dynamic_set_indices = group[group['fingerprint'] == fp].index

            # --- 5. 核心逻辑：基于时间排序来推断时相 ---
            dynamic_set_df = df.loc[dynamic_set_indices].copy()

            # 排序必须基于SeriesTime，否则无法进行时相判断
            if 'SeriesTime' not in dynamic_set_df.columns or dynamic_set_df['SeriesTime'].isnull().all():
                msg = f"Severe warning: Study {study_id} (fingerprint: {fp[:30]}...) dynamic set lacks valid 'SeriesTime', cannot determine phases."
                if progress_callback:
                    progress_callback(msg, "severe_warning_missing_series_time")
                else:
                    print(msg)
                continue  # 跳过这个无法处理的组

            # 按SeriesTime升序排列
            sorted_set = dynamic_set_df.sort_values(by='SeriesTime')

            # 分配动态组ID
            df.loc[dynamic_set_indices, 'dynamicGroup'] = next_dynamic_group_id

            # 第一个即为增强前，其余为增强后
            df.loc[sorted_set.index[0], 'dynamicPhase'] = 'PRE'
            for i, idx in enumerate(sorted_set.index[1:]):
                df.loc[idx, 'dynamicPhase'] = f'POST_{i+1}'

            next_dynamic_group_id += 1

    # 清理为指纹创建的临时列
    df.drop(columns=[f'{col}_str' for col in spatial_cols], inplace=True)
    contrast_regex = str(dynamic_cfg.get('contrast_protocol_regex', r'\+c|post|gd|enh|contrast|增强|dyn'))
    agent_exclude_regex = str(dynamic_cfg.get('contrast_agent_exclude_regex', 'no'))
    exclude_seq_regex = str(dynamic_cfg.get('exclude_sequence_regex', 'DWI|T2|LOCALIZER'))

    df['isContrastEnhanced'] = (
        (df['dynamicPhase'].str.startswith('POST', na=False)
         | df['protocolName_lower'].str.contains(contrast_regex, na=False, regex=True))
        & df['ContrastBolusAgent'].notna()
        & (~df['ContrastBolusAgent'].astype(str).str.contains(agent_exclude_regex, case=False, na=True, regex=True))
        & (~df['sequenceClass'].astype(str).str.contains(exclude_seq_regex, case=False, na=True, regex=True))
    )
    # done message already handled above
    return df


def propagate_enhancement_status(df, cfg: dict, progress_callback=None):
    """
    阶段五：传播增强状态以识别单次延迟增强序列。

    此函数在已识别出多时相动态组的基础上，进一步处理。
    它将识别出在已知增强扫描发生之后、且本身为T1加权的序列，
    并将它们也标记为增强序列。这解决了“单次、不同方位”的延迟期增强扫描的识别问题。

    Args:
        df (pd.DataFrame): 已执行过动态分析的DataFrame。

    Returns:
        pd.DataFrame: 更新了'dynamicPhase'和'isContrastEnhanced'列的DataFrame。
    """
    propagate_cfg = cfg.get('propagate', {})
    if progress_callback:
        progress_callback("Stage 5: propagating enhancement status to detect delayed single-phase enhancement...", "propagate_enhancement_status")
    else:
        print("Stage 5: propagating enhancement status to detect delayed single-phase enhancement...")

    # 在同一个Study内部进行状态传播
    grouped = df.groupby('StudyInstanceUID')

    for study_id, group in grouped:
        # 1. 寻找该Study中是否存在已确认的“增强后(POST)”序列
        post_contrast_series = group[group['isContrastEnhanced'] == True]

        if post_contrast_series.empty:
            # 如果没有增强序列，则无需进行任何操作，跳到下一个Study
            continue

        # 2. 确定该Study中“最晚的增强时间点”
        # 这是判断后续序列是否为延迟期的关键时间戳
        last_post_contrast_time = post_contrast_series['SeriesTime'].max()

        # 3. 筛选出需要被判断的“候选序列”
        # 候选序列必须满足以下条件：
        # - 尚未被分配任何时相 (即不是多期动态组的一员)
        # - 本身是T1加权序列 (增强扫描的基础)
        t1_contains = str(propagate_cfg.get('t1_contains', 'T1'))
        candidate_mask = (
            (group['dynamicPhase'] == '')
            & (group['sequenceClass'].astype(str).str.contains(t1_contains, na=False))
        )
        candidate_indices = group[candidate_mask].index

        if candidate_indices.empty:
            continue

        # 4. 应用传播规则
        for idx in candidate_indices:
            candidate_time = df.loc[idx, 'SeriesTime']

            # 如果一个T1序列的扫描时间晚于已知的最晚增强时间，
            # 则将其标记为传播而来的增强序列。
            if pd.notnull(candidate_time) and candidate_time > last_post_contrast_time:
                # 在主DataFrame上更新状态
                df.loc[idx, 'dynamicPhase'] = str(propagate_cfg.get('propagated_phase', 'POST_PROPAGATED'))
                df.loc[idx, 'isContrastEnhanced'] = True

    if progress_callback:
        progress_callback("Done. 'dynamicPhase' and 'isContrastEnhanced' columns updated.", "propagate_enhancement_status_done")
    else:
        print("Done. 'dynamicPhase' and 'isContrastEnhanced' columns updated.")
    return df
# ==============================================================================
# 主流程封装 (Main Workflow)
# ==============================================================================


def process_mri_dataframe(df, cfg: dict | None = None, config_path: str | None = None, progress_callback=None):
    """
    对包含MRI序列信息的DataFrame执行完整的分类流程。
    """
    cfg = _get_cfg(cfg, config_path)
    df_copy = df.copy()

    # Stage 1: atomic features
    df_featured = extract_atomic_features(df_copy, cfg, progress_callback=progress_callback)

    # Stage 2: hardware features
    df_hardware_featured = extract_hardware_features(df_featured, progress_callback=progress_callback)

    # Stage 3: classification
    if progress_callback:
        progress_callback("Stage 3: classifying sequences...", "classify_sequence")
    else:
        print("Stage 3: classifying sequences...")
    df_classified = df_hardware_featured.apply(lambda r: classify_sequence(r, cfg), axis=1)
    df_hardware_featured['sequenceClass'] = df_classified

    # Stage 4: dynamic contrast analysis
    df_dynamic = analyze_dynamic_series(df_hardware_featured, cfg, progress_callback=progress_callback)
    df_dynamic['isContrastEnhanced'] = (
        df_dynamic['protocolName_lower'].str.startswith('t1', na=False)
        & df_dynamic['ContrastBolusAgent'].notna()
        & (~df_dynamic['ContrastBolusAgent'].str.contains('no', case=False, na=True))
    )
    # Stage 5: propagate enhancement status
    df_final = propagate_enhancement_status(df_dynamic, cfg, progress_callback=progress_callback)

    if progress_callback:
        progress_callback("All processing steps completed. Saving results...", "process_complete")
    else:
        print("\n>>> All processing steps completed! Saving results... <<<")
    return df_final


if __name__ == '__main__':
    # ================== 使用示例 ==================
    # 假设你有一个名为 'mri_data.csv' 的文件
    # df_raw = pd.read_csv('mri_data.csv')


    startTime = time()
    df_raw = pd.read_excel('combined_data.xlsx')
    # 执行完整的处理流程
    df_classified = process_mri_dataframe(df_raw)

    df_classified.to_excel('classified_data.xlsx', index=False)
    print("消耗时间%.1f秒" % (time()-startTime))
