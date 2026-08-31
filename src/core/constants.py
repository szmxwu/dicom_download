# -*- coding: utf-8 -*-
"""
核心常量定义模块

集中管理跨模块共享的常量，避免重复定义。
"""

# 衍生序列关键词列表（默认值）
# 用于在 PACS 查询阶段和整理阶段过滤 MPR/MIP/3D 重建等衍生序列。
#
# 维护原则（2026-08 厂商命名调研后确立）：
# - 各厂商迭代/深度学习重建（Philips iDose/IMR、GE ASIR(-V)/MBIR/TrueFidelity、
#   Siemens SAFIRE/ADMIRE、Canon AIDR 3D/FIRST/AiCE、联影 KARL/DELTA、
#   东软 ClearView/ClearInfinity、富士 IPV、赛诺威盛 iDream）均为 ORIGINAL
#   诊断序列，不得出现在本列表中；其命名与下列关键词的子串碰撞通过
#   DERIVED_KEYWORD_EXCEPTIONS 例外表解决。
# - 短子串关键词（REF/LOC/KEY/SUM/VR 等）有误伤面，新增关键词前先排查
#   是否会命中真实诊断序列命名。
# - 'nodule' 曾在此列表中但为小写（匹配时 desc 会 upper 化，关键词不会），
#   属于从未生效的死代码；且会误伤 "Lung Nodule FU" 等肺结节随访诊断协议。
#   uAI_nodule_feature 等 AI 输出由 'AI_' 覆盖，故裸 'nodule' 已移除。
#
# 本院真实数据实证（2026-08）：
# - 联影 uAI 肺结节 AI 输出：SeriesDescription="uAI_nodule_feature"，
#   ImageType=DERIVED\SECONDARY\OTHER\UAI，Modality=CT，SOPClass=Secondary Capture，
#   设备 uCT 760 —— ImageType 过滤与 'AI_' 关键词均可命中。
# - 联影 MR 定位像：SeriesDescription 关键词为 "scout" —— 由 'SCOUT' 覆盖。
DEFAULT_DERIVED_SERIES_KEYWORDS = [
    'MPR', 'MIP', 'MINIP', 'SSD', 'VRT', 'VR',
    'CPR', 'CURVED', 'THICK',
    'SCOUT', 'TOPOGRAM', 'SURVEY', 'SURVIEW', 'SCANOGRAM',
    'PILOT', 'LOCALIZER', '定位像',
    'REF', 'REFERENCE', 'LOC', 'BATCH',
    'AVERAGE', 'SUM', 'REFORMAT',
    'PROJECTION', 'RAYSUM', 'KEY', 'ROI', 'DOSE',
    'TRACKER', 'AI_',
    # 以下为有实证的新增附属序列命名（2026-08 调研）：
    'SCREEN SAVE', 'SCREENSAVE',   # 剂量/报告截屏（Secondary Capture）
    'RDSR',                        # Radiation Dose Structured Report
    'SMARTPREP', 'SMART PREP',     # GE 团注追踪监测序列
    'BOLUS TRACK',                 # 团注监测序列（注意不过滤裸 'BOLUS'，避免误伤 DCE 命名）
    'MPR COLLECTION', 'CASCORING', # Siemens 派生重建输出
]

# 当 ImageType[0] == 'DERIVED' 时，部分临床有意义的序列不应被过滤。
# 白名单用于保留 ADC/Dixon 等后处理序列。
# 匹配范围：ImageType 其余值（厂商语义标记，如 Philips 拼合图像的 SPINE）
# 以及 SeriesDescription（子串、不区分大小写）。
DERIVED_IMAGE_TYPE_WHITELIST = [
    'ADC', 'EADC', 'WATER', 'FAT',
    'IN PHASE', 'INPHASE', 'OUT PHASE', 'OUTPHASE',
    'IDEAL', 'T1 MAP', 'T2 MAP', 'FA MAP',
    'DWI', 'DIFF', 'DIFFUSION', 'EP2D', 'EPI',  # 保留 DWI/EPI 序列（即使 ImageType=DERIVED）
    # 全脊柱/全下肢拼合图像（stitching）：技术上属 DERIVED，
    # 但对 DR 全脊柱/全长下肢检查是不可缺少的诊断图像。
    # 实证：Philips DR 拼合 ImageType=DERIVED\SECONDARY\SPINE\VIEWER VB15A，
    # SeriesDescription 如 "W LongSpine ap large"（520010500096DR 生产事故）
    'SPINE', 'STITCH', 'LONGLEG', 'LONG LEG',
]

# 2D X 光模态集合：这些模态的"诊断用"图像经常就是 ImageType=DERIVED
# （DR 探测器输出的 for-presentation 处理图像常规为 DERIVED\PRIMARY/SECONDARY，
# DSA 减影序列为 DERIVED\PRIMARY，MG processed view 同理）。
# 实证（2026-08-29，136 生产事故）：Mindray DR 普通胸片/四肢片
# （V04_0015、"W033 Thorax p.a." 等唯一诊断序列）ImageType[0]=DERIVED，
# 被接收端过滤整体误杀 → 单序列检查 0 文件落盘 → 任务失败率 85%。
# 对这些模态，ImageType=DERIVED 单独不足以判衍生，仍需 SeriesDescription
# 命中关键词（剂量报告 RDSR/Screen Save 等仍会被关键词过滤）。
XRAY_DERIVED_TOLERANT_MODALITIES = {
    'DR', 'DX', 'CR', 'XR', 'RF', 'XA',          # 普放/透视/造影（DSA 减影是 DERIVED 诊断图）
    'MG', 'MAMMO', 'MAMMOGRAPHY', 'BCT',         # 钼靶 processed view 同理
}

# 运行时可修改的过滤关键词（模块级可变状态）
# 通过 get_derived_keywords() / set_derived_keywords() 访问
_runtime_derived_keywords = list(DEFAULT_DERIVED_SERIES_KEYWORDS)

# 关键词匹配的例外表：当 SeriesDescription 同时包含例外子串时，该关键词不判为衍生。
# 典型场景：Philips iCT 的 iDose 迭代重建序列是诊断序列（ORIGINAL 重建），
# 但 SeriesDescription 含 "iDose"（如 "201 Chest iDose"），
# 会被 'DOSE' 关键词误判为剂量报告（Dose Report）衍生序列而被整体剔除。
DERIVED_KEYWORD_EXCEPTIONS = {
    # DOSE 的本意是过滤剂量报告（Dose Report / Dose Info / Screen Save）。
    # 以下例外均为诊断序列或设备技术品牌名：
    'DOSE': [
        'IDOSE',                 # Philips iDose 迭代重建（已发生生产事故）
        'IDREAM',                # 赛诺威盛 iDream 迭代重建（全称含 Dose Reduction）
        'DOSE REDUCTION',        # 迭代重建全称写法（Adaptive Iterative Dose Reduction 等）
        'AIDR',                  # Canon AIDR (Adaptive Iterative Dose Reduction) 若写全称
        'DOSERIGHT',             # Philips DoseRight AEC 技术名
        'DOSE4D',                # Siemens CARE Dose4D 管电流调制技术名
        'SMARTDOSE',             # GE SmartDose 技术名
        'LOW DOSE', 'LOW-DOSE', 'LOWDOSE',  # 低剂量肺筛诊断序列（如 "Low Dose Chest"）
    ],
    # THICK 的本意是过滤厚层 MPR 重建，但厚块 MRCP（thick-slab）是核心诊断序列
    'THICK': ['MRCP'],
    # LOC 的本意是过滤定位像（Localizer），但会命中 veLOCity（Philips QFlow 等
    # 相位对比血流定量诊断序列）
    'LOC': ['VELOCITY'],
    # KEY 的本意是过滤 Key Images（关键图像标记），但会命中 keyhole 采集技术
    # （4D 动态增强 MRA，如 Philips 4D-TRAK keyhole）
    'KEY': ['KEYHOLE'],
}


def match_derived_keyword(series_desc, keywords=None):
    """返回 SeriesDescription 命中的衍生序列关键词（含例外规则），未命中返回 None。

    匹配为子串包含（不区分大小写，desc 与关键词两侧都会归一化）。
    命中关键词但同时包含 DERIVED_KEYWORD_EXCEPTIONS 中登记的例外子串时，
    视为未命中。

    Args:
        series_desc: SeriesDescription 字符串
        keywords: 可选，本次生效的关键词列表；None 时使用全局 get_derived_keywords()
    """
    if not series_desc:
        return None
    desc_upper = series_desc.upper()
    for keyword in (keywords if keywords is not None else get_derived_keywords()):
        kw_upper = keyword.upper()  # 关键词侧也归一化（历史上混入过小写关键词导致死代码）
        if kw_upper in desc_upper:
            exceptions = DERIVED_KEYWORD_EXCEPTIONS.get(kw_upper, ())
            if any(exc in desc_upper for exc in exceptions):
                continue
            return kw_upper
    return None


def is_derived_series(series_desc, image_type, keywords=None, modality=None):
    """统一的衍生序列判定：ImageType 首值为 DERIVED，或 SeriesDescription 命中关键词。

    查询阶段（C-FIND identifier）与接收阶段（C-STORE 首文件 dataset）共用同一套
    规则，避免两处逻辑漂移。

    ImageType[0] == 'DERIVED' 时会继续检查 DERIVED_IMAGE_TYPE_WHITELIST
    （ImageType 其余值 + SeriesDescription），命中白名单的序列（ADC/Dixon/
    全脊柱拼合等）保留。

    modality 为 2D X 光模态（XRAY_DERIVED_TOLERANT_MODALITIES）时，
    ImageType=DERIVED 单独不足以判衍生（DR for-presentation 图像常规即 DERIVED），
    继续走关键词匹配。

    Args:
        series_desc: SeriesDescription 字符串（可为 None/空）
        image_type: ImageType 值（pydicom MultiValue / list / str / None）
        keywords: 可选，本次生效的关键词列表（每个任务可覆盖）；None 时用全局配置
        modality: 可选，模态代码（DR/CT/MR...）；用于 X 光模态的 DERIVED 宽容

    Returns:
        (is_derived: bool, reason: str)；未判为衍生时 reason 为空串。
    """
    xray_tolerant = bool(modality) and str(modality).upper().strip() in XRAY_DERIVED_TOLERANT_MODALITIES
    if image_type:
        # ImageType 第一个值才代表像素来源：DERIVED/ORIGINAL。
        # 第二个值 PRIMARY/SECONDARY 是采集上下文，不能用于过滤。
        if isinstance(image_type, (list, tuple)):
            first_val = str(image_type[0]).upper().strip() if image_type else ''
            if first_val == 'DERIVED':
                # 白名单：ImageType 其余值是厂商语义标记（如拼合图像的 SPINE）
                for extra_val in image_type[1:]:
                    val_upper = str(extra_val).upper()
                    for kw in DERIVED_IMAGE_TYPE_WHITELIST:
                        if kw in val_upper:
                            return False, ''
                if series_desc:
                    desc_upper = series_desc.upper()
                    for kw in DERIVED_IMAGE_TYPE_WHITELIST:
                        if kw in desc_upper:
                            return False, ''
                if not xray_tolerant:
                    return True, 'ImageType=DERIVED'
        elif 'DERIVED' in str(image_type).upper():
            if series_desc:
                desc_upper = series_desc.upper()
                for kw in DERIVED_IMAGE_TYPE_WHITELIST:
                    if kw in desc_upper:
                        return False, ''
            if not xray_tolerant:
                return True, 'ImageType=DERIVED'
    matched_kw = match_derived_keyword(series_desc, keywords=keywords)
    if matched_kw:
        return True, f"keyword '{matched_kw}'"
    return False, ''


def get_derived_keywords():
    """获取当前生效的衍生序列过滤关键词列表。"""
    return _runtime_derived_keywords


def get_filter_rules_fingerprint():
    """过滤规则指纹（短哈希），随规则内容自动变化。

    覆盖：当前生效关键词 + DERIVED_KEYWORD_EXCEPTIONS 例外表 +
    DERIVED_IMAGE_TYPE_WHITELIST 白名单。用于缓存键（cache.py），
    任何一处规则修改都会使旧缓存条目失配——避免旧过滤逻辑
    （如 iDose 误过滤、无全脊柱白名单）产生的结果被新代码命中。
    """
    import hashlib
    import json
    payload = json.dumps({
        'kw': sorted(k.upper() for k in get_derived_keywords()),
        'ex': sorted((k, tuple(v)) for k, v in DERIVED_KEYWORD_EXCEPTIONS.items()),
        'wl': sorted(DERIVED_IMAGE_TYPE_WHITELIST),
        # 规则行为版本：X 光模态 DERIVED 宽容（2026-08-29 引入，此前 X 光
        # DERIVED 诊断图会被误杀，旧缓存条目结果不完整，必须失配）
        'xray_derived_tolerant': True,
    }, sort_keys=True, ensure_ascii=False)
    return hashlib.md5(payload.encode('utf-8')).hexdigest()[:8]


def set_derived_keywords(keywords):
    """设置衍生序列过滤关键词列表（立即生效）。

    Args:
        keywords: 关键词字符串列表
    """
    global _runtime_derived_keywords
    validated = []
    for k in keywords:
        if isinstance(k, str) and k.strip():
            validated.append(k.strip().upper())
    # 去重并保持顺序
    seen = set()
    unique = []
    for k in validated:
        if k not in seen:
            seen.add(k)
            unique.append(k)
    _runtime_derived_keywords = unique


def reset_derived_keywords():
    """重置过滤关键词为系统默认值。"""
    global _runtime_derived_keywords
    _runtime_derived_keywords = list(DEFAULT_DERIVED_SERIES_KEYWORDS)


# 向后兼容：DERIVED_SERIES_KEYWORDS 指向当前运行时列表
DERIVED_SERIES_KEYWORDS = _runtime_derived_keywords


# ========== 模态同义词映射（解决 PACS 命名不规范问题）==========
#
# PACS 中同一模态可能存在多种写法，例如 2D X-ray 可能标记为
# DR、DX、CR、XR 等。建立同义词组后，当用户指定一个标准模态
# 时，系统会自动展开为组内所有别名进行匹配。
#
# 用法示例：
#   用户输入 modality_filter="DR" → 实际匹配 {DR, DX, CR, XR}
#   用户输入 modality_filter="CT" → 实际匹配 {CT, PETCT, CTPET}
#
MODALITY_SYNONYMS = {
    'DR': {'DR', 'DX', 'CR', 'XR', 'RF'},           # 2D X-ray / 普放
    'CT': {'CT', 'PETCT', 'CTPET', 'PET-CT'},       # CT / PET-CT
    'MR': {'MR', 'MRI', 'MRPT'},                    # 磁共振
    'MG': {'MG', 'MAMMO', 'MAMMOGRAPHY', 'BCT'},    # 乳腺钼靶
    'PT': {'PT', 'PET', 'PETCT', 'CTPET'},          # PET
    'NM': {'NM', 'NUC', 'NUCLEAR'},                 # 核医学
    'US': {'US', 'ULT', 'ULTRASOUND'},              # 超声
    'XR': {'XR', 'XRAY', 'DX', 'CR', 'DR', 'RF'},   # X-ray 通用别名
}


def expand_modality_filter(modality_filter: str) -> list:
    """展开模态过滤字符串，自动包含同义词组内的所有别名。

    例如输入 "MR,DR" 会返回 ['MR','MRI','MRPT','DR','DX','CR','XR','RF']。
    输入 "DX" 也会自动展开为 DR 同义词组（因为 DX 是 DR 组的成员）。
    如果某个输入模态不在任何同义词组中，则保留原值进行精确匹配。

    Args:
        modality_filter: 逗号分隔的模态字符串，如 "MR,CT"

    Returns:
        去重后的大写模态列表
    """
    if not modality_filter:
        return []
    raw = [m.strip().upper() for m in modality_filter.split(',') if m.strip()]
    expanded = set()
    for m in raw:
        # 1) 直接命中同义词组的键
        if m in MODALITY_SYNONYMS:
            expanded.update(MODALITY_SYNONYMS[m])
            continue
        # 2) 命中某个同义词组的成员（如输入 DX → 展开为 DR 组）
        found_group = False
        for group_name, synonyms in MODALITY_SYNONYMS.items():
            if m in synonyms:
                expanded.update(synonyms)
                found_group = True
                break
        if not found_group:
            # 3) 不在任何已知组中，保留原值精确匹配
            expanded.add(m)
    return sorted(list(expanded))
