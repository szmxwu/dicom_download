# MR 语义复查：3 处可复现问题

日期：2026-11-03。复查基线：main `a0ccd41da2372fc3830d15b8577b4939713f9bf7`，
重点对比原 PR 提交 `0a535cb` 与 MR 加固提交 `9a65c85`。

本文件只提交问题、复现证据和验收边界，交由本地 agent 结合真实数据修复。
没有修改业务代码、配置或测试，没有部署生产服务。以下结果来自合成元数据和临时目录；
未独立重跑文档记载的真实 MR 队列审计，不能据此推算真实发生率。

## 1. [P2] 增强传播后的兼容字段与 V2 给药状态不一致

位置：[mr_clean.py:830](https://github.com/szmxwu/dicom_download/blob/a0ccd41da2372fc3830d15b8577b4939713f9bf7/src/core/mr_clean.py#L830)，
以及随后生成 V2 时调用的 `contrast_attributes()`。

触发条件：显式开启 `propagate.enabled=true`。默认关闭时不触发。
同一检查先有明确 POST 的 T1 序列，后有时间更晚、自身给药状态尚未知的 T1 序列。

| 后续序列条件 | 当前兼容字段 | 当前 V2 属性 |
| --- | --- | --- |
| 自定义 propagated_phase=CUSTOM_DELAYED，对比剂字段为空 | dynamicPhase=CUSTOM_DELAYED；contrast_administration_state=CUSTOM_DELAYED；isContrastEnhanced=True | contrast_administration_state=UNKNOWN，status=unknown，evidence=[] |
| 默认 POST_PROPAGATED，ContrastBolusAgent="no" | dynamicPhase=POST_PROPAGATED；contrast_administration_state=POST_PROPAGATED；isContrastEnhanced=True | contrast_administration_state=CONFLICT，status=conflict |

本次加固为保留自定义阶段而取消最终状态重算，但 V2 仍从字段重新判定；
自定义字符串没有被解释为 POST，显式否定对比剂又会与传播结果冲突。
原 PR 在第二种输入下为 CONFLICT、增强=False；这是可对照确认的回归。
新增的 `test_custom_propagated_phase_is_not_overwritten` 只断言兼容字段，未核对 V2。

交给本地 agent 的验收边界：

- 给药状态使用稳定的 PRE/POST/UNKNOWN/CONFLICT 等规范枚举；自定义阶段单独表达。
- 传播结果必须进入 V2，保留来源序列、时间依据及推断规则，而不是丢成无证据 UNKNOWN。
- 明确冲突不能仍产生可用的 C+ 兼容标记；补上兼容字段、V2 JSON、V2 表之间的一致性验证。
- 用真实延迟增强、明确 PRE/POST 及否定对比剂记录核验，不能仅靠合成样本决定自动接受策略。

## 2. [P2] 派生图旧标记与 V2 训练属性采用不同语义，监督掩码仍开放

位置：[mr_clean.py:323](https://github.com/szmxwu/dicom_download/blob/a0ccd41da2372fc3830d15b8577b4939713f9bf7/src/core/mr_clean.py#L323)，
[mr_semantics.py:307](https://github.com/szmxwu/dicom_download/blob/a0ccd41da2372fc3830d15b8577b4939713f9bf7/src/core/mr_semantics.py#L307)。

输入：`ImageType=ORIGINAL\\PRIMARY`，`SeriesDescription="motion curve"`。
与新增 `test_derived_boolean_matches_refined_type` 使用同类输入。

| 字段 | 当前输出 |
| --- | --- |
| refinedImageType | DERIVED |
| isDerivedImage | True |
| V2 attributes.is_derived.value | False |
| V2 attributes.is_derived.status | known |
| V2 supervision_mask.is_derived | True |

旧视图按名称推断非诊断/派生用途，V2 按原始 ImageType 判断图像来源。
二者可能本来就是不同概念，不能仅凭这个合成输入断言真实图像必然派生或必然原始。
问题是目前用近似同义的属性名表达相反结论，且没有解释差异或阻止误用训练掩码。
旧的 refinedImageType 与 V2 已有此差异；本次令 isDerivedImage 跟随 refinedImageType，
没有解决跨视图定义问题。因此不能将其全部归因于本次提交。

交给本地 agent 的验收边界：

- 用真实 motion curve/report/efilm 及 ORIGINAL/SECONDARY 图像确认实际使用语义。
- 明确区分 DICOM 标注的来源、规则推断的用途和任务筛选资格；如果保留不同定义，名称与文档必须清楚。
- 若声称是同一属性，则应合并证据、记录冲突并相应控制监督掩码，不能一边 True、一边 known False 自动接受。
- 新增跨旧视图、V2 属性与监督掩码的断言。“isDerivedImage 与 refinedImageType 不一致为零”
  现在由赋值关系保证，不能单独作为独立正确性证据。

## 3. [P2] 部分目录重命名失败，三张表仍更新为未创建的新目录

位置：[unified.py:2159](https://github.com/szmxwu/dicom_download/blob/a0ccd41da2372fc3830d15b8577b4939713f9bf7/src/client/unified.py#L2159)。

复现：两个 MR 目录 a、b；a 移动成功，b 因模拟的 PermissionError 失败。
实际目录是 `001_T1_GRE_AX_3D` 与 `b`，但返回映射包含两项，且：

| 表 | b 对应的 SeriesFolder | 目录存在 |
| --- | --- | --- |
| DICOM_Metadata | 002_T1_GRE_AX_3D | False |
| MR_Cleaned | 002_T1_GRE_AX_3D | False |
| MR_Cleaned_v2 | 002_T1_GRE_AX_3D | False |

原因：`renamed_count > 0` 后使用完整计划 rename_map 更新所有记录，没有过滤失败项。
这是原始表更新中已有的问题，本次三表同步扩大了受影响的输出；不是本次首次引入。
新增测试只覆盖单目录成功路径，未覆盖混合成功/失败。

交给本地 agent 的验收边界：

- 只用实际成功的映射更新表格及返回结果；失败目录继续保留可访问的旧路径并记录失败。
- 验证单个成功、全部失败、部分失败、目标已存在、源目录不存在等情况。
- 在科研副本上模拟真实文件锁/目录冲突，并逐表检查 SeriesFolder 与实际索引目录可达性。

## 一次性最小复现

在上述基线的仓库根目录、已安装项目及 pytest 等测试依赖的环境运行。
代码仅处理合成元数据，在 TemporaryDirectory 中演示重命名，退出后自动清理。
输出用于观察当前问题，不是修复后应继续保留的验收断言。

```bash
python - <<'PY'
import contextlib
import io
import json
import shutil
import tempfile
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from src.client.unified import DICOMDownloadClient
from src.core.mr_clean import load_mr_clean_config, process_mri_dataframe


def row(**changes):
    values = dict(
        ProtocolName='t1_vibe', SeriesDescription='t1_vibe',
        ImageType=['ORIGINAL', 'PRIMARY'], ContrastBolusAgent='',
        MRAcquisitionType='3D', ScanningSequence='GR',
        RepetitionTime=5, EchoTime=2, FlipAngle=12, SliceThickness=1,
        MagneticFieldStrength=3, StudyInstanceUID='1.2.3',
        SeriesInstanceUID='1.2.3.4', FrameOfReferenceUID='1.2.5',
        ImageOrientationPatient=[1, 0, 0, 0, 1, 0],
        ImagePositionPatient=[0, 0, 0],
        AcquisitionDate='20261030', AcquisitionTime='120000',
    )
    values.update(changes)
    return values


def process(rows, cfg=None):
    with contextlib.redirect_stdout(io.StringIO()):
        return process_mri_dataframe(pd.DataFrame(rows), cfg)


# 问题 1：两种传播输出与 V2 对照。
cfg = load_mr_clean_config()
cfg['propagate']['enabled'] = True
for phase, agent in [('CUSTOM_DELAYED', ''), ('POST_PROPAGATED', 'no')]:
    cfg['propagate']['propagated_phase'] = phase
    result = process([
        row(dynamicPhase='POST'),
        row(SeriesInstanceUID='1.2.3.9', dynamicPhase='UNKNOWN',
            AcquisitionTime='120100', ContrastBolusAgent=agent),
    ], cfg).iloc[1]
    state = json.loads(result.mr_semantics_v2)['attributes']['contrast_administration_state']
    print('ISSUE_1', json.dumps(dict(
        configured_phase=phase, agent=agent,
        legacy_phase=result.dynamicPhase,
        legacy_state=result.contrast_administration_state,
        enhanced=bool(result.isContrastEnhanced),
        v2_value=state['value'], v2_status=state['status'],
        v2_evidence=state['evidence'],
    ), ensure_ascii=False))

# 问题 2：派生图与监督掩码对照。
result = process([row(SeriesDescription='motion curve')]).iloc[0]
v2 = json.loads(result.mr_semantics_v2)
print('ISSUE_2', dict(
    refined=result.refinedImageType,
    legacy_is_derived=bool(result.isDerivedImage),
    v2_is_derived=v2['attributes']['is_derived']['value'],
    v2_status=v2['attributes']['is_derived']['status'],
    supervision_mask=v2['supervision_mask']['is_derived'],
))

# 问题 3：a 成功、b 被模拟文件锁阻止；逐表检查真实目录。
with tempfile.TemporaryDirectory() as temp:
    root = Path(temp)
    organized = root / 'organized'
    (organized / 'a').mkdir(parents=True)
    (organized / 'b').mkdir()
    excel = root / 'metadata.xlsx'
    pd.DataFrame([
        row(Modality='MR', SeriesFolder='a', SeriesNumber=1),
        row(Modality='MR', SeriesFolder='b', SeriesNumber=2,
            SeriesInstanceUID='1.2.3.9'),
    ]).to_excel(excel, sheet_name='DICOM_Metadata', index=False)
    client = DICOMDownloadClient.__new__(DICOMDownloadClient)
    client.progress_callback = None
    original_move = shutil.move

    def fail_b(src, dst):
        if Path(src).name == 'b':
            raise PermissionError('synthetic file lock')
        return original_move(src, dst)

    with patch('shutil.move', side_effect=fail_b), contextlib.redirect_stdout(io.StringIO()):
        mapping = client.rename_mr_series_folders(str(organized), str(excel))
    print('ISSUE_3_MAP', mapping)
    print('ISSUE_3_DISK', sorted(p.name for p in organized.iterdir()))
    for sheet in ('DICOM_Metadata', 'MR_Cleaned', 'MR_Cleaned_v2'):
        folders = pd.read_excel(excel, sheet_name=sheet).SeriesFolder.tolist()
        print('ISSUE_3_SHEET', sheet,
              [(folder, (organized / folder).is_dir()) for folder in folders])
PY
```

复查时已有测试结果：MR 专项 68 passed；完整 `test` 目录（排除需外部真实源数据的
`verify_ct_geometry_v4_real_source.py`）307 passed / 3 skipped。
这些通过结果与上述问题同时存在；本文件没有声称已经修复它们。
