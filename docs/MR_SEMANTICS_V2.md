# MR 成像语义 V2：输出契约与验证边界

日期：2026-10-31。基础版本：main `062ff9e`。依据：源码路径及
`test/test_mr_semantics.py` 的合成反例与导出集成测试。尚无真实 MR 队列准确率证据。

## 已实现的行为

保留 `process_mri_dataframe()`、`sequenceClass`、`sequenceFamily` 与标准化命名接口，
修复错误标签，并在 Excel 中并行增加 `MR_Cleaned_v2`。没有部署或重启生产服务。

| 旧问题 | 当前行为 |
| --- | --- |
| 空对比剂字符串通过 notna() | 空白、NaN、None、null 等统一处理；缺失是 UNKNOWN |
| T1 + 对比剂名称强制增强 | 删除覆盖路径；旧配置中的 enabled=true 也不会恢复此逻辑 |
| PRE 被 POST 静默覆盖 | 明确矛盾为 CONFLICT；兼容增强布尔值为 False |
| noncontrast/precontrast/dyn 命中增强 | PRE/否定先处理；dyn 本身不提供给药证据 |
| Dixon 四种输出全被判压脂 | 独立识别水像、脂像、同相位、反相位；Dixon 名称本身不判压脂 |
| fatsat 被解释为脂像 | 按完整 token 匹配；历史 `_FAT` 不进行盲目批量迁移 |
| ImageType 多值格式不同产生不同结果 | 原生 MultiValue、列表、JSON/Python 列表字符串、反斜杠统一解析 |
| 名称 water 先于结构化 FAT 命中 | 分别收集证据；冲突不产生亚型后缀 |
| 3T TI=280 的 STIR 与压脂不一致 | 共享场强阈值；物理 STIR/FLAIR 推断需要 IR 准备证据 |
| 重复序列自动首期 PRE、后续 POST | 只形成候选时间组；给药状态独立判定 |
| 空 SeriesDescription 被直接排除 | 默认关闭这条交付过滤；其他可用字段仍可分类 |
| SECONDARY 与 DERIVED 合并 | 二者各有独立原子属性；SECONDARY 本身不等于 DERIVED |

名义压脂兼容布尔值不表示图像中压脂成功；`isContrastEnhanced` 兼容布尔值
仅表示存在未冲突的 POST 给药证据，不证明组织确实强化。
UNKNOWN/CONFLICT 均映射为 False，训练必须读取 V2 状态，不能把这个 False 当阴性真值。

## V2 契约

每个属性均提供 `value / status / evidence / rule_id / rule_version`。
status 为 `known / inferred / unknown / conflict`，不输出未经校准的概率。
证据包含使用的字段、原值及来源路径；配置 SHA256 与语义版本一起输出。

主要属性：

- `contrast_weighting`、`pulse_family`、`epi_readout`、`steady_state`、`inversion_recovery`。
- `fat_water_separation`、`water_fat_component`、`fat_suppression_method`、
  `nominal_fat_signal_removed`；真实 `fat_suppression_effect_observed` 保持 UNKNOWN。
- `complex_signal_component` 与水脂反相位分开。
- `contrast_agent_recorded`、`contrast_administration_state`；
  `enhancement_observed` 保持 UNKNOWN，不能由对比剂名称推出。
- `image_role`、`is_derived`、`is_secondary`、方向向量、法向量、倾斜角。
- `eligible_for_diagnostic_training` 和 `eligible_for_anatomical_localization` 独立于分类。
  定位像诊断训练资格为 False，解剖定位资格为 UNKNOWN，等待验证。

结构化 AcquisitionContrast、EchoPulseSequence 等存在且一致时优先使用；
特殊用途规则仍保留。V2 不把旧 TR/TE/TI 规则的输出自动升级为可靠物理标签。
不能确认加权时，可以保留已确认的脉冲家族或 IR 属性。

`process_mri_dataframe()` 的 `mr_semantics_v2` 列是完整 JSON；
`build_v2_dataframe()` 将其展开为值、状态列和 `attribute_evidence_json`。
Excel 的旧表不写入完整 JSON，新表包含它及 `supervision_mask_json`。

监督掩码仅对 known 属性为 True；inferred 默认不自动接受。
这是元数据证据等级策略，不是经人工测试集校准的准确率保证。
掩码只对 `mr_metadata_scope` 标明的输入范围有效：
`representative_frame` 不能自动监督整个 DICOM Series 或任意转换体积。
如果已发现系列异质性，或启用的索引不完整/失败，整条系列摘要的掩码关闭；
必须回到实例/帧或经过核验的体积对应关系重新生成标签。

## 检查内时间关系

保留兼容 `dynamicGroup`，新增 `temporal_group / temporal_index / temporal_relation_status / temporal_evidence`。

- 不同非空 Study、Series、FrameOfReferenceUID 不会合并。
- 必须有有效的单位方向向量、代表位置、层厚、TR、TE、翻转角；缺失值不能通过 NA 自动匹配。
- 优先使用 FrameAcquisitionDateTime、AcquisitionDateTime、采集日期+时间，最后回退 SeriesDate/SeriesTime。
- 不混用时间戳与纯时刻；时间相同、缺失、精度不足时不编造 temporal_index。
- 缺少 FrameOfReferenceUID 时仅保留低证据候选，证据中显式记录缺失。
- 代表位置匹配不证明覆盖范围一致或配准成功，二者均标为 unverified。
- 延迟增强传播默认关闭。配置 `propagate.enabled=true` 时，仅对同检查、完整可比日期时间、
  晚于明确 POST、且自身给药状态 UNKNOWN 的 T1 候选推断；明确 PRE/CONFLICT 保持原状态。

## MR 帧/体积索引

默认只读取代表帧的结构化元数据，标记 `mr_metadata_scope=representative_frame`。
标准 DiffusionBValue 显式映射到兼容 `b_value`。增强型 MR 按
Per-frame > Shared Functional Groups > 顶层顺序读取，并保留来源路径。
旧缓存中没有这些信息时不猜补；缺少标准扩散标签的厂商私有格式尚未适配。

开启研究验证用的完整头部索引：

```bash
export MR_FRAME_INDEX_ENABLED=1
```

在转换前元数据缓存路径和直接提取路径中，逐文件读取 header（`stop_before_pixels=True`），
输出每个序列目录的 `mr_frame_index.json`。默认关闭，避免对生产逐序列增加全量头部 I/O。
Windows 可用 `$env:MR_FRAME_INDEX_ENABLED='1'`，重启服务后生效。

索引保留：echo_times、b_values、时间位置、扩散方向、水脂/复数图像成分，
以及每个候选体积的源文件、SOPInstanceUID、1-based 帧号、位置及来源。
摘要写入元数据缓存，因此源 DICOM 删除后仍能导出到 Excel。
完整成员关系放在 JSON 侧文件，不塞入可能被 Excel 截断的单元格。

`candidate_volumes` 是按可见属性分组的候选集合。
缺少时间位置/扩散方向时，不能保证拆开所有重复体积；不声称已恢复真实的转换器输出顺序。
`output_volume_mapping_status=UNVERIFIED` 始终明确指出尚未核验到 NIfTI/NPZ 的实际体积。
任何读取失败、帧数不一致、Series UID 混合/缺失均令索引 incomplete，而不会假装完整。
完整索引可消费较多内存和 I/O，启用前应在科研副本中测量实际数据规模。

结果 ZIP 缓存指纹包含语义版本、配置内容和索引开关，避免复用旧规则产物。
部署后可能发生一次缓存失配；不会删除既有 ZIP 或迁移历史 `_FAT` 数据。

## 验证与下一步

运行：

```bash
python -m pytest -q test/test_mr_semantics.py
python -m pytest -q test --ignore=test/verify_ct_geometry_v4_real_source.py
```

覆盖最小反例、Dixon 四输出、多值编码、PRE 冲突、重复平扫、跨午夜、
STIR、标准 b 值、增强型 MR 多帧优先级、帧成员关系、索引失败、
源删除后的缓存导出，以及新旧 Excel 双表集成。
真实源 CT 验证脚本需要外部数据，不由合成测试替代；此次没有访问 PACS。
仓库约定的 Snyk 扫描工具在本次环境不可用，没有声称完成该扫描。

后续验收应独立进行：

1. 在科研副本抽取多厂商真实 MR，比较新旧输出差异，按属性人工核验。
   优先审核新旧不一致、CONFLICT、Dixon、IR、动态重复、混合回波/扩散序列。
2. 分别报告各属性自动接受标签的 precision、自动覆盖率、unknown/conflict 率；
   另报交付排除率和具体原因。UNKNOWN 下降不能单独作为优化成功依据。
3. 依据转换器 sidecar/输出维度与源帧索引验证真实体积对应，补齐覆盖范围比较和厂商适配器；
   核验前不可把候选关系视为精确配对或配准。
4. 使用逐属性掩码训练轻量模型；只有经独立人工测试集验证后，才扩展 inferred 自动接受范围。
   图像压脂效果、组织强化和跨对比度组织一致性另做图像验证。

这次 PR 建立了错误修复、证据输出、可选索引和新旧并行比较的基础。
精确输出体积映射及 MRI_YOLO 训练收益尚未完成；下述真实数据审计不替代人工金标准验证。

## 2026-10-31 审查加固

- 否定增强短语覆盖 `no contrast / not enhanced / 未增强 / 非增强`；不再输出错误的 POST/C+。
- 单字母 W/F/IP/OP 仅在明确 Dixon 上下文中用于成分识别；补充西门子
  `OPP_PHASE` 结构化标记及 Dixon 名称 `_opp` 后缀。
- 补齐 GRE/SPOILED/TSE 的 fat-only、fat-suppressed、in-phase、opposed-phase 家族映射，
  以及 `T2_GRE*`、`PD_UNKNOWN` 的既有分类到家族映射。
- `isDerivedImage` 与 `refinedImageType` 保持一致；失败清理保留 `mr_frame_index.json`。
- MR 重命名同步 `DICOM_Metadata`、`MR_Cleaned`、`MR_Cleaned_v2` 三表的 SeriesFolder。
- 自定义 `propagated_phase` 不再被重算覆盖；直接元数据索引使用 `run_cpu_bound()`。

## 2026-10-31 真实数据审计

审计只读取 `/mnt/d/download/MRdownloads` 的元数据 Excel，不读取 NIfTI 像素：

- 全量读取实际存在的 9260 个检查目录、110984 条序列，Excel 读取错误为 0；
  `isDerivedImage` 与 `refinedImageType` 不一致为 0，否定增强短语误判 POST 为 0。
- 初轮 8053 条 `sequenceFamily=UNKNOWN` 中，6424 条为 `EXCLUDED`，793 条为
  `sequenceClass=UNKNOWN`，其余 836 条是家族映射遗漏，不是应当一律修复的分类错误。
  补齐映射后剩余 7217 条 UNKNOWN 家族，均属于上述排除或保守未知类别。
- 患者去重分层抽样 484 个检查，覆盖 Siemens、UIH、Philips、GE、安科及多部位；
  其中 480 个可按内部 AccessionNumber 直接重读，共复核 5442 条序列。
  4 个检查的目录名与内部 AccessionNumber 不同，不影响全量审计。
- 分层样本中 367 条 Dixon 序列：水像 184、脂像 123、同相位 31、反相位 28；
  已分类序列的成分与命名后缀不一致为 0。另有一条已排除 PosDisp 的结构化 WATER
  与名称 F 冲突，按设计保留 CONFLICT，没有静默覆盖。
- 与 MR_YOLO 弱标签可按系列匹配的 3565 条中，家族一致 3242 条。323 条分歧主要是
  本 PR 有意细分的水脂成分、定量图家族口径差异及弱标签误标；弱标签不是人工金标准，
  该一致率不能解释为准确率。
- 484 条代表记录的 V2 JSON 均可解析，必需属性、状态和监督掩码检查无错误。

验证：专项 68 passed，全套 253 passed / 3 skipped。仍未验证真实 NIfTI 候选体积映射、
图像层面的组织强化/压脂效果，以及独立人工测试集上的逐属性 precision。
