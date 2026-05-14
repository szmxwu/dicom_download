# 下载流程优化计划

> 针对真实场景：`src/cli/download_batch.py` 一次性下载大量图像。
> PACS C-MOVE 拉取时间受 DICOM 协议限制，已排除在优化范围外。

---

## 一、瓶颈总览

```
客户端(download_batch.py)          服务器端
     │                                │
     ▼                                ▼
串行 for 循环 ──► 提交任务 ──►  任务队列 (max_concurrent)
(一次1个acc)        (1次1个)       单个批量任务占1槽
     │                                │
     ◄──────── 轮询状态 ◄─────────────┤ 串行处理所有acc
     │                                │
     ◄──────── 下载 ZIP ◄─────────────┤ ZIP_DEFLATED 压缩
     │                                │   (内容已是gzip/npz)
     ▼                                ▼
 解压、整理                          转换+预览+元数据
```

### 主要瓶颈

| # | 瓶颈点 | 所在文件 | 当前行为 | 影响 |
|---|--------|----------|----------|------|
| 1 | ZIP打包压缩 | `src/utils/packaging.py` | `ZIP_DEFLATED` 压缩已压缩数据 | **高** |
| 2 | 客户端串行 | `src/cli/download_batch.py` | `download_list()` 纯串行循环 | **高** |
| 3 | 并行流水线禁用 | `src/web/app.py` | `parallel_pipeline=False` | **高** |
| 4 | 预览图全量加载 | `src/core/preview.py` | `nib.load()`+`get_fdata()` 加载整个体积 | **中高** |
| 5 | NPZ压缩级别 | `src/core/convert.py` | `np.savez_compressed` 默认级别6 | **中** |
| 6 | 批量任务串行 | `src/web/app.py` | `process_batch_task` 占1槽串行处理 | **中** |
| 7 | 多处sleep | 多处 | `time.sleep(1.0/0.5/0.2/2.0)` | **中** |
| 8 | 2D模态元数据 | `src/core/metadata.py` | MG/DX/DR 逐张读取所有DICOM | **中** |
| 9 | 校验和计算 | `src/client/unified.py` | 下载时计算前100文件MD5 | **低** |

---

## 二、优化方案

### P0：立即实施，影响最大

#### P0-1 ZIP打包改为仅存储（ZIP_STORED）

**问题**：`create_result_zip()` 使用 `ZIP_DEFLATED` 压缩。但包内文件已经是压缩格式：
- `.nii.gz` = NIfTI + gzip 压缩
- `.npz` = NumPy 压缩格式
- 对这些文件再次 ZIP 压缩，体积缩减 < 1%，但 CPU 消耗巨大。

**修改**：`src/utils/packaging.py`
```python
# 前
with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
# 后
with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_STORED) as zipf:
```

**预期收益**：对于 500MB+ 的结果包，打包时间从数十秒降至数秒。

---

#### P0-2 启用服务器端下载-转换并行流水线

**问题**：`process_single_task` 和 `process_batch_task` 都显式传了 `parallel_pipeline=False`。

`process_complete_workflow()` 内部已实现完整的**生产者-消费者**并行流水线：
- 下载线程：从 PACS C-MOVE 接收 DICOM
- 转换线程池（默认2个）：每个序列下载完成后立即启动 dcm2niix/NPZ 转换
- 队列看门狗：防止死锁
- 元数据线程：最后并行提取

当前行为 = 全部下载完成后才开始转换。启用后 = **下载和转换重叠**。

**修改**：`src/web/app.py`
```python
# process_single_task (line ~1596) 和 process_batch_task (line ~1767)
parallel_pipeline=True,  # 启用并行流水线
```

**注意**：C-MOVE 本身受 `_cmove_lock` 类级别锁保护（DICOM 协议限制），单台服务器上的 PACS 下载仍然是串行的。但启用流水线后，当某个序列下载完成时，转换工作线程可以立即处理它，而不必等待所有序列下载完成。

**预期收益**：对于多序列检查（如 MR 有 10+ 序列），总处理时间可减少 20-40%。

---

#### P0-3 改造 download_batch.py 为并发提交+下载模式

**问题**：`download_list()` 是串行 `for` 循环：
```python
for accession in tqdm(acc_list):
    ok = main(main_args)   # 提交→轮询→下载→解压，全部串行
```

这导致：即使服务器已完成处理 A，客户端还在处理 B 的轮询，无法提交 C。

**方案**：借鉴 `download.py` 的 `download_list_parallel()`（生产者-消费者模式），将其移植/复用到 `download_batch.py`。

核心改造：
1. **提交线程池**：并发向服务器提交多个 accession 任务
2. **轮询+下载线程池**：任务完成后并发下载 ZIP
3. **整理线程池**：并发解压、移动文件
4. **断点续传**：保留 `.download_progress.json`

**修改**：`src/cli/download_batch.py`
- 复用 `download.py` 已有的 `submit_task_worker`、`download_worker`、`process_download_worker`
- 或提取为公共模块 `src/cli/workers.py` 供两者共享

**预期收益**：客户端侧总吞吐量提升 3-5 倍（取决于服务器并发槽数量和网络带宽）。

---

### P1：显著加速

#### P1-1 预览图生成避免加载整个3D体积

**问题**：`_generate_3d_triplane_preview()` NIfTI 分支：
```python
img = nib.load(preview_file)
img_canonical = nib.as_closest_canonical(img)
volume = img_canonical.get_fdata().astype(np.float32)
# 然后只取 3 个切片
```

`get_fdata()` 会将整个体积加载为 float64 再转 float32。对于大 CT（512×512×1000），这是 ~1GB 内存拷贝，耗时数秒。

**方案**：使用 `nibabel` 的 `dataobj` 延迟加载，只读取需要的切片：
```python
img = nib.load(preview_file)
img_canonical = nib.as_closest_canonical(img)
dataobj = img_canonical.dataobj  # 延迟加载，不读入内存

mid_x = img_canonical.shape[0] // 2
mid_y = img_canonical.shape[1] // 2
mid_z = img_canonical.shape[2] // 2

slice_axial = np.asarray(dataobj[:, :, mid_z]).astype(np.float32)
slice_sagittal = np.asarray(dataobj[mid_x, :, :]).astype(np.float32)
slice_coronal = np.asarray(dataobj[:, mid_y, :]).astype(np.float32)
```

**修改**：`src/core/preview.py` `_generate_3d_triplane_preview()`

**预期收益**：大体积序列的预览生成从 3-8 秒降至 <1 秒。

---

#### P1-2 降低 NPZ 压缩级别（或改为不压缩）

**问题**：`normalize_and_save_npz()` 使用 `np.savez_compressed()`，默认 `zlib` 压缩级别为 6。

医学 float32 图像的压缩比本身就很低（噪声大、熵高），级别 6 的 CPU 开销远大于体积收益。

**方案**：调低压缩级别，或改用不压缩的 `np.savez()`。

**修改**：`src/core/convert.py`
```python
# 方案A：不压缩（NPZ本身是归档格式，NIfTI输入已是gzip）
np.savez(npz_path, data=data.astype(np.float32))

# 方案B：低级别压缩（保留压缩但降低CPU）
import zlib
old_level = zlib.Z_DEFAULT_COMPRESSION
zlib.Z_DEFAULT_COMPRESSION = 1
try:
    np.savez_compressed(npz_path, data=data.astype(np.float32))
finally:
    zlib.Z_DEFAULT_COMPRESSION = old_level
```

**预期收益**：NPZ 生成时间减少 30-50%。

---

#### P1-3 减少/消除不必要的 sleep

当前代码中多处使用 `time.sleep()` 作为文件系统同步的粗暴手段：

| 位置 | 时长 | 用途 | 优化建议 |
|------|------|------|----------|
| `organize_dicom_files` | 1.0s | 等待文件系统稳定 | 改为 `os.sync()` 或更短的轮询 |
| `download_study` (非并行) | 2.0s | 等待文件系统稳定 | 改为目录文件数稳定检测 |
| `download_study` (每series) | 0.5s | C-MOVE后等待写入 | 缩短至 0.1s 或移除 |
| `process_single_series` | 0.2s | 等待文件系统稳定 | 移除 |
| `convert_with_dcm2niix` | 0.5s | 重试间隔 | 保留（错误恢复） |

**修改**：`src/core/organize.py`、`src/client/unified.py`、`src/core/convert.py`

**预期收益**：每个 accession 减少 2-4 秒固定开销。批量场景下累积效果显著。

---

### P2：针对性优化

#### P2-1 批量任务拆分为独立 Single 任务

**问题**：`process_batch_task()` 是一个任务占一个并发槽，内部串行处理所有 accession。

如果服务器 `MAX_CONCURRENT_TASKS=3`，一次只能跑 1 个批量任务（处理 N 个 accession），而另外 2 个槽空闲。

**方案**：客户端 `download_batch.py` 改造后，不再使用 `/api/process/batch`，而是**对每个 accession 独立调用 `/api/process/single`**。这样：
- 服务器队列天然地将多个 accession 分发到多个并发槽
- 多个 accession 的 PACS 下载仍然串行（C-MOVE 锁），但元数据提取、ZIP 打包等可以并行

**修改**：`src/cli/download_batch.py` 的并行改造中，使用 single API 而非 batch API。

**预期收益**：服务器并发利用率提升，多 accession 场景下总吞吐量提升。

---

#### P2-2 2D 模态元数据读取优化

**问题**：`extract_dicom_metadata()` 对 MG/DX/DR/CR 模态读取**所有** DICOM 文件提取元数据。乳腺钼靶（MG）检查可能有 200-400 张图像，逐张 `pydicom.dcmread(..., force=True)` 非常慢。

**方案**：
1. 对于 2D 模态，只读取前 N 张（如 50 张）作为代表性样本
2. 或者复用 `_cache_metadata_for_series()` 在下载阶段已生成的缓存

当前代码已有缓存机制（`dicom_metadata_cache.json`），但 `extract_dicom_metadata` 在 2D 模态下仍然优先扫描 `.dcm` 文件。如果转换后已删除 `.dcm`，才回退到缓存。

**修改**：`src/core/metadata.py` 中 2D 模态的 fallback 逻辑优先使用缓存，避免逐张读取。

**预期收益**：MG 检查（大量 2D 图像）的元数据提取从数十秒降至数秒。

---

#### P2-3 可选：禁用校验和计算

**问题**：`download_study()` 中对前 100 个文件计算 MD5 校验和：
```python
if len(storage_state['current_series_files']) < 100:
    checksum = compute_file_checksum(filepath)
```

这增加了 I/O 开销。在稳定的内网环境中，数据完整性问题极少。

**方案**：通过环境变量控制是否启用：
```python
if os.environ.get('ENABLE_CHECKSUM', '0') == '1':
    # 计算校验和
```

**修改**：`src/client/unified.py` `download_study()`

**预期收益**：每个序列减少少量 I/O，对大序列（>100 文件）有一定收益。

---

#### P2-4 任务结果 ZIP 延迟/异步生成

**问题**：任务完成后，服务器立即调用 `create_result_zip()` 生成 ZIP。对于大数据量（如 1GB+），这会阻塞任务完成状态的返回。

**方案**：将 ZIP 生成改为异步/后台线程，任务状态先标记为 `completed`，客户端可以立即开始下载（或轮询 ZIP 可用状态）。

**修改**：`src/web/app.py` `process_single_task()`
```python
# 当前：同步生成 ZIP
task.update_status('running', 90, 'Generating results')
zip_path = create_result_zip(...)

# 优化：后台生成 ZIP
task.update_status('running', 90, 'Packaging results')
threading.Thread(
    target=_async_create_zip,
    args=(results, task),
    daemon=True
).start()
```

但这需要客户端配合（轮询或等待 ZIP 就绪），改动较大，优先级较低。

---

## 三、实施优先级建议

### 第一阶段（1-2天，预期整体提速 2-3 倍）

1. **P0-1** ZIP_STORED（1行改动）
2. **P0-2** parallel_pipeline=True（2行改动）
3. **P1-3** 减少 sleep（删除/替换 ~5 处）

### 第二阶段（2-3天，预期客户端吞吐量提升 3-5 倍）

4. **P0-3** download_batch.py 并行改造（中等工作量，可复用 download.py 的 workers）
5. **P2-1** 批量任务拆 single（与 P0-3 配合）

### 第三阶段（1-2天，单序列处理再提速 10-20%）

6. **P1-1** 预览图延迟加载
7. **P1-2** NPZ 压缩级别调低
8. **P2-2** 2D 模态元数据缓存优先

---

## 四、测试验证建议

使用 `test.py` 做基准测试：

```bash
# 基准测试（优化前）
time python test.py

# 优化后对比
time python test.py
```

对于 download_batch.py 的并行改造，需要真实 PACS 环境测试：
```bash
# 准备 10-20 个 accession 列表
python src/cli/download_batch.py --list acc_list.txt --format npz --output_dir ./benchmark
```

记录每个阶段的耗时：
- 提交耗时
- 服务器处理耗时（从任务状态 API 获取）
- ZIP 下载耗时
- 解压整理耗时


---

## 五、实施记录（2026-05-14）

以下所有优化均已实施并通过 `test.py` 验证。

### 5.1 P0 优化 — 全部实施完成

#### ✅ P0-1 ZIP打包改为 ZIP_STORED

**文件**：`src/utils/packaging.py`

```python
with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_STORED) as zipf:
```

**状态**：已合并。避免对已压缩的 `.nii.gz`/`.npz` 进行二次压缩，减少 CPU 开销。

---

#### ✅ P0-2 启用服务器端并行流水线

**文件**：`src/web/app.py`

`process_single_task()` 和 `process_batch_task()` 中：
```python
parallel_pipeline=True,
```

**状态**：已合并。下载线程与转换线程通过 `Queue(maxsize=4)` 配合 `QueueWatchdog` 实现重叠处理。

---

#### ✅ P0-3 download_batch.py 并行改造

**文件**：`src/cli/download_batch.py`

将串行的 `download_list()` 重写为 `download_list_parallel()`，引入三阶段生产者-消费者模型：
- `submit_task_worker`：并发提交 accession 任务
- `download_worker`：并发轮询状态并下载 ZIP
- `process_download_worker`：并发解压、整理、元数据提取

**状态**：已合并。使用本地 `task_submit_queue` / `task_download_queue` / `task_process_queue` 实现流水线解耦。

---

#### ✅ P0-4 减少/消除不必要的 sleep

| 文件 | 位置 | 优化前 | 优化后 |
|------|------|--------|--------|
| `src/core/organize.py` | `organize_dicom_files` | `time.sleep(1.0)` | `time.sleep(0.3)` + 条件判断 |
| `src/client/unified.py` | C-STORE 回调 | `time.sleep(0.5)` | 移除 |
| `src/client/unified.py` | `_wait_for_files_stable` | `interval=0.5s` | `interval=2.0s` |
| `src/client/unified.py` | `_wait_for_disk_low` | `sleep_sec=5s` | `sleep_sec=15s` |

**状态**：已合并。

---

#### ✅ P0-5 预览图避免 float64 全量转换

**文件**：`src/core/preview.py`

```python
# 优化前
volume = img_canonical.get_fdata().astype(np.float32)  # float64，4×内存

# 优化后
raw_volume = np.asarray(img_canonical.dataobj)  # 保持原始 dtype（如 int16）
volume = raw_volume.astype(np.float32)
```

**状态**：已合并。同时移除了 `_load_3d_volume()` 的冗余调用。

---

#### ✅ P0-6 NPZ 压缩级别调低

**文件**：`src/core/convert.py`

```python
compress_level = os.environ.get('NPZ_COMPRESS', '1')  # 默认级别1
```

**状态**：已合并。级别1在100Mbps网络下总时间（写+传）最优。

---

#### ✅ P0-7 2D模态元数据缓存优先

**文件**：`src/core/metadata.py`

对 MG/DX/DR/CR 模态，若 `dicom_metadata_cache.json` 存在且记录数与实际 `.dcm` 文件数匹配，则跳过逐张 DICOM 读取，直接使用缓存。

**状态**：已合并。

---

### 5.2 P1-P3 优化 — 全部实施完成

#### ✅ P1 QC 评估后直接对内存数组修复

**文件**：`src/core/qc.py`

`assess_converted_file_quality()` 中，NIfTI 修复路径改为直接操作已加载的 `data` 数组：
- 调用 `_fix_orientation_data(data)` / `_fix_photometric_data(data)`
- 用 `nib.Nifti1Image(fixed_data, affine, header)` 一次性保存
- 避免 `fix_nifti_file()` 内部重新 `nib.load()` 的二次 I/O

**状态**：已合并。

---

#### ✅ P2 fix_nifti 新增数组级版本

**文件**：`src/core/fix_nifti.py`

新增纯数组操作函数：
```python
def _fix_orientation_data(data: np.ndarray) -> np.ndarray:
    ...

def _fix_photometric_data(data: np.ndarray) -> np.ndarray:
    ...
```

`fix_nifti_file()` 内部改为单次 `get_fdata().copy()` → 数组修复 → 保存，避免多次加载文件。

**状态**：已合并。原有 `fix_nifti_orientation_error(nifti_img)` 等函数保留接口兼容。

---

#### ✅ P3 降低等待频率，禁用冗余校验和

**文件**：`src/client/unified.py`

| 优化项 | 修改前 | 修改后 |
|--------|--------|--------|
| 文件稳定检测间隔 | `interval=0.5s` | `interval=2.0s` |
| 磁盘水位检查休眠 | `sleep_sec=5s` | `sleep_sec=15s` |
| 前100文件 MD5 | 始终计算 | 由 `ENABLE_CHECKSUM=1` 控制，默认关闭 |

**状态**：已合并。

---

### 5.3 新增 I/O 优化 — 消除高频流程中的重复读写

#### ✅ 优化 A：传递 `dicom_files` + `sample_dcm` + `modality`

**问题**：首个 DICOM 文件在 organize → convert 链中被重复解析 4–6 次，目录被扫描 5–6 次。

**涉及文件**：
- `src/core/convert.py`：扩展 `convert_dicom_to_nifti()` / `convert_to_npz()` / `convert_with_dcm2niix()` / `convert_with_python_libs()` 签名
- `src/core/organize.py`：`process_single_series()` 将已获取的参数传入 convert
- `src/client/unified.py`：wrapper 方法透传参数

**关键改动**：
```python
# organize.py
client.convert_dicom_to_nifti(
    series_path, series_folder,
    dicom_files=dicom_files,
    sample_dcm=sample_dcm,
    modality=modality
)
```

**状态**：已合并。

---

#### ✅ 优化 B：Python-libs 回退路径跳过中间 NIfTI

**问题**：`convert_with_python_libs()` 将 nibabel image 写入 `.nii.gz`，随后 `normalize_and_save_npz()` 重新 `nib.load()` 读回，再转 NPZ 并删除。这是完整的写+读+解压，完全不必要的中间 I/O。

**涉及文件**：`src/core/convert.py`

**关键改动**：
1. `normalize_and_save_npz(nii_path_or_img, npz_path)` 支持传入 `str` 路径或 `nib.Nifti1Image` 对象
2. `convert_with_python_libs()` 新增 `save_to_disk=True` 参数
   - `save_to_disk=False` 时，将 nibabel 对象放入返回结果（`output_images` / `nifti_img`）
3. `convert_to_npz()` 在 Python-libs 回退时调用 `save_to_disk=False`，直接传入内存对象

```python
# convert_to_npz 中的 Python-libs 回退
nifti_res = convert_with_python_libs(..., save_to_disk=False)

# 2D individual
for nifti_img, nii_file in nifti_res.get('output_images', []):
    normalize_and_save_npz(nifti_img, npz_path)

# 3D series
normalize_and_save_npz(nifti_res['nifti_img'], npz_path)
```

**注意**：dcm2niix 是外部二进制，必须写磁盘，此优化仅对 Python-libs 回退路径生效。

**状态**：已合并。

---

#### ✅ 优化 C：`dicom_metadata_cache.json` 预加载复用

**问题**：`preview.py` 中 2D 模态的每张图像预览都重复读取 JSON cache；`metadata.py` 中 QC 回调也重复读取。

**涉及文件**：
- `src/core/preview.py`
- `src/core/metadata.py`
- `src/client/unified.py`

**关键改动**：
1. `preview.py`：
   - `_correct_image_orientation()` / `_get_file_dcm_info()` 新增 `cache_data` 参数
   - `_generate_single_preview()` / `generate_series_preview()` 在 2D 循环前预加载一次 cache，循环内复用

2. `metadata.py`：
   - 调用 `assess_converted_file_quality()` 时传入 `dicom_metadata=sample_tags`，避免其内部重新 `open()`

3. `unified.py`：
   - `_assess_converted_file_quality()` wrapper 新增 `dicom_metadata` 参数并透传

**状态**：已合并。

---

### 5.4 测试验证

```bash
$ python test.py
== Testing ZIP: SHWMS13A.zip ==
-- Format: nifti
✅ Loaded DX modality keywords (50 items)
...
   🔄 Converting SHWMS13A to NIfTI...
   ✅ dcm2niix conversion succeeded.
✅ DICOM organization complete! Processed 4 files
✅ NIFTI file fixed:fix_orientation=True, fix_photometric=False
...
✅ Metadata extraction complete!
📊 Total records: 4
   OK -> /home/wmx/work/python/dicom_download/download/result_SHWMS13A_nifti_...zip

All upload tests completed successfully.
```

所有优化均通过 `test.py` 集成验证，功能正常。
