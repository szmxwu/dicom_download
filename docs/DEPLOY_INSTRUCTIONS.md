# MR Metadata 空值问题排查 - 部署说明

## 已推送的日志增强代码

Commit: `402dd29` - Add detailed logging for MR metadata empty bug investigation

新增的关键日志标记：
- `[CACHE]` - `_cache_metadata_for_series` 调用及结果
- `[COLLECT]` - `_collect_metadata_from_dicoms` 读取过程
- `[TAGS]` - `_build_sample_tags` 构建过程
- `[MINIMAL_CACHE]` - `_write_minimal_cache` 写入过程
- `[CONVERT_CACHE]` - `convert_dicom_to_nifti` 中缓存调用
- `[ORG]` - `process_single_series` 中缓存调用
- `[META]` - `extract_dicom_metadata` 缓存/DICOM 读取过程

## 服务器部署步骤

### 1. 服务器上拉取最新代码

```bash
cd /path/to/dicom_download
git pull origin main
```

### 2. 重启 Flask 服务

如果使用了 systemd/supervisor：
```bash
sudo systemctl restart dicom_download
# 或
sudo supervisorctl restart dicom_download
```

如果是手动启动的，先停止再启动：
```bash
# 找到并结束旧进程
pkill -f "python.*app.py"
# 或
pkill -f "flask run"

# 重新启动
python src/web/app.py
```

### 3. 运行 MR 下载任务

在 Web UI 上提交 AccessionNumber: `Z26012900821` 的下载任务。

确保参数：
- Output format: `nifti`
- Parallel pipeline: `True`（默认）

### 4. 任务完成后获取日志

```bash
# 复制日志到可下载的位置
cp /path/to/dicom_download/logs/app.log /path/to/dicom_download/logs/app.log.debug

# 或者直接用 sz 命令传输（如果支持）
```

把 `logs/app.log` 复制回本地后传给我分析。

## 分析重点

我需要查看以下关键日志：

1. `[CACHE]` 日志：`_cache_metadata_for_series` 是否被调用？`records` 数量？`sample_tags` 是否为空？
2. `[COLLECT]` 日志：`pydicom.dcmread` 是否成功？keywords 数量？
3. `[TAGS]` 日志：`_build_sample_tags` 是否成功？
4. `[CONVERT_CACHE]` 日志：转换前缓存是否被调用？
5. `[META]` 日志：metadata 提取时是读取 DICOM 还是缓存？缓存内容摘要？

通过这些日志可以精确定位是：
- `_cache_metadata_for_series` 没有被调用
- 调用时 `dicom_files` 已为空
- `_collect_metadata_from_dicoms` 或 `_build_sample_tags` 抛异常
- 缓存被 `_write_minimal_cache` 覆盖了
