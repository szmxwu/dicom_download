# DICOM 下载与处理客户端

这是一个统一的 DICOM 文件下载和处理工具，可以直接从 PACS 服务器下载数据，并进行元数据提取和格式转换。

## 功能特点

- **PACS 直接集成**: 使用 DICOM 协议 (C-FIND, C-MOVE) 直接与 PACS 服务器通信。
- **元数据提取**: 将 DICOM 标签提取到 Excel 文件中。支持不同模态（MR, CT, DX, MG）的自定义模板。
- **MR 元数据治理（MR_clean）**: 当存在 MR 序列时，导出的 Excel 会额外生成 `MR_Cleaned` 工作表，包含标准化的特征与分类结果（如 `sequenceClass`、`standardOrientation`、`isFatSuppressed`、`dynamicGroup`、`dynamicPhase` 等）。
- **图像转换**: 将 DICOM 序列转换为 NIfTI 格式。
- **Web 界面**: 提供友好的 Web 界面用于查询患者和管理任务。
- **多模态支持**: 针对 MRI、CT、数字X光 (DX/DR) 和乳腺钼靶 (MG) 提供专门的元数据提取支持。

## 安装说明

1. 克隆项目代码。
2. 安装依赖包：
   ```bash
   pip install -r requirements.txt
   ```

## 配置说明

### PACS 连接配置
在项目根目录下创建 `.env` 文件，并填入 PACS 服务器信息：

```ini
# DICOM Server Configuration
PACS_IP=172.17.250.192
PACS_PORT=2104
CALLING_AET=WMX01
CALLED_AET=pacsFIR
CALLING_PORT=1103
```

### 元数据模板配置
您可以通过编辑 `dicom_tags/` 目录下的 JSON 文件来自定义不同模态提取的 DICOM 标签：
- `mr.json`: 磁共振 (MRI)
- `ct.json`: CT
- `dx.json`: 数字X光 (DR/DX/CR)
- `mg.json`: 乳腺钼靶 (Mammography)

MRI 治理相关说明：
- `mr.json` 需要包含 `ImageType` 字段（`MR_clean.py` 用它做 refined image type / subtype 识别）。

### MR_clean 规则配置
MRI 治理的规则（关键词/阈值/正则等）已抽离到 `mr_clean_config.json`，便于后续直接改配置而不改代码。

- 默认行为：`MR_clean.process_mri_dataframe(df)` 会自动加载 `mr_clean_config.json`。
- 高级用法：可通过 `cfg=...` 直接传入配置字典，或通过 `config_path=...` 指定自定义配置文件路径。
- 常见可调项：
   - `thresholds.field_strength.*`：不同磁场强度的 TR/TE/TI 阈值
   - `classification.ruleA`：名称优先的特殊序列识别
   - `classification.sequence_family`：GRE/SE/TSE 家族判断规则
   - `dynamic.*`：动态增强分组与增强判定规则

## 使用方法

1. 启动 Web 应用：
   ```bash
   python app.py
   ```
2. 打开浏览器访问 `http://localhost:5000`。
3. 使用界面查询患者并开始下载/处理任务。

### 输出说明
- 元数据 Excel 至少包含 `DICOM_Metadata` 与 `Series_Summary` 两个工作表。
- 当存在 MR 记录时，会额外生成 `MR_Cleaned` 工作表。

## 项目结构

- `app.py`: Flask Web 应用程序入口。
- `dicom_client_unified.py`: 核心 DICOM 处理逻辑。
- `MR_clean.py`: MR 元数据治理与序列分类逻辑。
- `dicom_tags/`: 元数据提取配置文件目录。
- `templates/`: Web 界面 HTML 模板。
- `static/`: 静态资源 (CSS, JS)。
- `uploads/`: 上传文件目录。
- `results/`: 处理结果目录。
