# Windows Server 2012 R2 部署指南

由于 Windows Server 2012 R2 是较旧的操作系统，请按照以下步骤进行部署。

## 方法一：直接运行源代码 (推荐)

这是最灵活的方法，便于调试。

1. **安装 Python**: 
   - 下载并安装 [Python 3.8.10](https://www.python.org/downloads/release/python-3810/) (这是支持旧版 Windows 且非常稳定的版本)。
   - 安装时勾选 "Add Python to PATH"。

2. **准备运行**:
   - 将本项目文件夹拷贝到服务器。
   - 双击运行 `run_on_windows.bat`。
   - 脚本会自动创建虚拟环境、安装依赖并启动服务。

3. **访问**:
   - 浏览器打开 `http://127.0.0.1:5005`。

## 方法二：打包为 EXE

如果你希望分发一个不需要安装 Python 的文件夹，可以使用 PyInstaller 打包。

**注意**：打包必须在 Windows 机器上执行（不能在 Linux 上跨平台打包）。

1. 在一台联网的 Windows 开发机上，安装依赖：
   ```bash
   pip install -r requirements.txt
   ```
2. 运行打包脚本：
   ```bash
   python build_windows.py
   ```
3. 打包完成后，进入 `dist/DICOMDownloader` 文件夹。
4. 将整个 `DICOMDownloader` 文件夹拷贝到 Windows Server 2012 R2。
5. 双击 `DICOMDownloader.exe` 即可运行。

## 注意事项 (Windows Server 2012 R2)

1. **防火墙**: 若需远程访问，请在防火墙中开放 `5005` 端口。
2. **KB补丁**: 确保服务器已安装所有重要的 Windows Update 补丁（尤其是与 Universal C Runtime 相关的补丁）。
3. **管理员权限**: 运行批处理或 EXE 时，建议使用管理员权限，以确保有权限写入生成的 `results` 和 `uploads` 文件夹。
