# -*- coding: utf-8 -*-
import PyInstaller.__main__
import os
import shutil

# 定义程序名称
APP_NAME = "DICOMDownloader"
ENTRY_POINT = "app.py"

# 需要包含的数据文件/文件夹
# 格式为: (源路径, 目标文件夹名称)
DATA_FILES = [
    ('templates', 'templates'),
    ('static', 'static'),
    ('dicom_tags', 'dicom_tags'),
    ('keywords.json', '.'),
    ('mr_clean_config.json', '.'),
    ('README.md', '.'),
    ('README_CN.md', '.'),
]

def build():
    print(f"开始打包 {APP_NAME}...")
    
    # 构建 PyInstaller 命令参数
    args = [
        ENTRY_POINT,
        '--name=%s' % APP_NAME,
        '--onedir',  # 生成一个文件夹，包含所有库和exe
        '--windowed', # 不显示控制台窗口 (如果是开发调试可以去掉)
        '--noconfirm',
        '--clean',
    ]

    # 添加数据文件
    for src, dst in DATA_FILES:
        # 在 Windows 上分隔符通常是分号 ;
        args.append(f'--add-data={src}{os.pathsep}{dst}')

    # 添加可能需要的隐藏导入
    hidden_imports = [
        'engineio.async_drivers.threading',
        'eventlet',
        'gevent',
        'flask_socketio',
    ]
    for imp in hidden_imports:
        args.append(f'--hidden-import={imp}')

    # 执行打包
    PyInstaller.__main__.run(args)
    
    print("\n打包完成！可执行文件位于 dist/%s/%s.exe" % (APP_NAME, APP_NAME))
    print("请确保将整个 dist/%s 文件夹安装到 Windows Server 上。" % APP_NAME)

if __name__ == "__main__":
    # 检查是否安装了 pyinstaller
    try:
        import PyInstaller
    except ImportError:
        print("错误: 未找到 PyInstaller。请运行 'pip install pyinstaller'。")
    else:
        build()
