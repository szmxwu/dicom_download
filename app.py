# -*- coding: utf-8 -*-
#!/usr/bin/env python3
"""
DICOM处理Web应用
简化版本 - 仅包含文件处理功能，无需用户认证

修改说明：
- 每个需要下载的任务都会自动创建新的DICOM客户端实例
- 任务开始前会调用DICOM客户端的兼容性登录接口（当前实现不做真实认证）
- 任务完成后自动登出，确保会话安全
- 上传文件处理不需要登录，仅处理本地文件
- 全局客户端仅用于系统状态检查
"""

from flask import Flask, render_template, request, jsonify, send_file, send_from_directory
from flask_socketio import SocketIO, emit
import os
import json
import time
import uuid
import threading
from pathlib import Path
import zipfile
import shutil
import sys
import logging
from logging.handlers import RotatingFileHandler
from werkzeug.utils import secure_filename

from dotenv import set_key
import secrets

# 导入我们的DICOM处理客户端
from dicom_client_unified import DICOMDownloadClient

def get_base_path():
    """获取程序运行时的根目录路径，兼容 PyInstaller 打包"""
    if hasattr(sys, '_MEIPASS'):
        return sys._MEIPASS
    return os.path.abspath(".")

# Flask应用配置 - 指定静态文件和模板路径
app = Flask(__name__,
            static_folder=os.path.join(get_base_path(), 'static'),
            template_folder=os.path.join(get_base_path(), 'templates'))

_secret_key = os.environ.get('FLASK_SECRET_KEY')
if not _secret_key:
    # 仅用于本地/临时运行；生产环境请通过环境变量提供固定值
    _secret_key = secrets.token_hex(32)
app.config['SECRET_KEY'] = _secret_key
app.config['UPLOAD_FOLDER'] = './uploads'
app.config['RESULT_FOLDER'] = './results'
app.config['MAX_CONTENT_LENGTH'] = 1500 * 1024 * 1024  # 1500MB最大文件大小

# 创建必要的目录
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['RESULT_FOLDER'], exist_ok=True)
os.makedirs('./temp', exist_ok=True)
os.makedirs('./logs', exist_ok=True)

# 配置日志系统
def setup_logging():
    logger = logging.getLogger('DICOMApp')
    logger.setLevel(logging.INFO)
    
    # 防止重复添加 handler
    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        '[%(asctime)s] %(levelname)s in %(module)s: %(message)s'
    )

    # 文件日志 (按大小回滚)
    file_handler = RotatingFileHandler(
        'logs/app.log', maxBytes=10*1024*1024, backupCount=5, encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # 控制台日志
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger

logger = setup_logging()

# WebSocket支持
socketio = SocketIO(app, cors_allowed_origins="*")

# 全局变量存储处理任务
processing_tasks = {}

# 创建DICOM客户端实例用于系统状态检查（不登录）
dicom_client_checker = DICOMDownloadClient()

ENV_FILE_PATH = os.path.join(os.path.dirname(__file__), '.env')


def _parse_port(value, field_name: str) -> int:
    if value is None:
        raise ValueError(f"{field_name} is required")
    try:
        port = int(value)
    except (TypeError, ValueError):
        raise ValueError(f"{field_name} must be an integer")
    if port < 1 or port > 65535:
        raise ValueError(f"{field_name} must be between 1 and 65535")
    return port


def _normalize_aet(value, field_name: str) -> str:
    if value is None:
        raise ValueError(f"{field_name} is required")
    aet = str(value).strip()
    if not aet:
        raise ValueError(f"{field_name} is required")
    if len(aet) > 16:
        raise ValueError(f"{field_name} must be at most 16 characters")
    # DICOM AE Titles are typically uppercase ASCII
    aet = aet.upper()
    for ch in aet:
        if ord(ch) < 32 or ord(ch) > 126:
            raise ValueError(f"{field_name} contains invalid characters")
    return aet


def _normalize_host(value) -> str:
    if value is None:
        raise ValueError("PACS_IP is required")
    host = str(value).strip()
    if not host:
        raise ValueError("PACS_IP is required")
    if len(host) > 255:
        raise ValueError("PACS_IP is too long")
    return host

# 自动清理配置
CLEANUP_THRESHOLD_GB = 50  # 50GB阈值
CLEANUP_TARGET_GB = 40     # 清理到40GB以下

def get_directory_size(directory):
    """计算目录总大小（GB）"""
    total_size = 0
    try:
        for dirpath, dirnames, filenames in os.walk(directory):
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                if os.path.exists(filepath):
                    total_size += os.path.getsize(filepath)
    except (OSError, IOError) as e:
        logger.warning(f"计算目录大小时出错: {str(e)}")
    return total_size / (1024 ** 3)  # 转换为GB

def cleanup_old_results():
    """清理旧的结果文件，保持磁盘空间在合理范围内"""
    results_dir = app.config['RESULT_FOLDER']
    current_size = get_directory_size(results_dir)
    
    if current_size < CLEANUP_THRESHOLD_GB:
        return
    
    logger.info(f"结果目录大小: {current_size:.2f}GB, 启动自动清理")
    
    # 获取所有子目录（任务目录和ZIP文件）
    items_to_check = []
    
    try:
        # 扫描所有文件和目录
        for item in os.listdir(results_dir):
            item_path = os.path.join(results_dir, item)
            if os.path.exists(item_path):
                # 获取最后访问时间
                atime = os.path.getatime(item_path)
                size = 0
                
                if os.path.isfile(item_path):
                    size = os.path.getsize(item_path) / (1024 ** 3)
                elif os.path.isdir(item_path):
                    size = get_directory_size(item_path)
                    
                items_to_check.append({
                    'path': item_path,
                    'name': item,
                    'atime': atime,
                    'size': size,
                    'is_dir': os.path.isdir(item_path)
                })
                
    except Exception as e:
        logger.error(f"扫描结果目录失败: {str(e)}")
        return
    
    # 排除正在进行的任务
    active_task_ids = [task.task_id for task in processing_tasks.values() 
                      if task.status in ['running', 'pending']]
    
    # 过滤掉正在进行的任务
    items_to_clean = []
    for item in items_to_check:
        # 检查是否为活跃任务目录
        is_active = False
        for task_id in active_task_ids:
            if task_id in item['name']:
                is_active = True
                break
        
        if not is_active:
            items_to_clean.append(item)
    
    if not items_to_clean:
        logger.info("所有文件都属于活跃任务，跳过清理")
        return
    
    # 按访问时间排序，先删除最旧的
    items_to_clean.sort(key=lambda x: x['atime'])
    
    cleaned_size = 0
    target_to_clean = current_size - CLEANUP_TARGET_GB
    
    for item in items_to_clean:
        if cleaned_size >= target_to_clean:
            break
            
        try:
            logger.info(f"删除: {item['name']} ({item['size']:.2f}GB)")
            
            if item['is_dir']:
                shutil.rmtree(item['path'])
            else:
                os.remove(item['path'])
                
            cleaned_size += item['size']
            
        except Exception as e:
            logger.error(f"删除 {item['name']} 失败: {str(e)}")
    
    final_size = get_directory_size(results_dir)
    logger.info(f"清理完成: {current_size:.2f}GB → {final_size:.2f}GB (清理了 {cleaned_size:.2f}GB)")

def check_and_cleanup_results():
    """检查并清理结果目录的后台任务"""
    def cleanup_thread():
        try:
            cleanup_old_results()
        except Exception as e:
            logger.error(f"自动清理失败: {str(e)}")
    
    # 异步执行清理，避免阻塞主线程
    threading.Thread(target=cleanup_thread, daemon=True).start()

class ProcessingTask:
    """处理任务类 - 修复版"""
    def __init__(self, task_id, task_type, parameters):
        self.task_id = task_id
        self.task_type = task_type
        self.parameters = parameters
        self.status = 'pending'
        self.progress = 0
        self.current_step = ''
        self.steps = []
        self.result = None
        self.error = None
        self.start_time = time.time()
        self.end_time = None
        self.logs = []

    def add_log(self, message, level='info'):
        """添加日志"""
        log_entry = {
            'timestamp': time.strftime('%H:%M:%S'),
            'level': level,
            'message': message
        }
        self.logs.append(log_entry)
        
        # 使用统一日志系统记录
        log_method = getattr(logger, level.lower(), logger.info)
        log_method(f"[Task {self.task_id}] {message}")
        
        # 通过WebSocket发送更新
        try:
            socketio.emit('task_update', {
                'task_id': self.task_id,
                'status': self.status,
                'progress': self.progress,
                'current_step': self.current_step,
                'logs': self.logs[-5:]  # 只发送最新5条日志
            })  # 默认广播给所有连接的客户端
        except Exception as e:
            logger.error(f"WebSocket发送失败: {str(e)}")

    def update_status(self, status, progress=None, step=None):
        """更新任务状态"""
        self.status = status
        if progress is not None:
            self.progress = progress
        if step is not None:
            self.current_step = step
        
        # 使用 logger 记录状态转换
        logger.info(f"Task {self.task_id} status update: {status} ({progress or 0}% - {step or 'N/A'})")
        
        # 通过WebSocket发送更新
        try:
            socketio.emit('task_update', {
                'task_id': self.task_id,
                'status': self.status,
                'progress': self.progress,
                'current_step': self.current_step,
                'logs': self.logs[-5:]  # 只发送最新5条日志
            })  # 默认广播给所有连接的客户端
        except Exception as e:
            logger.error(f"WebSocket发送失败: {str(e)}")

@app.route('/api/debug/test-connection')
def test_connection():
    """测试PACS连接的调试接口"""
    try:
        client = DICOMDownloadClient()
        status = client.check_status()
        
        return jsonify({
            'pacs_connected': status,
            'pacs_config': {
                'ip': client.pacs_config['PACS_IP'],
                'port': client.pacs_config['PACS_PORT'],
                'calling_aet': client.pacs_config['CALLING_AET'],
                'called_aet': client.pacs_config['CALLED_AET']
            },
            'message': 'PACS connection OK' if status else 'PACS connection failed'
        })
    except Exception as e:
        return jsonify({
            'error': str(e),
            'message': 'Error occurred during test connection'
        }), 500
# 路由定义
@app.route('/')
def index():
    """主页面"""
    return render_template('index.html')

@app.route('/api/process/single', methods=['POST'])
def process_single():
    """处理单个AccessionNumber"""
    logger.debug(f"process_single被调用，IP: {request.remote_addr}")
    try:
        data = request.json
        accession_number = data.get('accession_number')
        options = data.get('options', {})
        
        if not accession_number:
            return jsonify({'error': 'Please provide AccessionNumber'}), 400
        
        # 创建任务
        task_id = str(uuid.uuid4())
        task = ProcessingTask(task_id, 'single', {
            'accession_number': accession_number,
            'options': options
        })
        
        processing_tasks[task_id] = task
        
        # 启动后台处理
        threading.Thread(target=process_single_task, args=(task,)).start()
        
        return jsonify({
            'task_id': task_id,
            'status': 'started',
            'message': f'开始处理AccessionNumber: {accession_number}'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/process/batch', methods=['POST'])
def process_batch():
    """批量处理多个AccessionNumber"""
    try:
        data = request.json
        accession_numbers = data.get('accession_numbers', [])
        options = data.get('options', {})
        
        if not accession_numbers:
            return jsonify({'error': 'Please provide AccessionNumber list'}), 400
        
        # 创建任务
        task_id = str(uuid.uuid4())
        task = ProcessingTask(task_id, 'batch', {
            'accession_numbers': accession_numbers,
            'options': options
        })
        
        processing_tasks[task_id] = task
        
        # 启动后台处理
        threading.Thread(target=process_batch_task, args=(task,)).start()
        
        return jsonify({
            'task_id': task_id,
            'status': 'started',
            'message': f'开始批量处理 {len(accession_numbers)} 个研究'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/process/upload', methods=['POST'])
def process_upload():
    """处理上传的ZIP文件"""
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file uploaded'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        if not file.filename.lower().endswith('.zip'):
            return jsonify({'error': 'Only ZIP files are supported'}), 400
        
        # 保存上传的文件
        filename = secure_filename(file.filename)
        timestamp = int(time.time())
        filename = f"{timestamp}_{filename}"
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)
        
        # 获取处理选项
        options = {}
        for key in request.form:
            options[key] = request.form[key] == 'true'
        
        # 创建任务
        task_id = str(uuid.uuid4())
        task = ProcessingTask(task_id, 'upload', {
            'filepath': filepath,
            'filename': filename,
            'options': options
        })
        
        processing_tasks[task_id] = task
        
        # 启动后台处理
        threading.Thread(target=process_upload_task, args=(task,)).start()
        
        return jsonify({
            'task_id': task_id,
            'status': 'started',
            'message': f'开始处理上传文件: {file.filename}'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/task/<task_id>/status')
def get_task_status(task_id):
    """获取任务状态"""
    task = processing_tasks.get(task_id)
    if not task:
        return jsonify({'error': 'Task not found'}), 404
    
    return jsonify({
        'task_id': task.task_id,
        'status': task.status,
        'progress': task.progress,
        'current_step': task.current_step,
        'steps': task.steps,
        'logs': task.logs,
        'result': task.result,
        'error': task.error,
        'duration': (task.end_time or time.time()) - task.start_time
    })


def _serialize_task_history(task: 'ProcessingTask') -> dict:
    result = task.result or {}
    duration = (task.end_time or time.time()) - task.start_time
    summary = ''

    if task.task_type == 'single':
        summary = task.parameters.get('accession_number', '')
    elif task.task_type == 'batch':
        count = len(task.parameters.get('accession_numbers', []) or [])
        summary = f"{count} studies"
    elif task.task_type == 'upload':
        summary = task.parameters.get('filename', '')

    return {
        'task_id': task.task_id,
        'task_type': task.task_type,
        'status': task.status,
        'summary': summary,
        'start_time': task.start_time,
        'end_time': task.end_time,
        'duration': duration,
        'has_excel': bool(result.get('excel_file')),
        'has_zip': bool(result.get('result_zip') or result.get('zip_file')),
        'series_count': len(result.get('series_info', {}) or {}) if isinstance(result.get('series_info'), dict) else result.get('series_count', 0)
    }


@app.route('/api/tasks/history')
def get_task_history():
    """返回已完成任务的历史列表"""
    completed_tasks = [t for t in processing_tasks.values() if t.status == 'completed']
    completed_tasks.sort(key=lambda x: x.end_time or x.start_time, reverse=True)
    return jsonify({
        'tasks': [_serialize_task_history(task) for task in completed_tasks]
    })

@app.route('/api/task/<task_id>/cancel', methods=['POST'])
def cancel_task(task_id):
    """取消任务"""
    task = processing_tasks.get(task_id)
    if not task:
        return jsonify({'error': 'Task not found'}), 404
    
    if task.status in ['completed', 'failed', 'cancelled']:
        return jsonify({'error': 'Task completed, cannot cancel'}), 400
    
    task.update_status('cancelled')
    task.add_log('任务已被用户取消', 'warning')
    
    return jsonify({'message': 'Task cancelled'})

@app.route('/api/download/<task_id>/<file_type>')
def download_result(task_id, file_type):
    """下载处理结果"""
    task = processing_tasks.get(task_id)
    if not task or not task.result:
        return jsonify({'error': 'Result file not found'}), 404
    
    try:
        if file_type == 'excel' and 'excel_file' in task.result:
            return send_file(
                task.result['excel_file'],
                as_attachment=True,
                download_name=f"metadata_{task_id}.xlsx"
            )
        elif (file_type == 'zip' or file_type == 'result_zip') and (task.result.get('zip_file') or task.result.get('result_zip')):
            path = task.result.get('result_zip') or task.result.get('zip_file')
            return send_file(
                path,
                as_attachment=True,
                download_name=f"organized_{task_id}.zip"
            )
        else:
            return jsonify({'error': 'File type not supported'}), 400
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/system/status')
def system_status():
    """获取系统状态"""
    # 检查DICOM服务状态
    dicom_status = 'unknown'
    error_msg = None
    try:
        # 每次调用创建独立的客户端，避免持久连接状态异常
        client = DICOMDownloadClient()
        if client.check_status():
            dicom_status = 'connected'
        else:
            dicom_status = 'disconnected'
    except Exception as e:
        dicom_status = 'error'
        error_msg = str(e)
        logger.error(f"DICOM状态检查失败: {error_msg}")
    
    # 获取存储空间信息
    results_size_gb = get_directory_size(app.config['RESULT_FOLDER'])
    
    response = {
        'status': 'running',
        'active_tasks': len([t for t in processing_tasks.values() if t.status == 'running']),
        'total_tasks': len(processing_tasks),
        'dicom_service_status': dicom_status,
        'storage': {
            'results_size_gb': round(results_size_gb, 2),
            'cleanup_threshold_gb': CLEANUP_THRESHOLD_GB,
            'cleanup_needed': results_size_gb >= CLEANUP_THRESHOLD_GB
        }
    }
    
    if error_msg:
        response['dicom_error'] = error_msg
    
    return jsonify(response)


@app.route('/api/pacs-config', methods=['GET'])
def get_pacs_config():
    """Get current PACS configuration (from environment/defaults)."""
    defaults = {
        'PACS_IP': '172.17.250.192',
        'PACS_PORT': 2104,
        'CALLING_AET': 'WMX01',
        'CALLED_AET': 'pacsFIR',
        'CALLING_PORT': 1103,
    }

    def _get_env_int(key, default):
        try:
            return int(os.getenv(key, default))
        except (TypeError, ValueError):
            return default

    return jsonify({
        'PACS_IP': os.getenv('PACS_IP', defaults['PACS_IP']),
        'PACS_PORT': _get_env_int('PACS_PORT', defaults['PACS_PORT']),
        'CALLING_AET': os.getenv('CALLING_AET', defaults['CALLING_AET']),
        'CALLED_AET': os.getenv('CALLED_AET', defaults['CALLED_AET']),
        'CALLING_PORT': _get_env_int('CALLING_PORT', defaults['CALLING_PORT']),
    })


@app.route('/api/pacs-config', methods=['POST'])
def set_pacs_config():
    """Persist PACS configuration to .env and update process env for new connections."""
    global dicom_client_checker

    data = request.json or {}
    try:
        pacs_ip = _normalize_host(data.get('PACS_IP'))
        pacs_port = _parse_port(data.get('PACS_PORT'), 'PACS_PORT')
        calling_aet = _normalize_aet(data.get('CALLING_AET'), 'CALLING_AET')
        called_aet = _normalize_aet(data.get('CALLED_AET'), 'CALLING_AET')
        calling_port = _parse_port(data.get('CALLING_PORT'), 'CALLING_PORT')
    except ValueError as e:
        return jsonify({'error': str(e)}), 400

    try:
        # Write to .env
        os.makedirs(os.path.dirname(ENV_FILE_PATH), exist_ok=True)
        set_key(ENV_FILE_PATH, 'PACS_IP', pacs_ip)
        set_key(ENV_FILE_PATH, 'PACS_PORT', str(pacs_port))
        set_key(ENV_FILE_PATH, 'CALLING_AET', calling_aet)
        set_key(ENV_FILE_PATH, 'CALLED_AET', called_aet)
        set_key(ENV_FILE_PATH, 'CALLING_PORT', str(calling_port))

        # Update in-memory env so new client instances pick it up without restart
        os.environ['PACS_IP'] = pacs_ip
        os.environ['PACS_PORT'] = str(pacs_port)
        os.environ['CALLING_AET'] = calling_aet
        os.environ['CALLED_AET'] = called_aet
        os.environ['CALLING_PORT'] = str(calling_port)

        # Refresh global checker client
        dicom_client_checker = DICOMDownloadClient()

        return jsonify({'message': 'Configuration saved'}), 200
    except Exception as e:
        return jsonify({'error': f'Failed to save configuration: {str(e)}'}), 500

# 处理任务函数
def process_single_task(task):
    """处理单个AccessionNumber任务 - 修复版"""
    client_logged_in = False
    task_client = None
    
    try:
        # 立即更新状态，确保WebSocket发送
        task.update_status('running', 5, 'Connecting to DICOM service')
        task.add_log("Connecting to DICOM service...")
        
        # 添加调试日志
        logger.debug(f"开始处理任务: {task.task_id}")
        logger.debug(f"AccessionNumber: {task.parameters['accession_number']}")
        
        # 创建新的DICOM客户端实例
        try:
            task_client = DICOMDownloadClient()
            task.add_log("DICOM client created successfully")
        except Exception as e:
            task.add_log(f"Failed to create DICOM client: {str(e)}", 'error')
            raise Exception(f"Failed to create DICOM client: {str(e)}")
        
        # 检查PACS连接状态
        task.update_status('running', 8, 'Checking PACS connection')
        task.add_log("Checking PACS connection status...")
        
        try:
            if not task_client.check_status():
                raise Exception("PACS service unavailable, please check network and configuration")
            task.add_log("PACS connection normal")
        except Exception as e:
            task.add_log(f"PACS connection check failed: {str(e)}", 'error')
            raise
        
        # 自动登录（兼容性接口：当前DICOM客户端不做真实认证）
        task.update_status('running', 10, 'Logging in to DICOM service')
        task.add_log("Logging in to DICOM service...")
        
        try:
            username = os.getenv('DICOM_USERNAME', '')
            password = os.getenv('DICOM_PASSWORD', '')
            if not task_client.login(username, password):
                raise Exception("DICOM service login failed")
            client_logged_in = True
            task.add_log("DICOM service login successful")
        except Exception as e:
            task.add_log(f"Login failed: {str(e)}", 'error')
            raise

        # Attach progress callback so MR_clean can report progress back to task logs
        def _mr_progress(msg, stage=None):
            try:
                task.add_log(msg)
            except Exception:
                pass
            logger.info(f"MR_PROGRESS[{stage}]: {msg}")

        task_client.progress_callback = _mr_progress
        
        # 获取参数
        accession_number = task.parameters['accession_number']
        options = task.parameters.get('options', {})
        
        task.update_status('running', 15, 'Preparing process')
        task.add_log(f"Start processing AccessionNumber: {accession_number}")
        
        # 创建结果目录
        result_dir = os.path.join(app.config['RESULT_FOLDER'], task.task_id)
        os.makedirs(result_dir, exist_ok=True)
        task.add_log(f"Created result directory: {result_dir}")
        
        # 设置处理步骤
        task.steps = ['Connect Service', 'Query Data', 'Download Files', 'Organize Files', 'NIfTI Conversion', 'Extract Metadata']
        
        # 执行处理流程
        task.update_status('running', 30, 'Querying PACS data')
        task.add_log('Querying data from PACS...')
        
        # 先查询是否存在数据
        try:
            series_metadata = task_client._query_series_metadata(accession_number)
            if not series_metadata:
                raise Exception(f"AccessionNumber not found in PACS: {accession_number}")
            task.add_log(f"Found {len(series_metadata)} Series")
        except Exception as e:
            task.add_log(f"Query failed: {str(e)}", 'error')
            raise
        
        task.update_status('running', 40, 'Downloading DICOM files')
        task.add_log('Downloading DICOM files...')
        
        try:
            # 使用已登录的客户端处理
            results = task_client.process_complete_workflow(
                accession_number=accession_number,
                base_output_dir=result_dir,
                auto_extract=options.get('auto_extract', True),
                auto_organize=options.get('auto_organize', True),
                auto_metadata=options.get('auto_metadata', True),
                keep_zip=options.get('keep_zip', True),
                keep_extracted=options.get('keep_extracted', False),
                output_format=options.get('output_format', 'nifti'),
                parallel_pipeline=True
            )
            
            if results and results.get('success'):
                task.update_status('running', 90, 'Generating results')
                task.add_log('Process successful, generating results...')
                
                # 创建结果ZIP文件
                if results.get('organized_dir'):
                    task.add_log('Creating result ZIP...')
                    zip_path = create_result_zip(
                        results['organized_dir'],
                        task.task_id,
                        extra_files=[results.get('excel_file')]
                    )
                    results['result_zip'] = zip_path
                    task.add_log('Result ZIP created')
                
                task.result = results
                task.update_status('completed', 100, 'Completed')
                task.add_log('✅ Process completed successfully!', 'success')
                task.end_time = time.time()
                
                # 输出成功日志
                logger.debug(f"任务完成: {task.task_id}")
                
                # 任务完成后检查并清理结果目录
                check_and_cleanup_results()
                
            else:
                error_msg = results.get('error', 'Unknown error during process') if results else 'No result returned'
                task.add_log(f'Process failed: {error_msg}', 'error')
                task.update_status('failed')
                task.error = error_msg
                
        except Exception as e:
            task.add_log(f'Error during process: {str(e)}', 'error')
            raise
            
    except Exception as e:
        error_msg = str(e)
        logger.error(f"任务处理失败: {task.task_id}, 错误: {error_msg}")
        task.add_log(f'Process error: {error_msg}', 'error')
        task.update_status('failed')
        task.error = error_msg
        task.end_time = time.time()
    
    finally:
        # 确保登出
        if client_logged_in and task_client:
            try:
                task_client.logout()
                task.add_log("Logged out from DICOM service")
                logger.debug(f"已登出DICOM服务")
            except Exception as e:
                task.add_log(f"Error during logout: {str(e)}", 'warning')
                logger.warning(f"登出失败: {str(e)}")

def process_batch_task(task):
    """处理批量AccessionNumber任务"""
    client_logged_in = False
    task_client = None  # 初始化变量
    try:
        accession_numbers = task.parameters['accession_numbers']
        options = task.parameters['options']
        
        task.update_status('running', 5, 'Connecting to DICOM service')
        task.add_log("Connecting to DICOM service...")
        
        # 创建新的DICOM客户端实例并登录
        task_client = DICOMDownloadClient()
        
        # 自动登录（兼容性接口：当前DICOM客户端不做真实认证）
        task.add_log("Logging in to DICOM service...")
        username = os.getenv('DICOM_USERNAME', '')
        password = os.getenv('DICOM_PASSWORD', '')
        if not task_client.login(username, password):
            raise Exception("DICOM service login failed, please check service status")
        
        client_logged_in = True
        task.add_log("DICOM service login successful")
        # Attach progress callback so MR_clean can report progress back to task logs
        def _mr_progress(msg, stage=None):
            try:
                task.add_log(msg)
            except Exception:
                pass
            logger.info(f"MR_PROGRESS[{stage}]: {msg}")

        task_client.progress_callback = _mr_progress
        
        task.update_status('running', 10, 'Initializing batch process')
        task.add_log(f"Start batch processing {len(accession_numbers)} studies")
        
        results = []
        total = len(accession_numbers)
        
        for i, accno in enumerate(accession_numbers):
            # 计算进度 (10-90%用于处理，剩余用于整理)
            progress = 10 + int((i / total) * 80)
            task.update_status('running', progress, f'Processing {accno} ({i+1}/{total})')
            task.add_log(f'Processing study {i+1}/{total}: {accno}')
            
            # 创建单独的结果目录
            result_dir = os.path.join(app.config['RESULT_FOLDER'], task.task_id, accno)
            os.makedirs(result_dir, exist_ok=True)
            
            try:
                result = task_client.process_complete_workflow(
                    accession_number=accno,
                    base_output_dir=result_dir,
                    auto_extract=options.get('auto_extract', True),
                    auto_organize=options.get('auto_organize', True),
                    auto_metadata=options.get('auto_metadata', True),
                    output_format=options.get('output_format', 'nifti'),
                    parallel_pipeline=True
                )
                results.append(result)
                task.add_log(f'{accno} Process completed')
                
            except Exception as e:
                task.add_log(f'{accno} Process failed: {str(e)}', 'error')
                results.append({'accession_number': accno, 'error': str(e)})
        
        task.update_status('running', 95, 'Creating batch results')
        task.add_log("Creating batch result files...")
        
        # 合并结果
        batch_result_dir = os.path.join(app.config['RESULT_FOLDER'], task.task_id)
        zip_path = create_result_zip(batch_result_dir, f"batch_{task.task_id}")
        
        task.result = {
            'batch_results': results,
            'result_zip': zip_path,
            'total_processed': len([r for r in results if r.get('success')]),
            'total_failed': len([r for r in results if r.get('error')])
        }
        
        task.update_status('completed', 100, 'Batch process completed')
        task.add_log('Batch process completed')
        task.end_time = time.time()
        
        # 批量任务完成后检查并清理结果目录
        check_and_cleanup_results()
        
    except Exception as e:
        task.add_log(f'Batch process error: {str(e)}', 'error')
        task.update_status('failed')
        task.error = str(e)
        task.end_time = time.time()
    
    finally:
        # 确保登出
        if client_logged_in and task_client:
            try:
                task_client.logout()
                task.add_log("Logged out from DICOM service")
            except Exception as e:
                task.add_log(f"Error during logout: {str(e)}", 'warning')

def process_upload_task(task):
    """处理上传文件任务"""
    try:
        filepath = task.parameters['filepath']
        options = task.parameters['options']
        
        task.update_status('running', 5, 'Initializing upload process')
        task.add_log(f"Start processing uploaded file: {task.parameters['filename']}")
        
        # 创建本地DICOM客户端实例（无需登录，仅用于本地文件处理）
        local_client = DICOMDownloadClient()
        
        # 创建结果目录
        result_dir = os.path.join(app.config['RESULT_FOLDER'], task.task_id)
        os.makedirs(result_dir, exist_ok=True)
        
        task.steps = ['Extract Files', 'Organize Files', 'NIfTI Conversion', 'Extract Metadata']
        
        # 解压文件
        task.update_status('running', 20, 'Extracting files')
        task.add_log("Extracting uploaded ZIP file...")
        extract_dir = local_client.extract_zip(filepath, os.path.join(result_dir, 'extracted'))
        
        # 初始化变量，确保在结果字典中可用
        organized_dir = extract_dir
        series_info = {}
        excel_file = None
        
        if extract_dir:
            task.add_log('File extraction completed')
            
            if options.get('auto_organize', True):
                # 整理文件
                task.update_status('running', 50, 'Organizing files')
                task.add_log("Organizing DICOM files by series...")
                organized_dir, series_info = local_client.organize_dicom_files(
                    extract_dir, 
                    output_format=options.get('output_format', 'nifti')
                )
                task.add_log(f'File organization completed, found {len(series_info)} series')
                
                if options.get('auto_metadata', True):
                    # 提取元数据
                    task.update_status('running', 80, 'Extracting metadata')
                    task.add_log("Extracting DICOM metadata...")
                    excel_file = local_client.extract_dicom_metadata(organized_dir)
                    task.add_log('Metadata extraction completed')
            else:
                # organized_dir, series_info, excel_file 已在上面初始化
                pass
            
            task.update_status('running', 95, 'Creating result files')
            task.add_log("Creating result files...")
            zip_path = create_result_zip(result_dir, task.task_id)
            
            task.result = {
                'extract_dir': extract_dir,
                'organized_dir': organized_dir,
                'excel_file': excel_file,
                'result_zip': zip_path,
                'series_count': len(series_info)
            }
            
            task.update_status('completed', 100, 'Upload process completed')
            task.add_log('Upload process completed')
            task.end_time = time.time()
            
            # 上传文件处理完成后检查并清理结果目录
            check_and_cleanup_results()
            
        else:
            task.add_log('File extraction failed', 'error')
            task.update_status('failed')
            task.error = 'Failed to extract uploaded file'
            
    except Exception as e:
        task.add_log(f'Upload process error: {str(e)}', 'error')
        task.update_status('failed')
        task.error = str(e)
        task.end_time = time.time()

def create_result_zip(source_dir, task_id, extra_files=None):
    """创建结果ZIP文件。

    extra_files: 额外需要打包的文件路径列表（会放在 ZIP 根目录）。
    """
    zip_path = os.path.join(app.config['RESULT_FOLDER'], f"result_{task_id}.zip")
    
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(source_dir):
            for file in files:
                file_path = os.path.join(root, file)
                arc_name = os.path.relpath(file_path, source_dir)
                zipf.write(file_path, arc_name)

        if extra_files:
            for extra_path in extra_files:
                if not extra_path or not os.path.exists(extra_path):
                    continue
                # 如果已经在 source_dir 内，跳过（避免重复）
                try:
                    base_dir = os.path.abspath(source_dir)
                    extra_abs = os.path.abspath(extra_path)
                    if os.path.commonpath([base_dir, extra_abs]) == base_dir:
                        continue
                except Exception:
                    pass
                arc_name = os.path.basename(extra_path)
                zipf.write(extra_path, arc_name)
    
    return zip_path

# WebSocket事件处理
@socketio.on('connect')
def handle_connect():
    logger.info('客户端已连接')

@socketio.on('disconnect')
def handle_disconnect():
    logger.info('客户端已断开')

@socketio.on('subscribe_task')
def handle_subscribe_task(data):
    """订阅任务更新"""
    task_id = data.get('task_id')
    if task_id in processing_tasks:
        emit('task_subscribed', {'task_id': task_id})

if __name__ == '__main__':
    logger.info("="*60)
    logger.info("🏥 DICOM处理Web应用启动")
    logger.info("="*60)
    logger.info("📡 访问地址: http://172.17.250.136:5005")
    
    # 检查DICOM服务连接状态
    try:
        checker = DICOMDownloadClient()
        if checker.check_status():
            logger.info("✅ PACS服务连接正常")
            logger.info(f"   - PACS IP: {checker.pacs_config['PACS_IP']}")
            logger.info(f"   - PACS Port: {checker.pacs_config['PACS_PORT']}")
            logger.info(f"   - Calling AET: {checker.pacs_config['CALLING_AET']}")
            logger.info(f"   - Called AET: {checker.pacs_config['CALLED_AET']}")
        else:
            logger.warning("⚠️  PACS服务连接异常，下载功能可能不可用")
    except Exception as e:
        logger.error(f"⚠️  无法连接PACS服务: {str(e)}")
        logger.info("   仅支持本地文件上传处理")
    
    logger.info("="*60)
    if os.getenv('DICOM_USERNAME') or os.getenv('DICOM_PASSWORD'):
        logger.info("🔐 已从环境变量读取DICOM登录信息")
    else:
        logger.info("🔐 未配置DICOM登录信息（当前实现无需真实认证）")
    logger.info("🚀 系统已就绪，等待用户请求...")
    logger.info("📡 测试URL:")
    logger.info("   - http://localhost:5005")
    logger.info("   - http://127.0.0.1:5005") 
    logger.info("   - http://172.17.250.136:5005")
    logger.info("="*60)
    
    # 启动应用，开启调试模式和自动重载
    socketio.run(app, host='0.0.0.0', port=5005, debug=False, allow_unsafe_werkzeug=True)