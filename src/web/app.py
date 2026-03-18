# -*- coding: utf-8 -*-
#!/usr/bin/env python3
"""
Flask Web 应用主模块

提供 DICOM 处理 Web 服务和 REST API 接口，支持：
- PACS 配置管理
- 单条/批量任务处理
- 文件上传处理
- WebSocket 实时通信
"""

import os
import sys

# 将项目根目录添加到 Python 路径（确保能找到 src 模块）
# 从 src/web/app.py 向上两级到达项目根目录
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Flask/Jinja2 3.0+ 兼容性修复
# 在导入 Flask 之前先修复所有兼容性问题
try:
    from markupsafe import Markup, escape
    import sys
    # 创建 jinja2 模块的兼容性层
    import jinja2
    jinja2.Markup = Markup
    jinja2.escape = escape
    sys.modules['jinja2'].Markup = Markup
    sys.modules['jinja2'].escape = escape
except ImportError:
    pass

from flask import Flask, render_template, request, jsonify, send_file
from flask_socketio import SocketIO, emit
import time
import uuid
import threading
import shutil
import logging
from logging.handlers import RotatingFileHandler
from werkzeug.utils import secure_filename

from dotenv import set_key
import secrets

# 导入我们的DICOM处理客户端
from src.client.unified import DICOMDownloadClient
from src.utils.packaging import create_result_zip

def get_base_path():
    """获取程序运行时的根目录路径，兼容 PyInstaller 打包"""
    if hasattr(sys, '_MEIPASS'):
        return sys._MEIPASS
    return os.path.abspath(".")

def get_project_root():
    """获取项目根目录路径（从 src/web/ 向上两级）"""
    current_file = os.path.abspath(__file__)
    # src/web/app.py -> 向上两级到达项目根目录
    return os.path.dirname(os.path.dirname(os.path.dirname(current_file)))

# Flask应用配置 - 指定静态文件和模板路径（从项目根目录查找）
project_root = get_project_root()
app = Flask(__name__,
            static_folder=os.path.join(project_root, 'static'),
            template_folder=os.path.join(project_root, 'templates'))

_secret_key = os.environ.get('FLASK_SECRET_KEY')
if not _secret_key:
    # 仅用于本地/临时运行；生产环境请通过环境变量提供固定值
    _secret_key = secrets.token_hex(32)
app.config['SECRET_KEY'] = _secret_key
app.config['UPLOAD_FOLDER'] = os.path.abspath('./uploads')
app.config['RESULT_FOLDER'] = os.path.abspath('./results')
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
_task_lock = threading.Lock()  # 任务字典锁，防止竞态条件

# 已完成任务缓存，避免每次请求全量序列化
_completed_tasks_lock = threading.Lock()
_completed_tasks_cache = []
_completed_task_ids = set()
_serialized_history_cache = []

# 任务取消标志字典 - 用于真正中断任务执行
_task_cancel_flags = {}

# 任务清理配置
TASK_MAX_AGE_HOURS = 24  # 任务最大保留时间（小时）
TASK_CLEANUP_INTERVAL = 3600  # 清理检查间隔（秒）


def _cleanup_old_tasks():
    """后台线程：定期清理过期任务"""
    while True:
        try:
            time.sleep(TASK_CLEANUP_INTERVAL)
            _do_cleanup_old_tasks()
        except Exception as e:
            logger.error(f"任务清理失败: {e}")


def _do_cleanup_old_tasks():
    """执行清理过期任务"""
    cutoff_time = time.time() - (TASK_MAX_AGE_HOURS * 3600)
    cleaned_count = 0
    
    with _task_lock:
        # 找出过期任务
        expired_tasks = []
        for task_id, task in processing_tasks.items():
            if task.end_time and task.end_time < cutoff_time:
                expired_tasks.append(task_id)
        
        # 删除过期任务
        for task_id in expired_tasks:
            del processing_tasks[task_id]
            _task_cancel_flags.pop(task_id, None)
            cleaned_count += 1
    
    if cleaned_count > 0:
        logger.info(f"清理了 {cleaned_count} 个过期任务（超过 {TASK_MAX_AGE_HOURS} 小时）")


# 启动后台清理线程
cleanup_thread = threading.Thread(target=_cleanup_old_tasks, daemon=True)
cleanup_thread.start()

# 创建DICOM客户端实例用于系统状态检查（不登录）
dicom_client_checker = DICOMDownloadClient()

ENV_FILE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), '.env')


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

# 自动清理配置（从 .env 读取，若不存在则写回 .env）
try:
    CLEANUP_THRESHOLD_GB = float(os.getenv('CLEANUP_THRESHOLD_GB', '50'))
except Exception:
    CLEANUP_THRESHOLD_GB = 50.0

try:
    CLEANUP_TARGET_GB = float(os.getenv('CLEANUP_TARGET_GB', '40'))
except Exception:
    CLEANUP_TARGET_GB = 40.0

# 将读取到的默认值持久化到 .env，便于用户修改与持久化配置
try:
    # 写入整数值时保留整型格式，浮点数保留原样
    thr_val = str(int(CLEANUP_THRESHOLD_GB) if float(CLEANUP_THRESHOLD_GB).is_integer() else CLEANUP_THRESHOLD_GB)
    tgt_val = str(int(CLEANUP_TARGET_GB) if float(CLEANUP_TARGET_GB).is_integer() else CLEANUP_TARGET_GB)
    set_key(ENV_FILE_PATH, 'CLEANUP_THRESHOLD_GB', thr_val)
    set_key(ENV_FILE_PATH, 'CLEANUP_TARGET_GB', tgt_val)
except Exception as e:
    logger.warning(f"无法将清理阈值写入 {ENV_FILE_PATH}: {e}")

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
        self._cancelled = False  # 取消标志
        self._log_buffer = []     # 日志缓冲区，用于批量发送
        self._last_emit_time = 0  # 上次发送时间
        self._emit_interval = 0.5  # 最小发送间隔（秒）

    def is_cancelled(self):
        """检查任务是否被取消"""
        # 检查本地标志
        if self._cancelled:
            return True
        # 检查全局取消标志
        return _task_cancel_flags.get(self.task_id, False)

    def cancel(self):
        """标记任务为已取消"""
        self._cancelled = True
        _task_cancel_flags[self.task_id] = True
        self.status = 'cancelled'
        self.end_time = time.time()

    def check_cancellation(self, step_name=""):
        """检查取消标志，如已取消则抛出异常中断执行"""
        if self.is_cancelled():
            raise InterruptedError(f"Task {self.task_id} cancelled at step: {step_name}")

    def _should_emit(self):
        """判断是否应该发送WebSocket更新（节流控制）"""
        current_time = time.time()
        if current_time - self._last_emit_time >= self._emit_interval:
            self._last_emit_time = current_time
            return True
        return False

    def _emit_update(self, force=False):
        """发送WebSocket更新（内部方法）"""
        if not force and not self._should_emit():
            return
        
        try:
            socketio.emit('task_update', {
                'task_id': self.task_id,
                'status': self.status,
                'progress': self.progress,
                'current_step': self.current_step,
                'logs': self.logs[-5:]  # 只发送最新5条日志
            })
        except Exception as e:
            logger.error(f"WebSocket发送失败: {str(e)}")

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
        
        # 通过WebSocket发送更新（节流控制）
        self._emit_update()

    def update_status(self, status, progress=None, step=None):
        """更新任务状态"""
        # 如果任务已取消，不再更新状态
        if self.is_cancelled() and status not in ['cancelled', 'failed']:
            logger.warning(f"Task {self.task_id} is cancelled, ignoring status update to {status}")
            return
        
        self.status = status
        if progress is not None:
            self.progress = progress
        if step is not None:
            self.current_step = step
        
        # 使用 logger 记录状态转换
        logger.info(f"Task {self.task_id} status update: {status} ({progress or 0}% - {step or 'N/A'})")
        
        # 通过WebSocket发送更新（强制发送状态变更）
        self._emit_update(force=True)

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


def _parse_pagination_param(value, default, min_value=1, max_value=None):
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default

    if parsed < min_value:
        parsed = min_value
    if max_value is not None and parsed > max_value:
        parsed = max_value
    return parsed


def _refresh_completed_cache_from_tasks():
    completed_tasks = [t for t in processing_tasks.values() if t.status == 'completed']
    completed_tasks.sort(key=lambda x: x.end_time or x.start_time, reverse=True)
    with _completed_tasks_lock:
        _completed_tasks_cache.clear()
        _completed_tasks_cache.extend(completed_tasks)
        _completed_task_ids.clear()
        _completed_task_ids.update(t.task_id for t in completed_tasks)
        _serialized_history_cache.clear()
        _serialized_history_cache.extend([_serialize_task_history(task) for task in completed_tasks])


def _record_task_completion(task: 'ProcessingTask'):
    with _completed_tasks_lock:
        if task.task_id in _completed_task_ids:
            return
        _completed_task_ids.add(task.task_id)
        _completed_tasks_cache.insert(0, task)
        _serialized_history_cache.insert(0, _serialize_task_history(task))


@app.route('/api/tasks/history')
def get_task_history():
    """返回已完成任务的历史列表"""
    page = _parse_pagination_param(request.args.get('page'), 1, min_value=1)
    page_size = _parse_pagination_param(request.args.get('page_size'), 20, min_value=1, max_value=200)

    if not _serialized_history_cache:
        _refresh_completed_cache_from_tasks()

    total = len(_serialized_history_cache)
    total_pages = (total + page_size - 1) // page_size if total > 0 else 0
    if total_pages > 0 and page > total_pages:
        page = total_pages

    start = (page - 1) * page_size
    end = start + page_size
    paged_tasks = _serialized_history_cache[start:end]

    return jsonify({
        'tasks': paged_tasks,
        'page': page,
        'page_size': page_size,
        'total': total,
        'total_pages': total_pages
    })

@app.route('/api/task/<task_id>/cancel', methods=['POST'])
def cancel_task(task_id):
    """取消任务 - 修复版，真正中断任务执行"""
    task = processing_tasks.get(task_id)
    if not task:
        return jsonify({'error': 'Task not found'}), 404
    
    if task.status in ['completed', 'failed', 'cancelled']:
        return jsonify({'error': 'Task completed, cannot cancel'}), 400
    
    # 设置取消标志，任务线程会在检查点抛出InterruptedError
    task.cancel()
    task.add_log('任务已被用户取消', 'warning')
    
    # 强制发送更新
    task._emit_update(force=True)
    
    return jsonify({'message': 'Task cancelled'})

@app.route('/api/download/<task_id>/<file_type>')
def download_result(task_id, file_type):
    """下载处理结果"""
    task = processing_tasks.get(task_id)
    if not task or not task.result:
        logger.warning(f"下载请求失败: 任务不存在或无结果 - task_id={task_id}")
        return jsonify({'error': 'Result file not found'}), 404
    
    try:
        if file_type == 'excel' and 'excel_file' in task.result:
            file_path = task.result['excel_file']
            logger.info(f"下载Excel文件: {file_path}")
            if not file_path or not os.path.exists(file_path):
                logger.error(f"Excel文件不存在: {file_path}")
                return jsonify({'error': f'File not found: {file_path}'}), 404
            
            # Windows下使用pathlib处理路径
            from pathlib import Path
            file_path = str(Path(file_path).resolve())
            
            return send_file(
                file_path,
                as_attachment=True,
                download_name=f"metadata_{task_id}.xlsx",
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
        elif (file_type == 'zip' or file_type == 'result_zip') and (task.result.get('zip_file') or task.result.get('result_zip')):
            path = task.result.get('result_zip') or task.result.get('zip_file')
            logger.info(f"下载ZIP文件: path={path}, exists={os.path.exists(path) if path else False}")
            
            if not path:
                logger.error("ZIP路径为空")
                return jsonify({'error': 'ZIP path is empty'}), 404
            
            # Windows下使用pathlib处理路径
            from pathlib import Path
            abs_path = str(Path(path).resolve())
            logger.info(f"绝对路径: {abs_path}, exists={os.path.exists(abs_path)}")
            
            if not os.path.exists(abs_path):
                logger.error(f"ZIP文件不存在: {abs_path}")
                # 尝试列出results目录内容帮助调试
                try:
                    result_dir = app.config['RESULT_FOLDER']
                    files = os.listdir(result_dir)
                    logger.info(f"Results目录内容: {files}")
                except Exception as list_e:
                    logger.error(f"无法列出results目录: {list_e}")
                return jsonify({'error': f'ZIP file not found: {abs_path}'}), 404
            
            # 检查文件大小
            try:
                file_size = os.path.getsize(abs_path)
                logger.info(f"ZIP文件大小: {file_size} bytes")
                if file_size == 0:
                    logger.error("ZIP文件大小为0")
                    return jsonify({'error': 'ZIP file is empty'}), 500
            except Exception as size_e:
                logger.error(f"无法获取文件大小: {size_e}")
            
            logger.info(f"开始发送文件: {abs_path}")
            response = send_file(
                abs_path,
                as_attachment=True,
                download_name=f"organized_{task_id}.zip",
                mimetype='application/zip'
            )
            logger.info(f"文件发送成功: {abs_path}")
            return response
        else:
            logger.warning(f"不支持的文件类型: {file_type}, result keys={list(task.result.keys())}")
            return jsonify({'error': 'File type not supported'}), 400
            
    except Exception as e:
        logger.error(f"下载文件时出错: {type(e).__name__}: {e}", exc_info=True)
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return jsonify({'error': f'{type(e).__name__}: {str(e)}'}), 500

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
    """处理单个AccessionNumber任务 - 修复版，支持取消检查"""
    client_logged_in = False
    task_client = None
    
    try:
        # 立即更新状态，确保WebSocket发送
        task.update_status('running', 5, 'Connecting to DICOM service')
        task.add_log("Connecting to DICOM service...")
        
        # 检查取消标志
        task.check_cancellation("initial_connect")
        
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
        
        # 检查取消标志
        task.check_cancellation("after_client_create")
        
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
        
        # 检查取消标志
        task.check_cancellation("after_pacs_check")
        
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
        
        # Attach download progress callback to update task status during C-MOVE
        def _download_progress(current_series, total_series, series_name, progress_pct):
            try:
                # 检查取消标志
                if task.is_cancelled():
                    raise InterruptedError("Task cancelled during download")
                
                step_name = f"Downloading series {current_series}/{total_series}: {series_name}"
                task.update_status('running', progress_pct, step_name)
                if current_series % 3 == 0 or current_series == total_series:  # 每3个series记录一次日志，避免日志过多
                    task.add_log(f"Downloaded {current_series}/{total_series} series: {series_name}")
            except InterruptedError:
                raise
            except Exception as e:
                logger.warning(f"Download progress callback error: {e}")
        
        task_client.download_progress_callback = _download_progress
        
        # 获取参数
        accession_number = task.parameters['accession_number']
        options = task.parameters.get('options', {})

        # 获取过滤参数
        modality_filter = options.get('modality_filter')
        min_series_files = options.get('min_series_files')
        exclude_derived = options.get('exclude_derived', True)  # 默认启用衍生序列过滤
        if min_series_files is not None:
            try:
                min_series_files = int(min_series_files)
            except (ValueError, TypeError):
                min_series_files = None

        task.update_status('running', 15, 'Preparing process')
        task.add_log(f"Start processing AccessionNumber: {accession_number}")
        
        # 检查取消标志
        task.check_cancellation("before_create_dir")
        
        # 创建结果目录
        result_dir = os.path.join(app.config['RESULT_FOLDER'], task.task_id)
        os.makedirs(result_dir, exist_ok=True)
        task.add_log(f"Created result directory: {result_dir}")
        
        # 设置处理步骤
        task.steps = ['Connect Service', 'Query Data', 'Download Files', 'Organize Files', 'NIfTI Conversion', 'Extract Metadata']
        
        # 执行处理流程
        task.update_status('running', 30, 'Querying PACS data')
        task.add_log('Querying data from PACS...')
        
        # 检查取消标志
        task.check_cancellation("before_query")
        
        # 先查询是否存在数据（应用过滤条件）
        try:
            series_metadata = task_client._query_series_metadata(
                accession_number,
                modality_filter=modality_filter,
                min_series_files=min_series_files,
                exclude_derived=exclude_derived
            )
            if not series_metadata:
                filter_info = []
                if modality_filter:
                    filter_info.append(f"Modality filter: {modality_filter}")
                if min_series_files:
                    filter_info.append(f"Min files: {min_series_files}")
                if exclude_derived:
                    filter_info.append(f"Exclude derived")
                filter_str = f" ({', '.join(filter_info)})" if filter_info else ""
                raise Exception(f"No series found in PACS for AccessionNumber: {accession_number}{filter_str}")
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
                parallel_pipeline=False,  # 禁用并行流水线，使用单线程顺序处理
                modality_filter=modality_filter,
                min_series_files=min_series_files,
                exclude_derived=exclude_derived
            )
            
            if results and results.get('success'):
                task.update_status('running', 90, 'Generating results')
                task.add_log('Process successful, generating results...')
                
                # 创建结果ZIP文件
                if results.get('organized_dir'):
                    task.add_log('Creating result ZIP...')
                    logger.info(f"Creating ZIP: source={results['organized_dir']}, task_id={task.task_id}, result_dir={app.config['RESULT_FOLDER']}")
                    try:
                        zip_path = create_result_zip(
                            results['organized_dir'],
                            task.task_id,
                            app.config['RESULT_FOLDER'],
                            extra_files=[results.get('excel_file')]
                        )
                        results['result_zip'] = zip_path
                        task.add_log(f'Result ZIP created: {zip_path}')
                        logger.info(f"ZIP created successfully: {zip_path}, exists={os.path.exists(zip_path)}")
                    except Exception as zip_e:
                        logger.error(f"Failed to create ZIP: {zip_e}", exc_info=True)
                        task.add_log(f'Failed to create ZIP: {str(zip_e)}', 'error')
                        raise
                
                task.result = results
                task.update_status('completed', 100, 'Completed')
                task.add_log('✅ Process completed successfully!', 'success')
                task.end_time = time.time()
                _record_task_completion(task)
                
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
            
    except InterruptedError:
        # 任务被取消，不要标记为失败
        task.add_log('Process cancelled by user', 'warning')
        # 状态已在cancel()方法中设置
        logger.info(f"任务被取消: {task.task_id}")
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
    """处理批量AccessionNumber任务 - 修复版，支持去重和取消检查"""
    client_logged_in = False
    task_client = None  # 初始化变量
    try:
        accession_numbers = task.parameters['accession_numbers']
        options = task.parameters.get('options', {})

        # 获取过滤参数
        modality_filter = options.get('modality_filter')
        min_series_files = options.get('min_series_files')
        exclude_derived = options.get('exclude_derived', True)  # 默认启用衍生序列过滤
        if min_series_files is not None:
            try:
                min_series_files = int(min_series_files)
            except (ValueError, TypeError):
                min_series_files = None
        if modality_filter:
            task.add_log(f"Modality filter: {modality_filter}")
        if min_series_files:
            task.add_log(f"Min series files: {min_series_files}")
        if exclude_derived:
            task.add_log(f"Exclude derived series: enabled")

        # 去重处理，保持顺序
        seen = set()
        unique_accession_numbers = []
        for acc in accession_numbers:
            if acc and acc not in seen:
                seen.add(acc)
                unique_accession_numbers.append(acc)

        if len(unique_accession_numbers) < len(accession_numbers):
            task.add_log(f"Removed {len(accession_numbers) - len(unique_accession_numbers)} duplicates, {len(unique_accession_numbers)} unique studies to process")
        
        accession_numbers = unique_accession_numbers
        
        task.update_status('running', 5, 'Connecting to DICOM service')
        task.add_log("Connecting to DICOM service...")
        
        # 检查取消标志
        task.check_cancellation("before_connect")
        
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
            # 检查取消标志
            task.check_cancellation(f"before_processing_{accno}")
            
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
                    parallel_pipeline=False,  # 禁用并行流水线，使用单线程顺序处理
                    modality_filter=modality_filter,
                    min_series_files=min_series_files,
                    exclude_derived=exclude_derived
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
        zip_path = create_result_zip(
            batch_result_dir,
            f"batch_{task.task_id}",
            app.config['RESULT_FOLDER']
        )
        
        # 计算详细的批处理统计信息
        total_processed = len([r for r in results if r.get('success')])
        total_failed = len([r for r in results if r.get('error')])
        
        # 收集质量统计数据
        quality_stats = {
            'normal': 0,
            'low_quality': 0,
            'fixed': 0,
            'unknown': 0
        }
        total_images = 0
        total_series = 0
        
        for r in results:
            if r.get('success'):
                series_info = r.get('series_info', {})
                total_series += len(series_info)
                
                # 尝试从Excel元数据文件中读取质量统计
                excel_file = r.get('excel_file')
                if excel_file and os.path.exists(excel_file):
                    try:
                        import pandas as pd
                        df = pd.read_excel(excel_file)
                        if 'Low_quality' in df.columns:
                            for _, row in df.iterrows():
                                low_quality = row.get('Low_quality', 0)
                                fixed = row.get('Fixed', '')
                                if fixed == 'Yes' or 'Fixed' in str(row.get('Low_quality_reason', '')):
                                    quality_stats['fixed'] += 1
                                elif low_quality == 0 or low_quality == False:
                                    quality_stats['normal'] += 1
                                elif low_quality == 1 or low_quality == True:
                                    quality_stats['low_quality'] += 1
                                else:
                                    quality_stats['unknown'] += 1
                                total_images += 1
                        else:
                            # 如果没有质量列，只统计文件数量
                            total_images += len(df)
                            quality_stats['unknown'] += len(df)
                    except Exception as e:
                        # 如果读取Excel失败，使用series_info统计
                        for series_name, series_data in series_info.items():
                            file_count = series_data.get('file_count', 0)
                            total_images += file_count
                            quality_stats['unknown'] += file_count
                else:
                    # 如果没有Excel文件，使用series_info统计
                    for series_name, series_data in series_info.items():
                        file_count = series_data.get('file_count', 0)
                        total_images += file_count
                        quality_stats['unknown'] += file_count
        
        # 计算处理时间
        duration = (task.end_time or time.time()) - task.start_time
        avg_speed = total_images / duration if duration > 0 else 0
        
        task.result = {
            'batch_results': results,
            'result_zip': zip_path,
            'total_processed': total_processed,
            'total_failed': total_failed,
            'total_studies': len(accession_numbers),
            'total_series': total_series,
            'total_images': total_images,
            'duration': round(duration, 2),
            'avg_speed': round(avg_speed, 2),
            'quality_distribution': quality_stats
        }
        
        task.update_status('completed', 100, 'Batch process completed')
        task.add_log('Batch process completed')
        task.end_time = time.time()
        _record_task_completion(task)
        
        # 批量任务完成后检查并清理结果目录
        check_and_cleanup_results()
        
    except InterruptedError:
        # 任务被取消，已在上面的循环中处理
        task.add_log('Batch process cancelled by user', 'warning')
        task.update_status('cancelled')
        task.end_time = time.time()
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
    """处理上传文件任务 - 修复版，支持取消检查"""
    try:
        filepath = task.parameters['filepath']
        options = task.parameters['options']
        
        task.update_status('running', 5, 'Initializing upload process')
        task.add_log(f"Start processing uploaded file: {task.parameters['filename']}")
        
        # 检查取消标志
        task.check_cancellation("initial")
        
        # 创建本地DICOM客户端实例（无需登录，仅用于本地文件处理）
        local_client = DICOMDownloadClient()
        
        # 检查取消标志
        task.check_cancellation("after_client_create")
        
        # 创建结果目录
        result_dir = os.path.join(app.config['RESULT_FOLDER'], task.task_id)
        os.makedirs(result_dir, exist_ok=True)
        
        task.steps = ['Extract Files', 'Organize Files', 'NIfTI Conversion', 'Extract Metadata']

        # 处理上传流程
        task.update_status('running', 20, 'Processing upload workflow')
        task.add_log("Processing uploaded ZIP file...")
        
        # 检查取消标志
        task.check_cancellation("before_processing")
        
        result = local_client.process_upload_workflow(filepath, result_dir, options)

        if result.get('success'):
            organized_dir = result.get('organized_dir')
            series_info = result.get('series_info', {})
            excel_file = result.get('excel_file')

            # 检查取消标志
            task.check_cancellation("before_create_zip")
            
            task.update_status('running', 95, 'Creating result files')
            task.add_log("Creating result files...")
            zip_path = create_result_zip(
                result_dir,
                task.task_id,
                app.config['RESULT_FOLDER']
            )

            task.result = {
                'extract_dir': result.get('extract_dir'),
                'organized_dir': organized_dir,
                'excel_file': excel_file,
                'result_zip': zip_path,
                'series_count': len(series_info)
            }

            task.update_status('completed', 100, 'Upload process completed')
            task.add_log('Upload process completed')
            task.end_time = time.time()
            _record_task_completion(task)

            # 上传文件处理完成后检查并清理结果目录
            check_and_cleanup_results()
        else:
            task.add_log('Upload processing failed', 'error')
            task.update_status('failed')
            task.error = result.get('error') or 'Failed to process uploaded file'
            
    except InterruptedError:
        # 任务被取消
        task.add_log('Upload process cancelled by user', 'warning')
        task.update_status('cancelled')
        task.end_time = time.time()
    except Exception as e:
        task.add_log(f'Upload process error: {str(e)}', 'error')
        task.update_status('failed')
        task.error = str(e)
        task.end_time = time.time()


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
