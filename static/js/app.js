// DICOM处理系统 - 前端JavaScript应用

class DICOMProcessor {
    constructor() {
        this.socket = null;
        this.currentTask = null;
        this.selectedFile = null;
        this.pacsConfigLoaded = false;
        this.successModal = null;
        this.errorModal = null;
        this.isHandlingTaskCompletion = false;
        this.historyPage = 1;
        this.historyPageSize = 20;
        this.historyTotalPages = 0;
        this.historyTotalCount = 0;
        
        // 防止重复提交的标志
        this.isProcessing = false;
        
        // 防抖包装的处理方法
        this.startSingleProcess = this.debounce(this._startSingleProcess.bind(this), 500, true);
        this.startBatchProcess = this.debounce(this._startBatchProcess.bind(this), 500, true);
        this.startUploadProcess = this.debounce(this._startUploadProcess.bind(this), 500, true);
        
        this.init();
    }

    // 带立即执行选项的防抖函数
    debounce(func, wait, immediate = false) {
        let timeout;
        return function executedFunction(...args) {
            const context = this;
            const later = () => {
                timeout = null;
                if (!immediate) func.apply(context, args);
            };
            const callNow = immediate && !timeout;
            clearTimeout(timeout);
            timeout = setTimeout(later, wait);
            if (callNow) func.apply(context, args);
        };
    }

    // 初始化应用
    init() {
        this.initLocalization();
        this.initModals();
        this.initializeSocket();
        this.bindEvents();
        this.updateCurrentTime();
        this.loadSystemStatus();
        this.loadFilterKeywords();
        
        // 防抖包装的方法
        this.debouncedLoadSystemStatus = this.debounce(() => this.loadSystemStatus(), 1000);
        
        // 设置定时器
        setInterval(() => this.updateCurrentTime(), 1000);
        setInterval(() => this.debouncedLoadSystemStatus(), 30000);
        
        console.log('🏥 DICOM处理系统已初始化');
    }

    initModals() {
        const successModalEl = document.getElementById('successModal');
        const errorModalEl = document.getElementById('errorModal');

        if (successModalEl) {
            this.successModal = bootstrap.Modal.getOrCreateInstance(successModalEl);
            successModalEl.addEventListener('hidden.bs.modal', () => this.cleanupModalArtifacts());
        }

        if (errorModalEl) {
            this.errorModal = bootstrap.Modal.getOrCreateInstance(errorModalEl);
            errorModalEl.addEventListener('hidden.bs.modal', () => this.cleanupModalArtifacts());
        }
    }

    cleanupModalArtifacts() {
        const hasVisibleModal = document.querySelector('.modal.show');
        if (!hasVisibleModal) {
            document.querySelectorAll('.modal-backdrop').forEach(backdrop => backdrop.remove());
            document.body.classList.remove('modal-open');
            document.body.style.removeProperty('padding-right');
        }
    }

    // 防抖函数
    debounce(func, wait) {
        let timeout;
        return function executedFunction(...args) {
            const later = () => {
                clearTimeout(timeout);
                func(...args);
            };
            clearTimeout(timeout);
            timeout = setTimeout(later, wait);
        };
    }

    // 节流函数
    throttle(func, limit) {
        let inThrottle;
        return function(...args) {
            if (!inThrottle) {
                func.apply(this, args);
                inThrottle = true;
                setTimeout(() => inThrottle = false, limit);
            }
        };
    }

    // 初始化多语言支持
    initLocalization() {
        this.translations = {
            'en': {
                'app_title': 'DICOM Processing System',
                'system_normal': 'System Normal',
                'system_abnormal': 'System Abnormal',
                'single_process': 'Single Process',
                'batch_process': 'Batch Process',
                'file_upload': 'File Upload',
                'task_list': 'Task List',
                'single_study_process': 'Single Study Process',
                'accession_number': 'AccessionNumber',
                'enter_accession_number': 'Please enter the AccessionNumber of the study to process',
                'start_process': 'Start Process',
                'batch_study_process': 'Batch Study Process',
                'accession_number_list': 'AccessionNumber List',
                'batch_input_placeholder': 'Enter one AccessionNumber per line...',
                'batch_input_help': 'Enter one AccessionNumber per line, the system will process them sequentially',
                'start_batch_process': 'Start Batch Process',
                'clear': 'Clear',
                'upload_zip_process': 'Upload ZIP File Process',
                'select_dicom_zip': 'Select DICOM ZIP File',
                'click_or_drag': 'Click to select file or drag here',
                'support_zip': 'Supports .zip format, max 500MB',
                'selected_file': 'Selected File:',
                'file_size': 'Size:',
                'process_progress': 'Process Progress',
                'cancel_process': 'Cancel Process',
                'current_status': 'Current Status:',
                'preparing': 'Preparing...',
                'waiting_start': 'Waiting to start...',
                'process_steps': 'Process Steps:',
                'process_log': 'Process Log:',
                'waiting_process': 'Waiting for process to start...',
                'process_result': 'Process Result',
                'history_tasks': 'History Tasks',
                'refresh': 'Refresh',
                'task_id': 'Task ID',
                'task_type': 'Type',
                'task_summary': 'Summary',
                'completed_time': 'Completed Time',
                'duration': 'Duration',
                'download': 'Download',
                'download_excel': 'Excel',
                'download_zip': 'ZIP',
                'no_history_tasks': 'No completed tasks yet',
                'page_size': 'Page size',
                'prev_page': 'Prev',
                'next_page': 'Next',
                'page': 'Page',
                'of': 'of',
                'process_options': 'Process Options',
                'basic_settings': 'Basic Settings',
                'auto_extract': 'Auto Extract',
                'auto_organize': 'Auto Organize Files',
                'auto_metadata': 'Extract Metadata',
                'file_management': 'File Management',
                'keep_original_zip': 'Keep Original ZIP',
                'keep_extracted_files': 'Keep Extracted Files',
                'metadata_fields': 'Metadata Fields',
                'output_format_settings': 'Output Format Settings',
                'output_format': 'Output Format',
                'nifti_format': 'NIfTI (.nii.gz)',
                'npz_format': 'NPZ (.npz - Normalized)',
                'dicom_server_config': 'DICOM Server Config',
                'pacs_ip': 'PACS IP',
                'pacs_port': 'PACS Port',
                'calling_aet': 'Calling AET',
                'called_aet': 'Called AET',
                'calling_port': 'Calling Port',
                'save_config': 'Save Config',
                'test_connection': 'Test Connection',
                'pacs_config_help': 'Saved to server .env and applied to new connections.',
                'config_saved': 'Configuration saved',
                'config_save_failed': 'Failed to save configuration',
                'config_load_failed': 'Failed to load configuration',
                'pacs_connection_ok': 'PACS connection OK',
                'pacs_connection_failed': 'PACS connection failed',
                'use_default_fields': 'Use Default Fields',
                'upload_custom_config': 'Upload Custom Config',
                'upload_json_help': 'Upload JSON format field list',
                'modality_filter': 'Modality Filter (Optional)',
                'modality_filter_help': 'Filter by modality, e.g. MR, CT (comma separated)',
                'min_series_files': 'Min Series Files (Optional)',
                'min_series_files_help': 'Skip series with fewer files than this',
                'exclude_derived': 'Exclude derived series (MPR, MIP, VR, etc.)',
                'exclude_derived_help': 'Automatically filter out MPR, MIP, 3D VR, and other derived series',
                'quick_actions': 'Quick Actions',
                'reset_options': 'Reset Options',
                'export_config': 'Export Config',
                'system_info': 'System Info',
                'active_tasks': 'Active Tasks:',
                'total_tasks': 'Total Tasks:',
                'dicom_service': 'DICOM Service:',
                'normal': 'Normal',
                'abnormal': 'Abnormal',
                'error': 'Error',
                'success': 'Success',
                'close': 'Close',
                'confirm_leave': 'Current task is running, are you sure you want to leave?',
                'only_zip_supported': 'Only ZIP files are supported',
                'enter_accession_number_error': 'Please enter AccessionNumber',
                'start_process_failed': 'Failed to start process',
                'network_error': 'Network Error: ',
                'process_started': 'Process task started',
                'process_queued': 'Task queued, waiting...',
                'batch_process_queued': 'Batch task queued, waiting...',
                'upload_process_queued': 'Upload task queued, waiting...',
                'enter_accession_number_list_error': 'Please enter AccessionNumber list',
                'no_valid_accession_number': 'No valid AccessionNumber',
                'start_batch_process_failed': 'Failed to start batch process',
                'batch_process_started': 'Batch process task started',
                'select_zip_error': 'Please select a ZIP file to upload',
                'upload_process_started': 'File upload process task started',
                'start_upload_failed': 'Failed to start file process',
                'confirm_cancel_task': 'Are you sure you want to cancel the current task?',
                'task_cancelled': 'Task cancelled',
                'cancel_task_failed': 'Failed to cancel task',
                'initializing': 'Initializing...',
                'status_running': 'Processing...',
                'status_completed': 'Completed',
                'status_failed': 'Failed',
                'status_cancelled': 'Cancelled',
                'status_unknown': 'Unknown Status',
                'task_completed_message': 'Processing completed!',
                'task_failed_message': 'Processing failed, please check logs',
                'system_normal_html': '<i class="fas fa-circle"></i> System Normal',
                'system_abnormal_html': '<i class="fas fa-exclamation-triangle"></i> System Abnormal',
                'connection_lost_html': '<i class="fas fa-wifi"></i> Connection Lost',
                'connection_error': 'Connection Error',
                'monitoring': 'Monitoring',
                'system_monitoring': 'System Monitoring',
                'disk_usage': 'Disk Usage',
                'memory_usage': 'Memory Usage',
                'cpu_usage': 'CPU Usage',
                'pacs_status': 'PACS Status',
                'used': 'Used',
                'available': 'Available',
                'cores': 'Cores',
                'checking': 'Checking...',
                'task_statistics': 'Task Statistics',
                'total': 'Total',
                'running': 'Running',
                'pending': 'Pending',
                'completed': 'Completed',
                'failed': 'Failed',
                'cancelled': 'Cancelled',
                'active_tasks_detail': 'Active Tasks',
                'progress': 'Progress',
                'current_step': 'Current Step',
                'elapsed_time': 'Elapsed',
                'action': 'Action',
                'no_active_tasks': 'No active tasks',
                'directory_usage': 'Directory Usage',
                'recent_completed': 'Recent Completed',
                'no_recent_tasks': 'No recent tasks',
                'auto_refresh': 'Auto',
                'stop_auto_refresh': 'Stop',
                'cancel': 'Cancel',
                'seconds_ago': 'seconds ago',
                'minutes_ago': 'minutes ago',
                'hours_ago': 'hours ago',
                'log_files': 'Log Files',
                'download_log': 'Download Log',
                'no_log_files': 'No log files',
                'file_size': 'Size',
                'modified_time': 'Modified',
                'cli_client': 'CLI Client',
                'cli_client_title': 'Command-line Batch Download Client',
                'filter_keywords_config': 'Filter Keywords',
                'derived_series_keywords': 'Derived Series Keywords',
                'filter_keywords_help': 'One keyword per line. These keywords are used to filter out derived series (MPR, MIP, etc.).',
                'save': 'Save',
                'reset': 'Reset',
                'filter_keywords_saved': 'Keywords saved. Will take effect on next task.'
            },
            'zh': {
                'app_title': 'DICOM处理系统',
                'system_normal': '系统正常',
                'system_abnormal': '系统异常',
                'single_process': '单个处理',
                'batch_process': '批量处理',
                'file_upload': '文件上传',
                'task_list': '任务列表',
                'single_study_process': '单个研究处理',
                'accession_number': 'AccessionNumber',
                'enter_accession_number': '请输入要处理的研究的AccessionNumber',
                'start_process': '开始处理',
                'batch_study_process': '批量研究处理',
                'accession_number_list': 'AccessionNumber列表',
                'batch_input_placeholder': '每行输入一个AccessionNumber...',
                'batch_input_help': '每行输入一个AccessionNumber，系统将依次处理',
                'start_batch_process': '开始批量处理',
                'clear': '清空',
                'upload_zip_process': '上传ZIP文件处理',
                'select_dicom_zip': '选择DICOM ZIP文件',
                'click_or_drag': '点击选择文件或拖拽到此处',
                'support_zip': '支持 .zip 格式，最大 500MB',
                'selected_file': '已选择文件:',
                'file_size': '大小:',
                'process_progress': '处理进度',
                'cancel_process': '取消处理',
                'current_status': '当前状态:',
                'preparing': '准备中...',
                'waiting_start': '等待开始...',
                'process_steps': '处理步骤:',
                'process_log': '处理日志:',
                'waiting_process': '等待处理开始...',
                'process_result': '处理结果',
                'history_tasks': '历史任务',
                'refresh': '刷新',
                'task_id': '任务ID',
                'task_type': '类型',
                'task_summary': '摘要',
                'completed_time': '完成时间',
                'duration': '耗时',
                'download': '下载',
                'download_excel': 'Excel',
                'download_zip': '文件包',
                'no_history_tasks': '暂无已完成任务',
                'page_size': '每页',
                'prev_page': '上一页',
                'next_page': '下一页',
                'page': '第',
                'of': '页/共',
                'process_options': '处理选项',
                'basic_settings': '基本设置',
                'auto_extract': '自动解压',
                'auto_organize': '自动整理文件',
                'auto_metadata': '提取元数据',
                'file_management': '文件管理',
                'keep_original_zip': '保留原始ZIP',
                'keep_extracted_files': '保留解压文件',
                'metadata_fields': '元数据字段',
                'output_format_settings': '输出格式设置',
                'output_format': '输出格式',
                'nifti_format': 'NIfTI (.nii.gz)',
                'npz_format': 'NPZ (.npz - 严格规范化)',
                'dicom_server_config': 'DICOM服务器配置',
                'pacs_ip': 'PACS地址',
                'pacs_port': 'PACS端口',
                'calling_aet': 'Calling AET',
                'called_aet': 'Called AET',
                'calling_port': 'Calling端口',
                'save_config': '保存配置',
                'test_connection': '测试连接',
                'pacs_config_help': '保存到服务器 .env，并应用到新连接。',
                'config_saved': '配置已保存',
                'config_save_failed': '保存配置失败',
                'config_load_failed': '加载配置失败',
                'pacs_connection_ok': 'PACS连接正常',
                'pacs_connection_failed': 'PACS连接失败',
                'use_default_fields': '使用默认字段',
                'upload_custom_config': '上传自定义配置',
                'upload_json_help': '上传JSON格式的字段列表',
                'modality_filter': '模态过滤 (可选)',
                'modality_filter_help': '按模态过滤，如 MR, CT (逗号分隔)',
                'min_series_files': '最小序列文件数 (可选)',
                'min_series_files_help': '跳过文件数少于此值的序列',
                'exclude_derived': '排除衍生序列 (MPR, MIP, VR 等)',
                'exclude_derived_help': '自动过滤 MPR、MIP、3D VR 等衍生序列',
                'quick_actions': '快速操作',
                'reset_options': '重置选项',
                'export_config': '导出配置',
                'system_info': '系统信息',
                'active_tasks': '活跃任务:',
                'total_tasks': '总任务数:',
                'dicom_service': 'DICOM服务:',
                'normal': '正常',
                'abnormal': '异常',
                'error': '错误',
                'success': '成功',
                'close': '关闭',
                'confirm_leave': '当前有任务正在处理，确定要离开吗？',
                'only_zip_supported': '只支持ZIP文件格式',
                'enter_accession_number_error': '请输入AccessionNumber',
                'start_process_failed': '启动处理失败',
                'network_error': '网络错误: ',
                'process_started': '处理任务已启动',
                'process_queued': '任务已加入队列，等待中...',
                'batch_process_queued': '批量任务已加入队列，等待中...',
                'upload_process_queued': '上传任务已加入队列，等待中...',
                'enter_accession_number_list_error': '请输入AccessionNumber列表',
                'no_valid_accession_number': '没有有效的AccessionNumber',
                'start_batch_process_failed': '启动批量处理失败',
                'batch_process_started': '批量处理任务已启动',
                'select_zip_error': '请先选择要上传的ZIP文件',
                'upload_process_started': '文件上传处理任务已启动',
                'start_upload_failed': '启动文件处理失败',
                'confirm_cancel_task': '确定要取消当前处理任务吗？',
                'task_cancelled': '任务已取消',
                'cancel_task_failed': '取消任务失败',
                'initializing': '初始化...',
                'status_running': '处理中...',
                'status_completed': '已完成',
                'status_failed': '处理失败',
                'status_cancelled': '已取消',
                'status_unknown': '未知状态',
                'task_completed_message': '处理完成！',
                'task_failed_message': '处理失败，请检查日志信息',
                'system_normal_html': '<i class="fas fa-circle"></i> 系统正常',
                'system_abnormal_html': '<i class="fas fa-exclamation-triangle"></i> 系统异常',
                'connection_lost_html': '<i class="fas fa-wifi"></i> 连接中断',
                'connection_error': '无法连接',
                'monitoring': '监控',
                'system_monitoring': '系统监控',
                'disk_usage': '磁盘使用',
                'memory_usage': '内存使用',
                'cpu_usage': 'CPU使用',
                'pacs_status': 'PACS状态',
                'used': '已用',
                'available': '可用',
                'cores': '核心',
                'checking': '检查中...',
                'task_statistics': '任务统计',
                'total': '总数',
                'running': '运行中',
                'pending': '等待中',
                'completed': '已完成',
                'failed': '失败',
                'cancelled': '已取消',
                'active_tasks_detail': '活跃任务',
                'progress': '进度',
                'current_step': '当前步骤',
                'elapsed_time': '已运行',
                'action': '操作',
                'no_active_tasks': '无活跃任务',
                'directory_usage': '目录使用',
                'recent_completed': '近期完成',
                'no_recent_tasks': '无近期任务',
                'auto_refresh': '自动',
                'stop_auto_refresh': '停止',
                'cancel': '取消',
                'seconds_ago': '秒前',
                'minutes_ago': '分钟前',
                'hours_ago': '小时前',
                'log_files': '日志文件',
                'download_log': '下载日志',
                'no_log_files': '无日志文件',
                'file_size': '大小',
                'modified_time': '修改时间',
                'cli_client': '客户端',
                'cli_client_title': '命令行批量下载客户端',
                'filter_keywords_config': '过滤关键词',
                'derived_series_keywords': '衍生序列过滤关键词',
                'filter_keywords_help': '每行一个关键词。这些关键词用于过滤衍生序列（MPR、MIP等）。',
                'save': '保存',
                'reset': '重置',
                'filter_keywords_saved': '关键词已保存，将在下一个任务时生效'
            }
        };

        // Load saved language or default to English
        const savedLang = localStorage.getItem('language') || 'en';
        this.setLanguage(savedLang);
    }

    setLanguage(lang) {
        if (!this.translations[lang]) return;
        
        this.currentLang = lang;
        localStorage.setItem('language', lang);
        document.documentElement.lang = lang;
        
        // Update dropdown text
        const langName = lang === 'en' ? 'English' : '中文';
        const currentLangEl = document.getElementById('currentLang');
        if (currentLangEl) currentLangEl.textContent = langName;

        this.updateTexts();
    }

    updateTexts() {
        const t = this.translations[this.currentLang];
        
        // Update elements with data-i18n attribute
        document.querySelectorAll('[data-i18n]').forEach(el => {
            const key = el.getAttribute('data-i18n');
            if (t[key]) {
                if (el.tagName === 'INPUT' || el.tagName === 'TEXTAREA') {
                    el.placeholder = t[key];
                } else {
                    el.textContent = t[key];
                }
            }
        });
        
        // Update specific elements that might need dynamic content
        this.updateCurrentTime();
        this.loadSystemStatus();
        this.renderHistoryPagination();
    }

    // 初始化WebSocket连接 - 带自动重连
    initializeSocket() {
        this.socketReconnectAttempts = 0;
        this.socketMaxReconnectAttempts = 5;
        this.socketReconnectDelay = 1000; // 初始重连延迟1秒
        
        const connectSocket = () => {
            this.socket = io();
            
            this.socket.on('connect', () => {
                console.log('✅ WebSocket连接成功');
                this.updateConnectionStatus(true);
                // 重置重连计数
                this.socketReconnectAttempts = 0;
                this.socketReconnectDelay = 1000;
                
                // 如果有当前任务，重新订阅
                if (this.currentTask && this.currentTask.id) {
                    this.subscribeToTask(this.currentTask.id);
                }
            });

            this.socket.on('disconnect', (reason) => {
                console.log('❌ WebSocket连接断开:', reason);
                this.updateConnectionStatus(false);
                
                // 如果断开原因是io server disconnect，需要手动重连
                if (reason === 'io server disconnect') {
                    this.attemptReconnect();
                }
            });

            this.socket.on('connect_error', (error) => {
                console.error('WebSocket连接错误:', error);
                this.attemptReconnect();
            });

            this.socket.on('task_update', (data) => {
                this.handleTaskUpdate(data);
            });
        };
        
        connectSocket();
    }

    // WebSocket重连
    attemptReconnect() {
        if (this.socketReconnectAttempts >= this.socketMaxReconnectAttempts) {
            console.error('WebSocket重连次数已达上限，请刷新页面');
            this.showError('连接丢失，请刷新页面重试');
            return;
        }
        
        this.socketReconnectAttempts++;
        const delay = Math.min(this.socketReconnectDelay * Math.pow(2, this.socketReconnectAttempts - 1), 30000);
        
        console.log(`WebSocket将在 ${delay}ms 后尝试第 ${this.socketReconnectAttempts} 次重连...`);
        
        setTimeout(() => {
            console.log('尝试重连WebSocket...');
            this.initializeSocket();
        }, delay);
    }

    // 绑定事件监听器
    bindEvents() {
        // 关键字配置切换
        document.querySelectorAll('input[name="keywordConfig"]').forEach(radio => {
            radio.addEventListener('change', this.handleKeywordConfigChange.bind(this));
        });

        // DICOM服务器配置：首次展开时加载
        const pacsCollapse = document.getElementById('collapseDicomServerConfig');
        if (pacsCollapse) {
            pacsCollapse.addEventListener('show.bs.collapse', () => {
                if (!this.pacsConfigLoaded) {
                    this.loadPacsConfig();
                    this.pacsConfigLoaded = true;
                }
            });
        }

        // DICOM服务器配置：保存
        const savePacsConfigBtn = document.getElementById('savePacsConfig');
        if (savePacsConfigBtn) {
            savePacsConfigBtn.addEventListener('click', () => this.savePacsConfig());
        }

        // DICOM服务器配置：测试连接
        const testPacsConnectionBtn = document.getElementById('testPacsConnection');
        if (testPacsConnectionBtn) {
            testPacsConnectionBtn.addEventListener('click', () => this.testPacsConnection());
        }

        // 过滤关键词配置：保存
        const saveFilterKeywordsBtn = document.getElementById('saveFilterKeywords');
        if (saveFilterKeywordsBtn) {
            saveFilterKeywordsBtn.addEventListener('click', () => this.saveFilterKeywords());
        }

        // 过滤关键词配置：重置
        const resetFilterKeywordsBtn = document.getElementById('resetFilterKeywords');
        if (resetFilterKeywordsBtn) {
            resetFilterKeywordsBtn.addEventListener('click', () => this.resetFilterKeywords());
        }

        // 文件拖拽上传
        const uploadArea = document.getElementById('uploadArea');
        if (uploadArea) {
            uploadArea.addEventListener('dragover', this.handleDragOver.bind(this));
            uploadArea.addEventListener('dragleave', this.handleDragLeave.bind(this));
            uploadArea.addEventListener('drop', this.handleFileDrop.bind(this));
        }

        // 任务列表刷新
        const historyRefresh = document.getElementById('historyRefresh');
        if (historyRefresh) {
            historyRefresh.addEventListener('click', () => this.loadTaskHistory());
        }

        const historyTab = document.getElementById('history-tab');
        if (historyTab) {
            historyTab.addEventListener('shown.bs.tab', () => this.loadTaskHistory());
        }

        // 监控标签页事件
        const monitoringTab = document.getElementById('monitoring-tab');
        if (monitoringTab) {
            monitoringTab.addEventListener('shown.bs.tab', () => {
                this.loadMonitoringData();
                // 自动开启自动刷新
                if (!this.monitoringAutoRefreshInterval) {
                    this.toggleAutoRefresh();
                }
            });
            // 离开监控页面时停止自动刷新
            monitoringTab.addEventListener('hidden.bs.tab', () => {
                if (this.monitoringAutoRefreshInterval) {
                    this.toggleAutoRefresh();
                }
            });
        }

        const historyPageSize = document.getElementById('historyPageSize');
        if (historyPageSize) {
            historyPageSize.addEventListener('change', (event) => {
                const nextSize = Number.parseInt(event.target.value, 10);
                this.historyPageSize = Number.isNaN(nextSize) ? this.historyPageSize : nextSize;
                this.historyPage = 1;
                this.loadTaskHistory(1);
            });
        }

        const historyPrev = document.getElementById('historyPrev');
        if (historyPrev) {
            historyPrev.addEventListener('click', () => {
                if (this.historyPage > 1) {
                    this.loadTaskHistory(this.historyPage - 1);
                }
            });
        }

        const historyNext = document.getElementById('historyNext');
        if (historyNext) {
            historyNext.addEventListener('click', () => {
                if (this.historyPage < this.historyTotalPages) {
                    this.loadTaskHistory(this.historyPage + 1);
                }
            });
        }

        // 键盘快捷键
        document.addEventListener('keydown', this.handleKeydown.bind(this));

        // 窗口关闭前确认
        window.addEventListener('beforeunload', (e) => {
            if (this.currentTask && this.currentTask.status === 'running') {
                e.preventDefault();
                e.returnValue = this.translations[this.currentLang]['confirm_leave'];
            }
        });
    }

    async loadTaskHistory(page = null) {
        const tbody = document.getElementById('historyTableBody');
        const emptyState = document.getElementById('historyEmpty');
        if (!tbody || !emptyState) {
            return;
        }

        const targetPage = page || this.historyPage || 1;
        const pageSize = this.historyPageSize || 20;

        try {
            const response = await fetch(`/api/tasks/history?page=${targetPage}&page_size=${pageSize}`);
            const data = await response.json();
            if (!response.ok) {
                throw new Error(data.error || 'Failed to load history');
            }
            this.historyPage = data.page || targetPage;
            this.historyPageSize = data.page_size || pageSize;
            this.historyTotalPages = data.total_pages || 0;
            this.historyTotalCount = data.total || 0;
            this.renderHistoryTasks(data.tasks || []);
            this.renderHistoryPagination();
        } catch (error) {
            console.error('获取历史任务失败:', error);
            this.renderHistoryTasks([]);
            this.historyTotalPages = 0;
            this.historyTotalCount = 0;
            this.renderHistoryPagination();
        }
    }

    renderHistoryTasks(tasks) {
        const tbody = document.getElementById('historyTableBody');
        const emptyState = document.getElementById('historyEmpty');
        if (!tbody || !emptyState) {
            return;
        }

        tbody.innerHTML = '';
        if (!tasks || tasks.length === 0) {
            emptyState.style.display = 'block';
            return;
        }

        emptyState.style.display = 'none';
        tasks.forEach(task => {
            const endTime = task.end_time ? new Date(task.end_time * 1000) : null;
            const endTimeText = endTime ? endTime.toLocaleString() : '-';
            const durationText = task.duration ? `${Math.round(task.duration)}s` : '-';
            const taskTypeLabel = this.getTaskTypeLabel(task.task_type);
            const summary = task.summary ? this.escapeHtml(task.summary) : '-';

            const downloadButtons = [];
            if (task.has_excel) {
                downloadButtons.push(
                    `<a href="/api/download/${task.task_id}/excel" class="btn btn-outline-success btn-sm me-1">` +
                    `<i class="fas fa-file-excel"></i> ${this.translations[this.currentLang]['download_excel']}</a>`
                );
            }
            if (task.has_zip) {
                downloadButtons.push(
                    `<a href="/api/download/${task.task_id}/zip" class="btn btn-outline-primary btn-sm">` +
                    `<i class="fas fa-file-archive"></i> ${this.translations[this.currentLang]['download_zip']}</a>`
                );
            }

            const row = document.createElement('tr');
            row.innerHTML = `
                <td><code>${this.escapeHtml(task.task_id)}</code></td>
                <td>${taskTypeLabel}</td>
                <td>${summary}</td>
                <td>${this.escapeHtml(endTimeText)}</td>
                <td>${durationText}</td>
                <td>${downloadButtons.join(' ') || '-'}</td>
            `;
            tbody.appendChild(row);
        });
    }

    renderHistoryPagination() {
        const container = document.getElementById('historyPagination');
        const info = document.getElementById('historyPageInfo');
        const prevBtn = document.getElementById('historyPrev');
        const nextBtn = document.getElementById('historyNext');
        const pageSizeSelect = document.getElementById('historyPageSize');
        if (!container || !info || !prevBtn || !nextBtn || !pageSizeSelect) {
            return;
        }

        if (pageSizeSelect.value !== String(this.historyPageSize)) {
            pageSizeSelect.value = String(this.historyPageSize);
        }

        const t = this.translations[this.currentLang];
        const totalPages = this.historyTotalPages || 0;
        const page = this.historyPage || 1;
        const total = this.historyTotalCount || 0;

        prevBtn.disabled = page <= 1 || totalPages <= 0;
        nextBtn.disabled = totalPages <= 0 || page >= totalPages;

        if (totalPages <= 0 || total <= 0) {
            info.textContent = '';
            return;
        }

        if (this.currentLang === 'zh') {
            info.textContent = `${t.page}${page}${t.of}${totalPages}`;
        } else {
            info.textContent = `${t.page} ${page} ${t.of} ${totalPages}`;
        }
    }

    getTaskTypeLabel(taskType) {
        const map = {
            'single': this.currentLang === 'en' ? 'Single' : '单个',
            'batch': this.currentLang === 'en' ? 'Batch' : '批量',
            'upload': this.currentLang === 'en' ? 'Upload' : '上传'
        };
        return map[taskType] || taskType || '-';
    }

    // 更新当前时间
    updateCurrentTime() {
        const now = new Date();
        const locale = this.currentLang === 'en' ? 'en-US' : 'zh-CN';
        const timeString = now.toLocaleTimeString(locale);
        document.getElementById('currentTime').textContent = timeString;
    }

    // 加载系统状态
    async loadSystemStatus() {
        try {
            const response = await fetch('/api/system/status');
            const data = await response.json();
            
            document.getElementById('activeTasks').textContent = data.active_tasks || 0;
            document.getElementById('totalTasks').textContent = data.total_tasks || 0;
            
            const statusText = data.dicom_service_status === 'connected' 
                ? (this.translations[this.currentLang]['system_normal'] || 'System Normal')
                : (this.translations[this.currentLang]['system_abnormal'] || 'System Abnormal');

            // 设置 serviceStatus 文本并根据连接状态调整文字颜色（正常：绿色，异常：红色）
            const serviceStatusEl = document.getElementById('serviceStatus');
            if (serviceStatusEl) {
                serviceStatusEl.textContent = statusText;
                console.log('DICOM服务状态:', data.dicom_service_status);
                if (data.dicom_service_status === 'connected') {
                    serviceStatusEl.style.color = 'green';
                } else {
                    serviceStatusEl.style.color = 'red';
                }
            }

            const statusElement = document.getElementById('systemStatus');
            if (data.status === 'running') {
                statusElement.innerHTML = this.translations[this.currentLang]['system_normal_html'];
                statusElement.className = 'badge bg-success me-2';
            } else {
                statusElement.innerHTML = this.translations[this.currentLang]['system_abnormal_html'];
                statusElement.className = 'badge bg-warning me-2';
            }
            
        } catch (error) {
            console.error('获取系统状态失败:', error);
            const serviceStatusEl = document.getElementById('serviceStatus');
            if (serviceStatusEl) {
                serviceStatusEl.textContent = this.translations[this.currentLang]['connection_error'];
                serviceStatusEl.style.color = 'red';
            }
        }
    }

    async loadPacsConfig() {
        try {
            const response = await fetch('/api/pacs-config');
            const data = await response.json();

            if (!response.ok) {
                throw new Error(data.error || this.translations[this.currentLang]['config_load_failed']);
            }

            const setValue = (id, value) => {
                const el = document.getElementById(id);
                if (el) el.value = value ?? '';
            };

            setValue('pacsIp', data.PACS_IP);
            setValue('pacsPort', data.PACS_PORT);
            setValue('callingAet', data.CALLING_AET);
            setValue('calledAet', data.CALLED_AET);
            setValue('callingPort', data.CALLING_PORT);
        } catch (error) {
            console.error('Failed to load PACS config:', error);
            this.showError(`${this.translations[this.currentLang]['config_load_failed']}: ${error.message}`);
        }
    }

    async savePacsConfig() {
        try {
            const pacsIp = (document.getElementById('pacsIp')?.value || '').trim();
            const pacsPortRaw = (document.getElementById('pacsPort')?.value || '').trim();
            const callingAet = (document.getElementById('callingAet')?.value || '').trim();
            const calledAet = (document.getElementById('calledAet')?.value || '').trim();
            const callingPortRaw = (document.getElementById('callingPort')?.value || '').trim();

            const payload = {
                PACS_IP: pacsIp,
                PACS_PORT: pacsPortRaw === '' ? null : Number.parseInt(pacsPortRaw, 10),
                CALLING_AET: callingAet,
                CALLED_AET: calledAet,
                CALLING_PORT: callingPortRaw === '' ? null : Number.parseInt(callingPortRaw, 10)
            };

            const response = await fetch('/api/pacs-config', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(payload)
            });

            const data = await response.json();
            if (!response.ok) {
                this.showError(data.error || this.translations[this.currentLang]['config_save_failed']);
                return;
            }

            this.showSuccess(data.message || this.translations[this.currentLang]['config_saved']);
            this.loadSystemStatus();
        } catch (error) {
            console.error('Failed to save PACS config:', error);
            this.showError(`${this.translations[this.currentLang]['config_save_failed']}: ${error.message}`);
        }
    }

    async testPacsConnection() {
        const btn = document.getElementById('testPacsConnection');
        const originalDisabled = btn ? btn.disabled : false;
        if (btn) btn.disabled = true;

        try {
            const response = await fetch('/api/debug/test-connection');
            const data = await response.json();

            if (!response.ok) {
                throw new Error(data.error || 'Test connection failed');
            }

            if (data.pacs_connected) {
                this.showSuccess(this.translations[this.currentLang]['pacs_connection_ok']);
            } else {
                this.showError(this.translations[this.currentLang]['pacs_connection_failed']);
            }
        } catch (error) {
            console.error('PACS test connection error:', error);
            this.showError(`${this.translations[this.currentLang]['pacs_connection_failed']}: ${error.message}`);
        } finally {
            if (btn) btn.disabled = originalDisabled;
        }
    }

    // 加载过滤关键词
    async loadFilterKeywords() {
        try {
            const response = await fetch('/api/filter-keywords');
            const data = await response.json();
            if (response.ok) {
                const textarea = document.getElementById('filterKeywordsTextarea');
                if (textarea) {
                    textarea.value = data.keywords.join('\n');
                }
            }
        } catch (error) {
            console.error('Failed to load filter keywords:', error);
        }
    }

    // 保存过滤关键词
    async saveFilterKeywords() {
        const textarea = document.getElementById('filterKeywordsTextarea');
        const statusDiv = document.getElementById('filterKeywordsStatus');
        if (!textarea) return;

        const keywords = textarea.value.split('\n').map(k => k.trim()).filter(k => k);

        try {
            const response = await fetch('/api/filter-keywords', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ keywords })
            });

            const data = await response.json();
            if (response.ok) {
                if (statusDiv) {
                    statusDiv.style.display = 'block';
                    setTimeout(() => { statusDiv.style.display = 'none'; }, 3000);
                }
                // 更新textarea为服务器返回的实际值（可能已去重）
                textarea.value = data.keywords.join('\n');
            } else {
                this.showError(data.error || 'Failed to save filter keywords');
            }
        } catch (error) {
            console.error('Failed to save filter keywords:', error);
            this.showError(`Failed to save filter keywords: ${error.message}`);
        }
    }

    // 重置过滤关键词
    async resetFilterKeywords() {
        const textarea = document.getElementById('filterKeywordsTextarea');
        const statusDiv = document.getElementById('filterKeywordsStatus');

        try {
            const response = await fetch('/api/filter-keywords/reset', {
                method: 'POST'
            });

            const data = await response.json();
            if (response.ok) {
                if (textarea) {
                    textarea.value = data.keywords.join('\n');
                }
                if (statusDiv) {
                    statusDiv.style.display = 'block';
                    statusDiv.innerHTML = '<span class="text-success"><i class="fas fa-check-circle"></i> Reset to default successfully</span>';
                    setTimeout(() => {
                        statusDiv.style.display = 'none';
                        statusDiv.innerHTML = '<span class="text-success"><i class="fas fa-check-circle"></i> <span data-i18n="filter_keywords_saved">Keywords saved. Will take effect on next task.</span></span>';
                    }, 3000);
                }
            } else {
                this.showError(data.error || 'Failed to reset filter keywords');
            }
        } catch (error) {
            console.error('Failed to reset filter keywords:', error);
            this.showError(`Failed to reset filter keywords: ${error.message}`);
        }
    }

    // 更新连接状态
    updateConnectionStatus(connected) {
        const statusElement = document.getElementById('systemStatus');
        if (connected) {
            statusElement.innerHTML = this.translations[this.currentLang]['system_normal_html'];
            statusElement.className = 'badge bg-success me-2';
        } else {
            statusElement.innerHTML = this.translations[this.currentLang]['connection_lost_html'];
            statusElement.className = 'badge bg-danger me-2';
        }
    }

    // 处理关键字配置变化
    handleKeywordConfigChange(event) {
        const customFileDiv = document.getElementById('customKeywordFile');
        if (event.target.value === 'custom') {
            customFileDiv.style.display = 'block';
        } else {
            customFileDiv.style.display = 'none';
        }
    }

    // 处理拖拽悬停
    handleDragOver(event) {
        event.preventDefault();
        event.currentTarget.classList.add('dragover');
    }

    // 处理拖拽离开
    handleDragLeave(event) {
        event.currentTarget.classList.remove('dragover');
    }

    // 处理文件拖拽
    handleFileDrop(event) {
        event.preventDefault();
        event.currentTarget.classList.remove('dragover');
        
        const files = event.dataTransfer.files;
        if (files.length > 0) {
            const file = files[0];
            if (file.name.toLowerCase().endsWith('.zip')) {
                this.handleFileSelection(file);
            } else {
                this.showError(this.translations[this.currentLang]['only_zip_supported']);
            }
        }
    }

    // 处理键盘快捷键
    handleKeydown(event) {
        // Ctrl+Enter 开始处理
        if (event.ctrlKey && event.key === 'Enter') {
            const activeTab = document.querySelector('.nav-link.active');
            if (activeTab) {
                if (activeTab.id === 'single-tab') {
                    this.startSingleProcess();
                } else if (activeTab.id === 'batch-tab') {
                    this.startBatchProcess();
                } else if (activeTab.id === 'upload-tab') {
                    this.startUploadProcess();
                }
            }
            event.preventDefault();
        }
        
        // Esc 取消当前任务
        if (event.key === 'Escape' && this.currentTask) {
            this.cancelCurrentTask();
        }
    }

    // 获取处理选项
    getProcessingOptions() {
        return {
            auto_extract: document.getElementById('autoExtract').checked,
            auto_organize: document.getElementById('autoOrganize').checked,
            auto_metadata: document.getElementById('autoMetadata').checked,
            keep_zip: document.getElementById('keepZip').checked,
            keep_extracted: document.getElementById('keepExtracted').checked,
            output_format: document.querySelector('input[name="outputFormat"]:checked')?.value || 'nifti'
        };
    }

    // 开始单个处理 - 内部实现
    async _startSingleProcess() {
        // 防止重复提交
        if (this.isProcessing) {
            console.warn('已有处理任务在进行中');
            return;
        }

        const accessionNumber = document.getElementById('accessionNumber').value.trim();
        
        if (!accessionNumber) {
            this.showError(this.translations[this.currentLang]['enter_accession_number_error']);
            return;
        }

        this.isProcessing = true;

        // 先检查系统状态
        try {
            console.log('检查系统状态...');
            const statusResponse = await fetch('/api/system/status');
            const statusData = await statusResponse.json();
            
            console.log('系统状态:', statusData);
            
            // 暂时跳过DICOM状态检查，直接执行
            if (statusData.dicom_service_status !== 'connected') {
                console.warn('DICOM服务状态异常，但继续执行处理');
                // 不再返回，继续执行
            }
        } catch (error) {
            console.error('检查系统状态失败:', error);
            console.warn('状态检查失败，但继续执行处理');
            // 不再返回，继续执行
        }

        const options = this.getProcessingOptions();

        // 添加过滤参数
        const modalityFilter = document.getElementById('modalityFilter')?.value?.trim();
        const minSeriesFiles = document.getElementById('minSeriesFiles')?.value;
        const excludeDerived = document.getElementById('excludeDerived')?.checked;
        if (modalityFilter) {
            options.modality_filter = modalityFilter;
        }
        if (minSeriesFiles && parseInt(minSeriesFiles) > 0) {
            options.min_series_files = parseInt(minSeriesFiles);
        }
        if (excludeDerived !== false) {
            options.exclude_derived = true;
        }

        try {
            console.log('发送处理请求:', { accession_number: accessionNumber, options });

            const response = await fetch('/api/process/single', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    accession_number: accessionNumber,
                    options: options
                })
            });

            const data = await response.json();
            console.log('处理响应:', data);

            if (response.ok) {
                const isQueued = data.status === 'queued';
                this.currentTask = {
                    id: data.task_id,
                    type: 'single',
                    status: isQueued ? 'pending' : 'running'
                };
                this.showProgressCard();
                this.subscribeToTask(data.task_id);
                if (isQueued) {
                    this.showSuccess(this.translations[this.currentLang]['process_queued'] || 'Task queued, waiting...');
                } else {
                    this.showSuccess(this.translations[this.currentLang]['process_started']);
                }
            } else if (response.status === 503) {
                // 队列满
                this.showError((data.error || 'Task queue is full') + '. Please try again later.');
            } else {
                this.showError(data.error || this.translations[this.currentLang]['start_process_failed']);
            }
        } catch (error) {
            this.showError(this.translations[this.currentLang]['network_error'] + error.message);
        } finally {
            // 延迟重置标志，防止快速重复点击
            setTimeout(() => {
                this.isProcessing = false;
            }, 1000);
        }
    }

    // 开始批量处理 - 内部实现
    async _startBatchProcess() {
        // 防止重复提交
        if (this.isProcessing) {
            console.warn('已有处理任务在进行中');
            return;
        }

        const batchText = document.getElementById('batchAccessionNumbers').value.trim();
        
        if (!batchText) {
            this.showError(this.translations[this.currentLang]['enter_accession_number_list_error']);
            return;
        }

        const accessionNumbers = batchText.split('\n')
            .map(line => line.trim())
            .filter(line => line.length > 0);

        if (accessionNumbers.length === 0) {
            this.showError(this.translations[this.currentLang]['no_valid_accession_number']);
            return;
        }

        this.isProcessing = true;
        const options = this.getProcessingOptions();

        // 添加批量过滤参数
        const modalityFilter = document.getElementById('batchModalityFilter')?.value?.trim();
        const minSeriesFiles = document.getElementById('batchMinSeriesFiles')?.value;
        const excludeDerived = document.getElementById('batchExcludeDerived')?.checked;
        if (modalityFilter) {
            options.modality_filter = modalityFilter;
        }
        if (minSeriesFiles && parseInt(minSeriesFiles) > 0) {
            options.min_series_files = parseInt(minSeriesFiles);
        }
        if (excludeDerived !== false) {
            options.exclude_derived = true;
        }

        try {
            const response = await fetch('/api/process/batch', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    accession_numbers: accessionNumbers,
                    options: options
                })
            });

            const data = await response.json();

            if (response.ok) {
                const isQueued = data.status === 'queued';
                this.currentTask = {
                    id: data.task_id,
                    type: 'batch',
                    status: isQueued ? 'pending' : 'running'
                };
                this.showProgressCard();
                this.subscribeToTask(data.task_id);
                if (isQueued) {
                    this.showSuccess(`${this.translations[this.currentLang]['batch_process_queued'] || 'Batch task queued'} (${accessionNumbers.length})`);
                } else {
                    this.showSuccess(`${this.translations[this.currentLang]['batch_process_started']} (${accessionNumbers.length})`);
                }
            } else if (response.status === 503) {
                // 队列满
                this.showError((data.error || 'Task queue is full') + '. Please try again later.');
            } else {
                this.showError(data.error || this.translations[this.currentLang]['start_batch_process_failed']);
            }
        } catch (error) {
            this.showError(this.translations[this.currentLang]['network_error'] + error.message);
        } finally {
            setTimeout(() => {
                this.isProcessing = false;
            }, 1000);
        }
    }

    // 开始上传文件处理 - 内部实现
    async _startUploadProcess() {
        // 防止重复提交
        if (this.isProcessing) {
            console.warn('已有处理任务在进行中');
            return;
        }

        if (!this.selectedFile) {
            this.showError(this.translations[this.currentLang]['select_zip_error']);
            return;
        }

        this.isProcessing = true;
        const formData = new FormData();
        formData.append('file', this.selectedFile);
        
        const options = this.getProcessingOptions();
        for (const [key, value] of Object.entries(options)) {
            formData.append(key, value);
        }

        try {
            const response = await fetch('/api/process/upload', {
                method: 'POST',
                body: formData
            });

            const data = await response.json();

            if (response.ok) {
                const isQueued = data.status === 'queued';
                this.currentTask = {
                    id: data.task_id,
                    type: 'upload',
                    status: isQueued ? 'pending' : 'running'
                };
                this.showProgressCard();
                this.subscribeToTask(data.task_id);
                if (isQueued) {
                    this.showSuccess(this.translations[this.currentLang]['upload_process_queued'] || 'Upload task queued, waiting...');
                } else {
                    this.showSuccess(this.translations[this.currentLang]['upload_process_started']);
                }
            } else if (response.status === 503) {
                // 队列满
                this.showError((data.error || 'Task queue is full') + '. Please try again later.');
            } else {
                this.showError(data.error || this.translations[this.currentLang]['start_upload_failed']);
            }
        } catch (error) {
            this.showError(this.translations[this.currentLang]['network_error'] + error.message);
        } finally {
            setTimeout(() => {
                this.isProcessing = false;
            }, 1000);
        }
    }

    // 取消当前任务
    async cancelCurrentTask() {
        if (!this.currentTask) {
            return;
        }

        if (confirm(this.translations[this.currentLang]['confirm_cancel_task'])) {
            try {
                const response = await fetch(`/api/task/${this.currentTask.id}/cancel`, {
                    method: 'POST'
                });

                if (response.ok) {
                    this.showSuccess(this.translations[this.currentLang]['task_cancelled']);
                } else {
                    const data = await response.json();
                    this.showError(data.error || this.translations[this.currentLang]['cancel_task_failed']);
                }
            } catch (error) {
                this.showError(this.translations[this.currentLang]['network_error'] + error.message);
            }
        }
    }

    // 订阅任务更新
    subscribeToTask(taskId) {
        this.socket.emit('subscribe_task', { task_id: taskId });
    }

    // 处理任务更新
    handleTaskUpdate(data) {
        if (!this.currentTask || this.currentTask.id !== data.task_id) {
            return;
        }

        // 更新进度
        this.updateProgress(data.progress, data.current_step);
        
        // 更新状态
        this.updateStatus(data.status);
        
        // 更新日志
        if (data.logs && data.logs.length > 0) {
            this.updateLogs(data.logs);
        }

        // 如果任务完成
        if (data.status === 'completed') {
            this.handleTaskCompleted();
        } else if (data.status === 'failed' || data.status === 'cancelled') {
            this.handleTaskFailed(data.status);
        }
    }

    // 显示进度卡片
    showProgressCard() {
        document.getElementById('progressCard').style.display = 'block';
        document.getElementById('resultCard').style.display = 'none';
        
        // 显示取消按钮
        this.showCancelButton();
        
        // 重置进度
        this.updateProgress(0, this.translations[this.currentLang]['initializing']);
        this.updateStatus('running');
        this.clearLogs();
        
        // 滚动到进度卡片
        document.getElementById('progressCard').scrollIntoView({ 
            behavior: 'smooth', 
            block: 'center' 
        });
    }

    // 显示取消按钮
    showCancelButton() {
        const cancelBtn = document.querySelector('#progressCard .btn-outline-danger');
        if (cancelBtn) {
            cancelBtn.style.display = 'inline-block';
        }
    }

    // 隐藏取消按钮
    hideCancelButton() {
        const cancelBtn = document.querySelector('#progressCard .btn-outline-danger');
        if (cancelBtn) {
            cancelBtn.style.display = 'none';
        }
    }

    // 更新进度
    updateProgress(progress, step) {
        const progressBar = document.getElementById('progressBar');
        const progressPercent = document.getElementById('progressPercent');
        const currentStep = document.getElementById('currentStep');
        
        if (progressBar) {
            progressBar.style.width = `${progress}%`;
            progressBar.setAttribute('aria-valuenow', progress);
        }
        
        if (progressPercent) {
            progressPercent.textContent = `${progress}%`;
        }
        
        if (currentStep && step) {
            currentStep.textContent = step;
        }
    }

    // 更新状态
    updateStatus(status) {
        const statusElement = document.getElementById('currentStatus');
        if (!statusElement) return;

        const t = this.translations[this.currentLang] || this.translations.en;

        let statusText, statusClass;
        
        switch (status) {
            case 'running':
                statusText = t.status_running;
                statusClass = 'bg-primary';
                break;
            case 'completed':
                statusText = t.status_completed;
                statusClass = 'bg-success';
                break;
            case 'failed':
                statusText = t.status_failed;
                statusClass = 'bg-danger';
                break;
            case 'cancelled':
                statusText = t.status_cancelled;
                statusClass = 'bg-warning';
                break;
            default:
                statusText = t.status_unknown;
                statusClass = 'bg-secondary';
        }

        statusElement.textContent = statusText;
        statusElement.className = `badge ${statusClass}`;
        
        if (this.currentTask) {
            this.currentTask.status = status;
        }
    }

    // 更新日志 - 增量更新优化
    updateLogs(logs) {
        const logContainer = document.getElementById('logContainer');
        if (!logContainer) return;

        // 如果没有日志，显示等待信息
        if (!logs || logs.length === 0) {
            if (logContainer.children.length === 0 || 
                logContainer.children[0].classList.contains('text-muted')) {
                logContainer.innerHTML = '<div class="text-muted text-center p-3">等待处理开始...</div>';
            }
            return;
        }

        // 清除等待信息（如果存在）
        if (logContainer.children.length === 1 && 
            logContainer.children[0].classList.contains('text-muted')) {
            logContainer.innerHTML = '';
        }

        // 获取当前已显示的日志数量
        const currentLogCount = logContainer.querySelectorAll('.log-entry').length;
        
        // 只添加新日志（增量更新）
        const newLogs = logs.slice(currentLogCount);
        
        newLogs.forEach(log => {
            const logEntry = document.createElement('div');
            logEntry.className = `log-entry ${log.level}`;
            logEntry.innerHTML = `
                <span class="log-timestamp">${log.timestamp}</span>
                <span class="log-message">${this.escapeHtml(log.message)}</span>
            `;
            logContainer.appendChild(logEntry);
        });

        // 限制日志数量，防止DOM过大（保留最近100条）
        const maxLogs = 100;
        const allLogs = logContainer.querySelectorAll('.log-entry');
        if (allLogs.length > maxLogs) {
            const toRemove = allLogs.length - maxLogs;
            for (let i = 0; i < toRemove; i++) {
                allLogs[i].remove();
            }
        }

        // 滚动到底部
        logContainer.scrollTop = logContainer.scrollHeight;
    }

    // 清空日志
    clearLogs() {
        const logContainer = document.getElementById('logContainer');
        if (logContainer) {
            logContainer.innerHTML = '<div class="text-muted text-center p-3">等待处理开始...</div>';
        }
    }

    // 处理任务完成
    async handleTaskCompleted() {
        if (this.isHandlingTaskCompletion) {
            return;
        }

        this.isHandlingTaskCompletion = true;
        this.showSuccess(this.translations[this.currentLang]['task_completed_message']);
        
        // 隐藏取消按钮
        this.hideCancelButton();
        
        // 获取任务结果
        try {
            const response = await fetch(`/api/task/${this.currentTask.id}/status`);
            const data = await response.json();
            
            if (response.ok && data.result) {
                this.showResultCard(data.result);
            }
        } catch (error) {
            console.error('获取任务结果失败:', error);
        } finally {
            this.currentTask = null;
            this.isHandlingTaskCompletion = false;
        }
    }

    // 处理任务失败
    handleTaskFailed(status) {
        const message = status === 'cancelled'
            ? this.translations[this.currentLang]['task_cancelled']
            : this.translations[this.currentLang]['task_failed_message'];
        this.showError(message);
        
        // 隐藏取消按钮
        this.hideCancelButton();
        
        this.currentTask = null;
    }

    // 显示结果卡片
    showResultCard(result) {
        const resultCard = document.getElementById('resultCard');
        const resultContent = document.getElementById('resultContent');

        // 将来自后端的结果对象归一化为“仅包含安全原始类型”的结构，
        // 避免将不可信数据直接拼接进 innerHTML。
        const safeResult = this.normalizeResultForRender(result);
        
        let html = '';
        
        if (this.currentTask.type === 'single') {
            html = this.renderSingleResult(safeResult);
        } else if (this.currentTask.type === 'batch') {
            html = this.renderBatchResult(safeResult);
        } else if (this.currentTask.type === 'upload') {
            html = this.renderUploadResult(safeResult);
        }
        
        resultContent.innerHTML = html;
        resultCard.style.display = 'block';
        resultCard.classList.add('fade-in');
        
        // 如果是批量处理结果，初始化质量分布图表
        if (this.currentTask.type === 'batch' && window.Chart) {
            setTimeout(() => this.initBatchReportChart(), 100);
        }
        
        // 滚动到结果卡片
        resultCard.scrollIntoView({ 
            behavior: 'smooth', 
            block: 'center' 
        });
    }

    // 将后端返回结果规整为安全可渲染的原始字段
    normalizeResultForRender(result) {
        const safe = {
            excel_file: Boolean(result && result.excel_file),
            result_zip: Boolean(result && result.result_zip),
            total_processed: Number((result && result.total_processed) || 0),
            total_failed: Number((result && result.total_failed) || 0),
            series_count: Number((result && result.series_count) || 0),
            total_series: Number((result && result.total_series) || 0),
            total_images: Number((result && result.total_images) || 0),
            duration: Number((result && result.duration) || 0),
            avg_speed: Number((result && result.avg_speed) || 0),
            quality_distribution: {
                normal: Number((result && result.quality_distribution && result.quality_distribution.normal) || 0),
                low_quality: Number((result && result.quality_distribution && result.quality_distribution.low_quality) || 0),
                fixed: Number((result && result.quality_distribution && result.quality_distribution.fixed) || 0),
                unknown: Number((result && result.quality_distribution && result.quality_distribution.unknown) || 0)
            }
        };

        if (result && result.series_info && typeof result.series_info === 'object') {
            try {
                safe.series_count = Object.keys(result.series_info).length;
            } catch (error) {
                // ignore
            }
        }

        return safe;
    }

    // 渲染单个处理结果
    renderSingleResult(result) {
        const seriesCount = Number(result.series_count || 0);
        
        return `
            <div class="row">
                <div class="col-md-6">
                    <h6><i class="fas fa-chart-bar text-primary"></i> 处理统计</h6>
                    <div class="result-stat">
                        <div class="number">${seriesCount}</div>
                        <div class="label">序列数量</div>
                    </div>
                    <div class="result-stat">
                        <div class="number text-success">✓</div>
                        <div class="label">NIfTI转换</div>
                    </div>
                </div>
                <div class="col-md-6">
                    <h6><i class="fas fa-download text-success"></i> 下载文件</h6>
                    <div class="d-grid gap-2">
                        ${result.excel_file ? `
                            <a href="/api/download/${this.currentTask.id}/excel" 
                               class="btn btn-success btn-sm">
                                <i class="fas fa-file-excel"></i> 下载Excel报告
                            </a>
                        ` : ''}
                        ${result.result_zip ? `
                            <a href="/api/download/${this.currentTask.id}/zip" 
                               class="btn btn-primary btn-sm">
                                <i class="fas fa-file-archive"></i> 下载NIfTI文件包
                            </a>
                        ` : ''}
                    </div>
                </div>
            </div>
            <div class="row mt-3">
                <div class="col-12">
                    <div class="alert alert-info">
                        <i class="fas fa-info-circle"></i>
                        <strong>说明：</strong> DICOM文件已自动转换为NIfTI格式(.nii.gz)，便于后续分析处理。原始DICOM文件已被清理以节省存储空间。
                    </div>
                </div>
            </div>
        `;
    }

    // 渲染批量处理结果
    renderBatchResult(result) {
        const total = (result.total_processed || 0) + (result.total_failed || 0);
        const successRate = total > 0 ? ((result.total_processed || 0) / total * 100).toFixed(1) : 0;
        const duration = Number(result.duration || 0);
        const avgSpeed = Number(result.avg_speed || 0);
        const minutes = Math.floor(duration / 60);
        const seconds = Math.floor(duration % 60);
        const durationStr = minutes > 0 ? `${minutes}分${seconds}秒` : `${seconds}秒`;
        
        // 质量分布数据
        const qualityDist = result.quality_distribution || { normal: 0, low_quality: 0, fixed: 0, unknown: 0 };
        const totalQuality = qualityDist.normal + qualityDist.low_quality + qualityDist.fixed + qualityDist.unknown;
        
        // 生成唯一的canvas ID
        const chartId = `qualityChart_${this.currentTask ? this.currentTask.id : Date.now()}`;
        
        // 存储质量数据用于后续图表初始化
        this._batchQualityData = qualityDist;
        this._batchChartId = chartId;
        
        return `
            <div class="batch-report" id="batchReport_${chartId}">
                <!-- 统计概览 -->
                <div class="row mb-4">
                    <div class="col-12">
                        <h6 class="text-primary mb-3"><i class="fas fa-chart-bar"></i> 处理统计概览</h6>
                    </div>
                    <div class="col-6 col-md-3 mb-2">
                        <div class="result-stat">
                            <div class="number text-success">${result.total_processed || 0}</div>
                            <div class="label">成功研究</div>
                        </div>
                    </div>
                    <div class="col-6 col-md-3 mb-2">
                        <div class="result-stat">
                            <div class="number text-danger">${result.total_failed || 0}</div>
                            <div class="label">失败研究</div>
                        </div>
                    </div>
                    <div class="col-6 col-md-3 mb-2">
                        <div class="result-stat">
                            <div class="number text-info">${result.total_series || 0}</div>
                            <div class="label">总序列数</div>
                        </div>
                    </div>
                    <div class="col-6 col-md-3 mb-2">
                        <div class="result-stat">
                            <div class="number text-primary">${result.total_images || 0}</div>
                            <div class="label">总图像数</div>
                        </div>
                    </div>
                </div>
                
                <!-- 时间和速度 -->
                <div class="row mb-4">
                    <div class="col-12">
                        <div class="card bg-light">
                            <div class="card-body py-2">
                                <div class="row text-center">
                                    <div class="col-4">
                                        <small class="text-muted">处理时长</small>
                                        <div class="fw-bold">${durationStr}</div>
                                    </div>
                                    <div class="col-4">
                                        <small class="text-muted">成功率</small>
                                        <div class="fw-bold text-${successRate >= 90 ? 'success' : successRate >= 70 ? 'warning' : 'danger'}">${successRate}%</div>
                                    </div>
                                    <div class="col-4">
                                        <small class="text-muted">平均速度</small>
                                        <div class="fw-bold">${avgSpeed.toFixed(1)} 张/秒</div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
                
                <!-- 质量分布图表 -->
                ${totalQuality > 0 ? `
                <div class="row mb-4">
                    <div class="col-md-6">
                        <h6 class="text-primary mb-3"><i class="fas fa-chart-pie"></i> 质量分布</h6>
                        <div style="max-width: 250px; margin: 0 auto;">
                            <canvas id="${chartId}" data-quality='${JSON.stringify(qualityDist)}'></canvas>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <h6 class="text-primary mb-3"><i class="fas fa-list"></i> 质量详情</h6>
                        <ul class="list-group list-group-flush">
                            <li class="list-group-item d-flex justify-content-between align-items-center py-1">
                                <span><i class="fas fa-check-circle text-success"></i> 正常质量</span>
                                <span class="badge bg-success rounded-pill">${qualityDist.normal}</span>
                            </li>
                            <li class="list-group-item d-flex justify-content-between align-items-center py-1">
                                <span><i class="fas fa-exclamation-circle text-warning"></i> 低质量</span>
                                <span class="badge bg-warning rounded-pill">${qualityDist.low_quality}</span>
                            </li>
                            <li class="list-group-item d-flex justify-content-between align-items-center py-1">
                                <span><i class="fas fa-wrench text-info"></i> 已修复</span>
                                <span class="badge bg-info rounded-pill">${qualityDist.fixed}</span>
                            </li>
                            ${qualityDist.unknown > 0 ? `
                            <li class="list-group-item d-flex justify-content-between align-items-center py-1">
                                <span><i class="fas fa-question-circle text-secondary"></i> 未检测</span>
                                <span class="badge bg-secondary rounded-pill">${qualityDist.unknown}</span>
                            </li>
                            ` : ''}
                        </ul>
                    </div>
                </div>
                ` : ''}
                
                <!-- 下载按钮 -->
                <div class="mt-3">
                    <h6 class="text-success mb-3"><i class="fas fa-download"></i> 下载批量结果</h6>
                    <div class="d-grid">
                        ${result.result_zip ? `
                            <a href="/api/download/${this.currentTask.id}/zip" 
                               class="btn btn-primary">
                                <i class="fas fa-file-archive"></i> 下载批量结果ZIP
                            </a>
                        ` : ''}
                    </div>
                </div>
            </div>
        `;
    }

    // 初始化批量报告图表（在结果渲染后调用）
    initBatchReportChart() {
        if (!this._batchChartId || !this._batchQualityData) return;
        
        const canvas = document.getElementById(this._batchChartId);
        if (!canvas || !window.Chart) return;
        
        const qualityDist = this._batchQualityData;
        
        new Chart(canvas, {
            type: 'pie',
            data: {
                labels: ['正常', '低质量', '已修复', '未检测'],
                datasets: [{
                    data: [qualityDist.normal, qualityDist.low_quality, qualityDist.fixed, qualityDist.unknown],
                    backgroundColor: ['#198754', '#ffc107', '#0dcaf0', '#6c757d'],
                    borderWidth: 1
                }]
            },
            options: {
                responsive: true,
                plugins: {
                    legend: {
                        position: 'bottom',
                        labels: {
                            boxWidth: 12,
                            font: { size: 11 }
                        }
                    }
                }
            }
        });
        
        // 清理
        this._batchChartId = null;
        this._batchQualityData = null;
    }

    // 渲染上传文件结果
    renderUploadResult(result) {
        return `
            <div class="row">
                <div class="col-md-6">
                    <h6><i class="fas fa-chart-bar text-primary"></i> 处理统计</h6>
                    <div class="result-stat">
                        <div class="number">${result.series_count || 0}</div>
                        <div class="label">发现序列</div>
                    </div>
                    <div class="result-stat">
                        <div class="number text-success">✓</div>
                        <div class="label">NIfTI转换</div>
                    </div>
                </div>
                <div class="col-md-6">
                    <h6><i class="fas fa-download text-success"></i> 下载文件</h6>
                    <div class="d-grid gap-2">
                        ${result.excel_file ? `
                            <a href="/api/download/${this.currentTask.id}/excel" 
                               class="btn btn-success btn-sm">
                                <i class="fas fa-file-excel"></i> 下载Excel报告
                            </a>
                        ` : ''}
                        ${result.result_zip ? `
                            <a href="/api/download/${this.currentTask.id}/zip" 
                               class="btn btn-primary btn-sm">
                                <i class="fas fa-file-archive"></i> 下载NIfTI文件包
                            </a>
                        ` : ''}
                    </div>
                </div>
            </div>
            <div class="row mt-3">
                <div class="col-12">
                    <div class="alert alert-info">
                        <i class="fas fa-info-circle"></i>
                        <strong>说明：</strong> 上传的DICOM文件已自动转换为NIfTI格式(.nii.gz)，每个序列生成一个NIfTI文件。原始DICOM文件已被清理。
                    </div>
                </div>
            </div>
        `;
    }

    // 文件选择处理
    handleFileSelection(file) {
        this.selectedFile = file;
        
        document.getElementById('fileName').textContent = file.name;
        document.getElementById('fileSize').textContent = this.formatFileSize(file.size);
        document.getElementById('selectedFile').style.display = 'block';
        document.getElementById('uploadProcessBtn').disabled = false;
    }

    // 清除文件选择
    clearFileSelection() {
        this.selectedFile = null;
        document.getElementById('selectedFile').style.display = 'none';
        document.getElementById('uploadProcessBtn').disabled = true;
        document.getElementById('zipFile').value = '';
    }

    // 格式化文件大小
    formatFileSize(bytes) {
        if (bytes === 0) return '0 Bytes';
        const k = 1024;
        const sizes = ['Bytes', 'KB', 'MB', 'GB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    }

    // 清空输入
    clearInput(elementId) {
        document.getElementById(elementId).value = '';
        if (elementId === 'zipFile') {
            this.clearFileSelection();
        }
    }

    // 重置所有选项
    resetAllOptions() {
        document.getElementById('autoExtract').checked = true;
        document.getElementById('autoOrganize').checked = true;
        document.getElementById('autoMetadata').checked = true;
        document.getElementById('keepZip').checked = true;
        document.getElementById('keepExtracted').checked = false;
        document.getElementById('defaultKeywords').checked = true;
        document.getElementById('customKeywordFile').style.display = 'none';
        
        this.showSuccess('选项已重置为默认值');
    }

    // 导出配置
    exportSettings() {
        const settings = {
            options: this.getProcessingOptions(),
            keywordConfig: document.querySelector('input[name="keywordConfig"]:checked').value,
            timestamp: new Date().toISOString()
        };
        
        const blob = new Blob([JSON.stringify(settings, null, 2)], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `dicom_settings_${Date.now()}.json`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
        
        this.showSuccess('配置已导出');
    }

    // 显示成功消息
    showSuccess(message) {
        document.getElementById('successMessage').textContent = message;
        if (!this.successModal) {
            const successModalEl = document.getElementById('successModal');
            if (successModalEl) {
                this.successModal = bootstrap.Modal.getOrCreateInstance(successModalEl);
            }
        }
        if (this.successModal) {
            this.successModal.show();
        }
    }

    // 显示错误消息
    showError(message) {
        document.getElementById('errorMessage').textContent = message;
        if (!this.errorModal) {
            const errorModalEl = document.getElementById('errorModal');
            if (errorModalEl) {
                this.errorModal = bootstrap.Modal.getOrCreateInstance(errorModalEl);
            }
        }
        if (this.errorModal) {
            this.errorModal.show();
        }
    }

    // HTML转义
    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    // ==================== 监控页面功能 ====================

    // 加载监控数据
    async loadMonitoringData() {
        try {
            const response = await fetch('/api/system/monitoring');
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            const data = await response.json();
            this.updateMonitoringUI(data);

            // 更新最后更新时间
            const now = new Date();
            const timeStr = now.toLocaleTimeString();
            const lastUpdateEl = document.getElementById('lastUpdateTime');
            if (lastUpdateEl) {
                lastUpdateEl.textContent = timeStr;
            }
        } catch (error) {
            console.error('加载监控数据失败:', error);
        }
    }

    // 更新监控页面UI
    updateMonitoringUI(data) {
        const t = this.translations[this.currentLang];

        // 更新磁盘使用
        if (data.disk && !data.disk.error) {
            const diskPercent = data.disk.percent_used || 0;
            const diskBar = document.getElementById('diskProgressBar');
            if (diskBar) {
                diskBar.style.width = `${diskPercent}%`;
                diskBar.className = `progress-bar ${diskPercent > 90 ? 'bg-danger' : diskPercent > 70 ? 'bg-warning' : 'bg-primary'}`;
            }
            const diskUsedEl = document.getElementById('diskUsed');
            if (diskUsedEl) diskUsedEl.textContent = data.disk.used_gb || '--';
            const diskTotalEl = document.getElementById('diskTotal');
            if (diskTotalEl) diskTotalEl.textContent = data.disk.total_gb || '--';
            const diskFreeEl = document.getElementById('diskFree');
            if (diskFreeEl) diskFreeEl.textContent = `${t['available'] || 'Free'}: ${data.disk.free_gb || '--'} GB`;
        }

        // 更新内存使用
        if (data.memory && !data.memory.error) {
            const memPercent = data.memory.percent_used || 0;
            const memBar = document.getElementById('memoryProgressBar');
            if (memBar) {
                memBar.style.width = `${memPercent}%`;
            }
            const memUsedEl = document.getElementById('memoryUsed');
            if (memUsedEl) memUsedEl.textContent = memPercent;
            const memAvailableEl = document.getElementById('memoryAvailable');
            if (memAvailableEl) memAvailableEl.textContent = data.memory.available_gb || '--';
        }

        // 更新CPU使用
        if (data.cpu && !data.cpu.error) {
            const cpuPercentEl = document.getElementById('cpuPercent');
            if (cpuPercentEl) cpuPercentEl.textContent = `${data.cpu.percent || '--'}%`;
            const cpuCoresEl = document.getElementById('cpuCores');
            if (cpuCoresEl) cpuCoresEl.textContent = data.cpu.count || '--';
        }

        // 更新PACS状态
        if (data.pacs_connection) {
            const pacsBadge = document.getElementById('pacsStatusBadge');
            if (pacsBadge) {
                if (data.pacs_connection.connected) {
                    pacsBadge.innerHTML = `<span class="badge bg-success"><i class="fas fa-check"></i> ${t['normal'] || 'Normal'}</span>`;
                } else {
                    pacsBadge.innerHTML = `<span class="badge bg-danger"><i class="fas fa-times"></i> ${t['error'] || 'Error'}</span>`;
                }
            }
            const pacsSummary = document.getElementById('pacsConfigSummary');
            if (pacsSummary && data.pacs_connection.config) {
                const cfg = data.pacs_connection.config;
                pacsSummary.textContent = `${cfg.called_aet} @ ${cfg.pacs_ip}:${cfg.pacs_port}`;
            }
        }

        // 更新任务统计
        if (data.task_summary) {
            const statTotal = document.getElementById('statTotal');
            if (statTotal) statTotal.textContent = data.task_summary.total || 0;
            const statRunning = document.getElementById('statRunning');
            if (statRunning) statRunning.textContent = data.task_summary.running || 0;
            const statPending = document.getElementById('statPending');
            if (statPending) statPending.textContent = data.task_summary.pending || 0;
            const statCompleted = document.getElementById('statCompleted');
            if (statCompleted) statCompleted.textContent = data.task_summary.completed || 0;
            const statFailed = document.getElementById('statFailed');
            if (statFailed) statFailed.textContent = data.task_summary.failed || 0;
            const statCancelled = document.getElementById('statCancelled');
            if (statCancelled) statCancelled.textContent = data.task_summary.cancelled || 0;
        }

        // 更新活跃任务列表
        this.updateActiveTasksTable(data.active_tasks || [], t);

        // 更新目录使用
        this.updateDirectoryUsage(data.directories || {}, t);

        // 更新近期完成任务
        this.updateRecentCompletedTable(data.recent_completed || [], t);

        // 更新日志文件列表
        this.updateLogFilesTable(data.log_files || [], t);
    }

    // 更新活跃任务表格
    updateActiveTasksTable(tasks, t) {
        const tbody = document.getElementById('activeTasksBody');
        if (!tbody) return;

        if (tasks.length === 0) {
            tbody.innerHTML = `<tr><td colspan="7" class="text-center text-muted">${t['no_active_tasks'] || 'No active tasks'}</td></tr>`;
            return;
        }

        tbody.innerHTML = tasks.map(task => {
            const elapsed = task.elapsed_seconds ? this.formatElapsedTime(task.elapsed_seconds) : '--';
            const progressBar = `
                <div class="progress" style="height: 6px;">
                    <div class="progress-bar" role="progressbar" style="width: ${task.progress || 0}%"></div>
                </div>
                <small>${task.progress || 0}%</small>
            `;
            const statusBadge = task.status === 'running'
                ? '<span class="badge bg-primary">Running</span>'
                : '<span class="badge bg-warning">Pending</span>';

            let taskInfo = '';
            if (task.type === 'single' && task.parameters.accession_number) {
                taskInfo = `<small class="text-muted">${this.escapeHtml(task.parameters.accession_number)}</small>`;
            } else if (task.type === 'batch' && task.parameters.batch_count) {
                taskInfo = `<small class="text-muted">${task.parameters.batch_count} items</small>`;
            }

            return `
                <tr>
                    <td><small>${task.task_id.substring(0, 8)}...</small></td>
                    <td>${task.type}${taskInfo ? '<br>' + taskInfo : ''}</td>
                    <td>${statusBadge}</td>
                    <td>${progressBar}</td>
                    <td><small>${this.escapeHtml(task.current_step || '--')}</small></td>
                    <td><small>${elapsed}</small></td>
                    <td>
                        <button class="btn btn-sm btn-outline-danger" onclick="cancelTaskById('${task.task_id}')">
                            <i class="fas fa-stop"></i> ${t['cancel'] || 'Cancel'}
                        </button>
                    </td>
                </tr>
            `;
        }).join('');
    }

    // 更新目录使用显示
    updateDirectoryUsage(directories, t) {
        const container = document.getElementById('directoryUsageContainer');
        if (!container) return;

        const dirNames = {
            'results': { icon: 'fa-folder', color: 'primary', label: 'Results' },
            'uploads': { icon: 'fa-upload', color: 'info', label: 'Uploads' },
            'temp': { icon: 'fa-temp', color: 'warning', label: 'Temp' },
            'logs': { icon: 'fa-file-alt', color: 'secondary', label: 'Logs' }
        };

        let html = '';
        for (const [key, info] of Object.entries(directories)) {
            const config = dirNames[key] || { icon: 'fa-folder', color: 'secondary', label: key };
            html += `
                <div class="col-md-3 col-sm-6 mb-3">
                    <div class="d-flex align-items-center">
                        <i class="fas ${config.icon} fa-2x text-${config.color} me-2"></i>
                        <div>
                            <div class="small text-muted">${config.label}</div>
                            <div class="fw-bold">${info.size_gb} GB</div>
                        </div>
                    </div>
                </div>
            `;
        }

        container.innerHTML = html || `<div class="col-12 text-muted text-center">${t['no_data'] || 'No data'}</div>`;
    }

    // 更新近期完成任务表格
    updateRecentCompletedTable(tasks, t) {
        const tbody = document.getElementById('recentCompletedBody');
        if (!tbody) return;

        if (tasks.length === 0) {
            tbody.innerHTML = `<tr><td colspan="4" class="text-center text-muted">${t['no_recent_tasks'] || 'No recent tasks'}</td></tr>`;
            return;
        }

        const statusColors = {
            'completed': 'success',
            'failed': 'danger',
            'cancelled': 'secondary'
        };

        tbody.innerHTML = tasks.map(task => {
            const statusColor = statusColors[task.status] || 'secondary';
            const duration = task.elapsed_seconds ? this.formatElapsedTime(task.elapsed_seconds) : '--';

            return `
                <tr>
                    <td><small>${task.task_id.substring(0, 8)}...</small></td>
                    <td>${task.type}</td>
                    <td><span class="badge bg-${statusColor}">${task.status}</span></td>
                    <td>${duration}</td>
                </tr>
            `;
        }).join('');
    }

    // 更新日志文件列表
    updateLogFilesTable(logFiles, t) {
        const tbody = document.getElementById('logFilesBody');
        if (!tbody) return;

        if (logFiles.length === 0) {
            tbody.innerHTML = `<tr><td colspan="4" class="text-center text-muted">${t['no_log_files'] || 'No log files'}</td></tr>`;
            return;
        }

        tbody.innerHTML = logFiles.map(log => {
            const modifiedDate = new Date(log.modified * 1000).toLocaleString();
            return `
                <tr>
                    <td><small>${this.escapeHtml(log.name)}</small></td>
                    <td><small>${log.size_mb} MB</small></td>
                    <td><small>${modifiedDate}</small></td>
                    <td>
                        <a href="/api/logs/download/${encodeURIComponent(log.name)}" class="btn btn-sm btn-outline-primary" download>
                            <i class="fas fa-download"></i> ${t['download_log'] || 'Download'}
                        </a>
                    </td>
                </tr>
            `;
        }).join('');
    }

    // 格式化已运行时间
    formatElapsedTime(seconds) {
        if (seconds < 60) {
            return `${Math.floor(seconds)}s`;
        } else if (seconds < 3600) {
            return `${Math.floor(seconds / 60)}m ${Math.floor(seconds % 60)}s`;
        } else {
            const hours = Math.floor(seconds / 3600);
            const mins = Math.floor((seconds % 3600) / 60);
            return `${hours}h ${mins}m`;
        }
    }

    // 切换自动刷新
    toggleAutoRefresh() {
        if (this.monitoringAutoRefreshInterval) {
            clearInterval(this.monitoringAutoRefreshInterval);
            this.monitoringAutoRefreshInterval = null;
            const icon = document.getElementById('autoRefreshIcon');
            const text = document.getElementById('autoRefreshText');
            if (icon) icon.className = 'fas fa-play';
            if (text) text.textContent = this.translations[this.currentLang]['auto_refresh'] || 'Auto';
        } else {
            this.loadMonitoringData();
            this.monitoringAutoRefreshInterval = setInterval(() => {
                this.loadMonitoringData();
            }, 5000);
            const icon = document.getElementById('autoRefreshIcon');
            const text = document.getElementById('autoRefreshText');
            if (icon) icon.className = 'fas fa-pause';
            if (text) text.textContent = this.translations[this.currentLang]['stop_auto_refresh'] || 'Stop';
        }
    }

    // 取消指定任务
    async cancelTaskById(taskId) {
        const t = this.translations[this.currentLang];
        if (confirm(t['confirm_cancel_task'] || 'Are you sure you want to cancel this task?')) {
            try {
                const response = await fetch(`/api/task/${taskId}/cancel`, {
                    method: 'POST'
                });

                if (response.ok) {
                    this.showSuccess(t['task_cancelled'] || 'Task cancelled');
                    this.loadMonitoringData();
                } else {
                    const data = await response.json();
                    this.showError(data.error || t['cancel_task_failed'] || 'Failed to cancel task');
                }
            } catch (error) {
                this.showError((t['network_error'] || 'Network error: ') + error.message);
            }
        }
    }
}

// 全局函数（供HTML调用）
let processor;

// 页面加载完成后初始化
document.addEventListener('DOMContentLoaded', function() {
    processor = new DICOMProcessor();
});

// 全局函数定义
function startSingleProcess() {
    processor.startSingleProcess();
}

function startBatchProcess() {
    processor.startBatchProcess();
}

function startUploadProcess() {
    processor.startUploadProcess();
}

function cancelCurrentTask() {
    processor.cancelCurrentTask();
}

function handleFileSelect(input) {
    if (input.files && input.files[0]) {
        processor.handleFileSelection(input.files[0]);
    }
}

function clearInput(elementId) {
    processor.clearInput(elementId);
}

function clearFileSelection() {
    processor.clearFileSelection();
}

function resetAllOptions() {
    processor.resetAllOptions();
}

function exportSettings() {
    processor.exportSettings();
}

function cancelTaskById(taskId) {
    processor.cancelTaskById(taskId);
}