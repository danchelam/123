import eventlet
eventlet.monkey_patch()

from flask import Flask, render_template, request
from flask_socketio import SocketIO, emit
import threading
import time
import os
import sys
import importlib.util
import re
import json
import urllib.request
import urllib.error

# PyInstaller 资源路径处理
def get_resource_path(relative_path):
    if hasattr(sys, '_MEIPASS'):
        return os.path.join(sys._MEIPASS, relative_path)
    return os.path.join(os.path.abspath("."), relative_path)

# 确保 templates 目录被 Flask 正确找到
template_dir = get_resource_path(os.path.join("pythonProject", "cursor", "贾老板", "templates"))
if not os.path.exists(template_dir):
    # 如果是在开发环境，或者打包结构不同，尝试上一级或当前目录
    template_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")

app = Flask(__name__, template_folder=template_dir)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app, async_mode='eventlet')

# 自动更新配置（仅更新外部脚本 aixcrypto测试版.py）
# 你需要把下面两个 URL 替换成自己的 GitHub Raw 地址
CHECK_UPDATE_ON_START = True
UPDATE_META_URL = "https://raw.githubusercontent.com/danchelam/123/refs/heads/main/version.json"
UPDATE_SCRIPT_URL = "https://raw.githubusercontent.com/danchelam/123/refs/heads/main/aixcrypto%E6%B5%8B%E8%AF%95%E7%89%88.py"

def get_base_dir():
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))

def get_local_script_path():
    return os.path.join(get_base_dir(), "aixcrypto测试版.py")

def read_local_version(script_path: str) -> str:
    if not os.path.exists(script_path):
        return "0"
    try:
        with open(script_path, "r", encoding="utf-8") as f:
            content = f.read()
        m = re.search(r"__version__\s*=\s*['\"]([^'\"]+)['\"]", content)
        return m.group(1) if m else "0"
    except Exception:
        return "0"

def parse_version(v: str):
    # 将版本号分解为数字元组，例如 "2025.01.21" -> (2025, 1, 21)
    nums = re.findall(r"\d+", v)
    return tuple(int(x) for x in nums) if nums else (0,)

def fetch_remote_version() -> str:
    if not UPDATE_META_URL:
        return ""
    try:
        with urllib.request.urlopen(UPDATE_META_URL, timeout=10) as resp:
            data = resp.read().decode("utf-8").strip()
        # 支持 JSON 或纯文本
        if data.startswith("{"):
            obj = json.loads(data)
            return str(obj.get("version", "")).strip()
        return data
    except Exception:
        return ""

def download_new_script() -> str:
    if not UPDATE_SCRIPT_URL:
        return ""
    try:
        with urllib.request.urlopen(UPDATE_SCRIPT_URL, timeout=20) as resp:
            return resp.read().decode("utf-8")
    except Exception:
        return ""

def try_auto_update():
    if not CHECK_UPDATE_ON_START:
        return
    if not UPDATE_META_URL or not UPDATE_SCRIPT_URL:
        print("【更新】未配置 UPDATE_META_URL 或 UPDATE_SCRIPT_URL，跳过自动更新。")
        return

    local_path = get_local_script_path()
    local_version = read_local_version(local_path)
    remote_version = fetch_remote_version()
    if not remote_version:
        print("【更新】无法获取远程版本号，跳过自动更新。")
        return

    if parse_version(remote_version) <= parse_version(local_version):
        print(f"【更新】当前版本已是最新：{local_version}")
        return

    print(f"【更新】发现新版本：{remote_version}（本地：{local_version}），开始更新...")
    new_code = download_new_script()
    if not new_code:
        print("【更新】下载脚本失败，取消更新。")
        return

    try:
        # 备份旧脚本
        if os.path.exists(local_path):
            backup_path = local_path + ".bak"
            with open(local_path, "r", encoding="utf-8") as old_f:
                old_content = old_f.read()
            with open(backup_path, "w", encoding="utf-8") as bak_f:
                bak_f.write(old_content)

        with open(local_path, "w", encoding="utf-8") as f:
            f.write(new_code)
        print("【更新】脚本更新成功。")
    except Exception as e:
        print(f"【更新】写入脚本失败: {e}")

# 动态加载核心逻辑模块 (支持热更新)
def load_core_module():
    # 1. 优先在 EXE/脚本 同级目录找 py 文件
    if getattr(sys, 'frozen', False):
        base_dir = os.path.dirname(sys.executable)
    else:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        
    script_path = os.path.join(base_dir, "aixcrypto测试版.py")
    
    if os.path.exists(script_path):
        print(f"【热更新】检测到外部脚本，优先加载: {script_path}")
        try:
            spec = importlib.util.spec_from_file_location("core_module", script_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            return module
        except Exception as e:
            print(f"【热更新】加载外部脚本失败: {e}，将回退到内置版本。")
    
    # 2. 如果没有或加载失败，才加载打包在内部的 (作为兜底)
    print("【系统】加载内置脚本版本。")
    try:
        from aixcrypto测试版 import (
            AdsBrowserManager, 
            run_account_task, 
            ADSPOWER_API_KEY, 
            stop_all_tasks, 
            set_logger_callback,
            is_account_completed,
            STOP_FLAG
        )
        import aixcrypto测试版 as module
        return module
    except ImportError:
        # 开发环境路径兼容
        sys.path.append(os.path.dirname(os.path.abspath(__file__)))
        import aixcrypto测试版 as module
        return module

# 启动时自动检查更新（仅更新外部脚本）
try_auto_update()

# 初始化加载模块
core_module = load_core_module()

# 全局变量控制任务
task_thread = None
is_task_running = False

def log_emitter(msg):
    """
    回调函数：将日志通过 WebSocket 发送给前端
    """
    socketio.emit('new_log', msg)

# 设置脚本的日志回调
if core_module:
    core_module.set_logger_callback(log_emitter)

def run_batch_logic(thread_count):
    global is_task_running
    
    # 每次运行前重新加载模块，实现真正的“热”更新（无需重启EXE）
    global core_module
    core_module = load_core_module()
    if core_module:
        core_module.set_logger_callback(log_emitter)
    else:
        log_emitter("无法加载核心模块！")
        is_task_running = False
        socketio.emit('status_update', {'running': False})
        return

    # 重置停止标志
    core_module.STOP_FLAG = False
    
    # 修改：兼容打包后的路径或当前路径寻找 shuju.xlsx
    excel_path = "shuju.xlsx"
    if getattr(sys, 'frozen', False):
        # 打包后，从 EXE 所在目录寻找
        base_dir = os.path.dirname(sys.executable)
        excel_path = os.path.join(base_dir, "shuju.xlsx")
    elif not os.path.exists(excel_path):
        # 开发环境：尝试在上一级目录找
        base_dir = os.path.dirname(os.path.abspath(__file__))
        excel_path = os.path.join(base_dir, "shuju.xlsx")
    
    log_emitter(f"正在加载 Excel: {os.path.abspath(excel_path)}")
    manager = core_module.AdsBrowserManager(excel_path=excel_path, api_key=core_module.ADSPOWER_API_KEY)
    all_accounts = manager.get_account_list()
    
    if not all_accounts:
        log_emitter("错误：未找到账号，请检查 shuju.xlsx")
        is_task_running = False
        socketio.emit('status_update', {'running': False})
        return

    log_emitter(f"共加载 {len(all_accounts)} 个账号，并发数: {thread_count}")

    from concurrent.futures import ThreadPoolExecutor
    import threading
    
    semaphore = threading.Semaphore(thread_count)
    futures = []

    try:
        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            for account in all_accounts:
                if core_module.STOP_FLAG:
                    log_emitter("停止信号已接收，停止提交新任务...")
                    break
                
                # 信号量控制并发提交
                semaphore.acquire()
                
                if core_module.is_account_completed(account.id):
                    log_emitter(f"[{account.id}] 任务已完成，跳过")
                    semaphore.release()
                    continue
                
                if core_module.STOP_FLAG:
                     semaphore.release()
                     break

                def task_wrapper(acc, key):
                    try:
                        core_module.run_account_task(acc, api_key=key)
                    except Exception as e:
                        log_emitter(f"[{acc.id}] 执行异常: {e}")
                    finally:
                        semaphore.release()

                future = executor.submit(task_wrapper, account, core_module.ADSPOWER_API_KEY)
                futures.append(future)
                time.sleep(2) # 间隔启动

            # 等待所有已提交的任务完成
            for future in futures:
                if core_module.STOP_FLAG:
                    break # 如果强制停止，不再等待结果（虽然线程还在跑）
                try:
                    future.result()
                except Exception:
                    pass
    except Exception as e:
        log_emitter(f"批量任务异常: {e}")
    finally:
        is_task_running = False
        socketio.emit('status_update', {'running': False})
        log_emitter("所有任务已结束或被停止。")

@app.route('/')
def index():
    return render_template('index.html')

@socketio.on('start_task')
def handle_start_task(data):
    global task_thread, is_task_running
    if is_task_running:
        emit('new_log', "任务已经在运行中...")
        return

    try:
        threads = int(data.get('threads', 2))
    except:
        threads = 2

    is_task_running = True
    emit('status_update', {'running': True})
    
    task_thread = socketio.start_background_task(target=run_batch_logic, thread_count=threads)

@socketio.on('stop_task')
def handle_stop_task():
    global is_task_running
    if not is_task_running:
        return
    
    emit('new_log', "正在发送停止信号...")
    if core_module:
        core_module.stop_all_tasks() # 设置 STOP_FLAG = True

@socketio.on('shutdown_server')
def handle_shutdown_server():
    emit('new_log', "正在关闭程序...")
    # 延迟关闭，让前端能收到消息
    def kill():
        time.sleep(1)
        os._exit(0)
    threading.Thread(target=kill).start()

if __name__ == '__main__':
    # 自动打开浏览器
    print("启动 Web UI 服务...")
    print("请在浏览器访问: http://127.0.0.1:5000")
    
    # 延迟打开浏览器，确保服务已启动
    def open_browser():
        time.sleep(1.5)
        import webbrowser
        webbrowser.open("http://127.0.0.1:5000")
    threading.Thread(target=open_browser).start()
    
    socketio.run(app, debug=False, port=5000)