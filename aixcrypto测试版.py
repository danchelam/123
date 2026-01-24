"""
AdsPower浏览器批量自动化操作模块
- 支持自定义API地址
- 内部集成shuju.xlsx账号信息批量读取
- 提供批量账号管理、单账号启动/关闭等接口
"""

from DrissionPage import ChromiumPage
from typing import Optional, Dict, List, Union
import pandas as pd
import os
import sys
import socket
import random
import threading
import datetime
import json
from concurrent.futures import ThreadPoolExecutor

# 引入前段框架里的 OKX 解锁模块
_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_FRAMEWORK_DIR = os.path.normpath(os.path.join(_BASE_DIR, "..", "前段框架"))
if _FRAMEWORK_DIR not in sys.path:
    sys.path.append(_FRAMEWORK_DIR)
from okx_wallet import OKXWallet

# 版本号（用于自动更新比较）
__version__ = "2026.01.31"

# 全局API地址参数
ADSPOWER_API_BASE_URL = "http://127.0.0.1:50325"
ADSPOWER_API_KEY = "5b9664bf3e65c5a0622d1b5d0d766eac"

def get_completed_tasks_file():
    # 判断是否在打包后的 EXE 环境中运行
    if getattr(sys, 'frozen', False):
        base_dir = os.path.dirname(sys.executable)
    else:
        base_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base_dir, "completed_tasks.json")

COMPLETED_TASKS_FILE = get_completed_tasks_file()
print_lock = threading.Lock()
file_lock = threading.Lock()

# 全局停止标志
STOP_FLAG = False
# 日志回调函数 (用于Web端推送)
logger_callback = None

def set_logger_callback(callback):
    global logger_callback
    logger_callback = callback

def log(account_id: str, msg: str):
    now = datetime.datetime.now().strftime("%H:%M:%S")
    full_msg = f"[{now}] [窗口 {account_id}] {msg}"
    with print_lock:
        print(full_msg)
    # 如果有回调，推送日志
    if logger_callback:
        try:
            logger_callback(full_msg)
        except Exception:
            pass

def stop_all_tasks():
    global STOP_FLAG
    STOP_FLAG = True

def get_task_cycle_start_time() -> datetime.datetime:
    """
    获取当前任务周期的起始时间（每天早上 8:00 重置）。
    如果当前时间 >= 8:00，则起始时间为今天的 8:00。
    如果当前时间 < 8:00，则起始时间为昨天的 8:00。
    """
    now = datetime.datetime.now()
    today_8am = now.replace(hour=8, minute=0, second=0, microsecond=0)
    if now >= today_8am:
        return today_8am
    else:
        return today_8am - datetime.timedelta(days=1)

def load_completed_tasks() -> Dict[str, float]:
    if not os.path.exists(COMPLETED_TASKS_FILE):
        return {}
    try:
        with open(COMPLETED_TASKS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}

def save_completed_task(account_id: str):
    with file_lock:
        data = load_completed_tasks()
        data[account_id] = datetime.datetime.now().timestamp()
        try:
            with open(COMPLETED_TASKS_FILE, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4)
        except Exception as e:
            print(f"保存任务完成记录失败: {e}")

def is_account_completed(account_id: str) -> bool:
    data = load_completed_tasks()
    last_ts = data.get(account_id)
    if not last_ts:
        return False
    
    last_time = datetime.datetime.fromtimestamp(last_ts)
    cycle_start = get_task_cycle_start_time()
    
    # 如果完成时间晚于当前周期的起始时间，则视为已完成
    return last_time > cycle_start

class AccountInfo:
    """
    账号信息数据结构
    """

    def __init__(self, id: str, ua: str, proxy: str = ""):
        self.id = id
        self.ua = ua
        self.proxy = proxy


class AdsBrowserManager:
    """
    AdsPower浏览器批量管理器
    """

    def __init__(
        self,
        api_base_url: str = ADSPOWER_API_BASE_URL,
        excel_path: str = "shuju.xlsx",
        api_key: str = "",
    ):
        """
        Initializes the AdsBrowserManager.
        :param api_base_url: The base URL for the AdsPower API.
        :param excel_path: The absolute path to the shuju.xlsx file.
        """
        self.api_base_url = api_base_url
        self.api_key = api_key
        # 支持 .xlsx 或 .csv；若给了 .xlsx 但不存在，尝试同名 .csv
        base, ext = os.path.splitext(excel_path)
        chosen = excel_path
        if not os.path.exists(chosen):
            alt_csv = base + ".csv"
            if os.path.exists(alt_csv):
                chosen = alt_csv
        self.excel_path = chosen
        self.accounts: List[AccountInfo] = []
        self.load_accounts()

    def load_accounts(self):
        """
        读取Excel文件，加载所有账号信息到self.accounts
        """
        try:
            print(f"正在从路径加载账号数据文件: {self.excel_path}")
            if str(self.excel_path).lower().endswith(".csv"):
                df = pd.read_csv(self.excel_path, dtype=str, encoding="utf-8", keep_default_na=False)
            else:
                df = pd.read_excel(self.excel_path, dtype=str)
                df = df.fillna("")
            print("读取到的Excel内容 (前5行):")
            print(df.head())

            def _sv(x) -> str:
                try:
                    if x is None:
                        return ""
                    return str(x).strip()
                except Exception:
                    return ""

            for _, row in df.iterrows():
                # id 支持多种列名：id / user_id / acc_id
                id_val = _sv(row.get("id", ""))
                if not id_val:
                    id_val = _sv(row.get("user_id", ""))
                if not id_val:
                    id_val = _sv(row.get("acc_id", ""))

                ua_val = _sv(row.get("ua", ""))
                proxy_val = _sv(row.get("proxy", "")) if "proxy" in row else ""

                # 仅要求有 id 即可，其余可为空
                if id_val:
                    self.accounts.append(AccountInfo(id=id_val, ua=ua_val, proxy=proxy_val))

            print(f"最终加载账号数量: {len(self.accounts)}")

        except Exception as e:
            print(f"加载账号信息失败: {e}")
            print(f"Excel路径: {self.excel_path}")

    def get_account_list(self) -> List[AccountInfo]:
        """
        获取所有账号信息列表
        :return: List[AccountInfo]
        """
        return self.accounts

    def start_browser_with_addr(self, account: AccountInfo):
        """
        启动指定账号的 AdsPower 浏览器，并返回 (ChromiumPage, connection_address)。
        :return: Tuple[Optional[ChromiumPage], Optional[str]]
        """
        import requests
        import time

        user_id = account.id
        start_url = f"{self.api_base_url}/api/v1/browser/start?user_id={user_id}"
        headers = {"X-Api-Key": self.api_key} if self.api_key else None

        for attempt in range(5):
            try:
                resp = requests.get(start_url, timeout=60, headers=headers)
                resp.raise_for_status()
                api_data = resp.json()

                if api_data.get("code") == 0 and "data" in api_data and "debug_port" in api_data["data"]:
                    debug_port_info = api_data["data"]["debug_port"]
                    if ":" in str(debug_port_info):
                        connection_address = debug_port_info
                    else:
                        connection_address = f"127.0.0.1:{debug_port_info}"
                    time.sleep(1)
                    page = ChromiumPage(addr_or_opts=connection_address, timeout=10)
                    return page, connection_address

                elif "Too many request" in api_data.get("msg", ""):
                    if attempt < 4:
                        wait_time = (attempt + 1) * 2
                        log(user_id, f"遭遇API速率限制，将在 {wait_time} 秒后进行第 {attempt + 2} 次尝试...")
                        time.sleep(wait_time)
                        continue
                    else:
                        log(user_id, "在5次尝试后仍因速率限制无法启动浏览器。")
                        return None, None

                else:
                    log(user_id, f"AdsPower API启动失败: {api_data.get('msg', '未知错误')}")
                    return None, None

            except Exception as e:
                log(user_id, f"启动AdsPower浏览器时发生连接异常: {e}")
                return None, None

        return None, None

    def start_browser(self, account: AccountInfo) -> Optional[ChromiumPage]:
        page, _ = self.start_browser_with_addr(account)
        return page

    def close_browser(self, user_id: str) -> bool:
        import requests

        stop_url = f"{self.api_base_url}/api/v1/browser/stop?user_id={user_id}"
        try:
            headers = {"X-Api-Key": self.api_key} if self.api_key else None
            resp = requests.get(stop_url, timeout=30, headers=headers)
            resp.raise_for_status()
            api_data = resp.json()
            if api_data.get("code") == 0:
                log(user_id, "成功关闭浏览器")
                return True
            else:
                log(user_id, f"关闭浏览器API调用失败: {api_data.get('msg', '未知错误')}")
                return False
        except Exception as e:
            log(user_id, f"关闭浏览器时发生异常: {e}")
            return False

    def batch_start_all(self) -> List[Optional[ChromiumPage]]:
        """
        批量启动所有账号的浏览器，返回所有ChromiumPage对象列表
        :return: List[Optional[ChromiumPage]]
        """
        pass

    def batch_close_all(self, pages: List[ChromiumPage]):
        """
        批量关闭所有浏览器实例
        :param pages: ChromiumPage对象列表
        """
        pass


def _try_detect_and_click(page_or_tab: Union[ChromiumPage, object], selector: str, account_id: str = "Unknown", timeout: int = 10) -> bool:
    """
    简单检测并点击元素（用于测试阶段）。
    """
    try:
        # DrissionPage 4.1.1.2 直接用 ele() 并传入 timeout
        ele = page_or_tab.ele(selector, timeout=timeout)
        if ele:
            try:
                try:
                    ele.scroll.to_see()
                except Exception:
                    pass
                ele.click()
            except Exception:
                ele.click(by_js=True)
            log(account_id, f"已点击元素: {selector}")
            return True
        # log(account_id, f"未找到元素: {selector}")
        return False
    except Exception as e:
        log(account_id, f"检测/点击元素异常: {e}")
        return False


def _is_port_open(addr: str, timeout: float = 1.0) -> bool:
    try:
        host, port_str = addr.split(":")
        port = int(port_str)
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception:
        return False


def _check_wallet_login_state(page: ChromiumPage, account_id: str, timeout: int = 3) -> str:
    """
    检测钱包登录状态：
    - 若存在 Not Connected 文本元素，判定为未登录
    - 若存在包含地址的元素，判定为已登录
    :return: "logged_in" | "not_logged_in" | "unknown"
    """
    try:
        not_connected = page.ele("t:div@@class=text-sm text-neutral-500@@tx():Not Connected", timeout=timeout)
        if not_connected:
            return "not_logged_in"

        # 仅判断是否存在“地址样式”的元素，不写死具体地址
        addr_ele = page.ele(
            "t:span@@class=text-xs text-[#C6AA84] font-medium leading-tight group-hover:text-white "
            "transition-colors cursor-pointer hover:underline",
            timeout=timeout,
        )
        if addr_ele:
            return "logged_in"

        return "unknown"
    except Exception as e:
        log(account_id, f"检测登录状态异常: {e}")
        return "unknown"


def _login_if_needed(page: ChromiumPage, target_url: str, home_url: str, main_tab_id: str, account_id: str) -> Union[bool, object]:
    """
    根据页面元素判断是否需要登录，若未登录则执行登录点击。
    返回 True 或 Tab对象 表示已点击登录相关的按钮（需要后续处理弹窗），False 表示已登录或未点击成功。
    """
    # 既然外部判断了，这里直接执行登录动作
    log(account_id, "执行登录点击流程...")
    main_tab = page.get_tab(main_tab_id) if main_tab_id else page
    
    # 优先尝试点击 Connect Wallet 大按钮
    connect_sel = "t:button@@class=w-full md:w-[460px] h-12 bg-white text-black rounded-full font-medium text-lg transition-all hover:bg-neutral-200 active:scale-95 shadow-[0_0_20px_rgba(255,255,255,0.1)] disabled:opacity-50@@tx():Connect Wallet"
    
    detected_connect = False
    # 先尝试检测一次
    if main_tab.ele(connect_sel, timeout=5):
        detected_connect = True
        # 循环点击直到消失
        import time
        for k in range(5):
            # 每次点击前先检测是否出现了 "Continue with a wallet" (表示上一次点击其实生效了但页面还在过渡)
            continue_ele = main_tab.ele("t:div@@class=Grow-sc-681ff332-0 jelEMa@@tx():Continue with a wallet", timeout=0.5)
            if continue_ele:
                log(account_id, "检测到 Continue with a wallet，不再点击 Connect Wallet。")
                break

            if _try_detect_and_click(main_tab, connect_sel, account_id=account_id, timeout=1):
                time.sleep(1.5)
                # 再次检测 Continue with a wallet
                continue_ele = main_tab.ele("t:div@@class=Grow-sc-681ff332-0 jelEMa@@tx():Continue with a wallet", timeout=0.5)
                if continue_ele:
                    log(account_id, "检测到 Continue with a wallet，停止重试。")
                    break
                
                if not main_tab.ele(connect_sel, timeout=1):
                    log(account_id, "Connect Wallet 按钮已消失。")
                    break
                log(account_id, f"Connect Wallet 按钮仍存在，重试第 {k+2} 次点击...")
            else:
                break
    
    if not detected_connect:
         # 兜底：尝试点击旧的 Login 按钮
         _try_detect_and_click(
            main_tab,
            "t:button@@class=w-full flex items-center gap-3 px-4 py-3 text-sm "
            "font-medium text-neutral-400 hover:text-white rounded-lg hover:bg-white/5 "
            "transition-colors@@tx():Login",
            account_id=account_id,
            timeout=5,
        )

    # 增加判断：如果点击 Connect Wallet 后直接变更为已登录状态，则跳过后续点击 OKX 步骤
    import time
    time.sleep(3)
    if _check_wallet_login_state(page, account_id, timeout=2) == "logged_in":
        log(account_id, "点击 Connect Wallet 后检测到已自动登录，跳过后续步骤。")
        return False  # 返回 False 告诉外层不需要处理弹窗了

    # 1. 尝试点击 "Continue with a wallet" (如果有)
    # 这一步非常重要，用户反馈如果没有点击这个直接点 OKX Wallet 可能导致弹窗无法捕捉
    if _try_detect_and_click(
        main_tab,
        "t:div@@class=Grow-sc-681ff332-0 jelEMa@@tx():Continue with a wallet",
        account_id=account_id,
        timeout=3
    ):
        log(account_id, "已点击 Continue with a wallet，准备点击 OKX Wallet。")
        time.sleep(1)

    # 2. 尝试点击 "OKX Wallet" 并使用增强的弹窗捕捉逻辑
    okx_ele = main_tab.ele("t:span@@class=WalletName-sc-5a8fc7d8-5 ccpuDh@@tx():OKX Wallet", timeout=8)
    if okx_ele:
        # 记录旧的 tab ids
        old_tabs = page.tab_ids
        
        # Method 1: 先尝试 click.for_new_tab
        try:
            new_tab = okx_ele.click.for_new_tab(timeout=15)
            if new_tab:
                log(account_id, "点击 OKX Wallet 并捕捉到新弹窗 (method 1)。")
                return new_tab
        except Exception:
            pass

        # Method 2: 如果 method 1 失败，尝试强制 JS 点击并手动等待新 tab
        try:
            log(account_id, "尝试 JS 点击 OKX Wallet 并等待新 Tab...")
            okx_ele.click(by_js=True)
            
            # 轮询 tab_ids 直到出现新的
            start_wait = time.time()
            while time.time() - start_wait < 15:
                current_tabs = page.tab_ids
                for tid in current_tabs:
                    if tid not in old_tabs:
                        log(account_id, f"捕捉到新标签页 ID: {tid} (method 2)")
                        return page.get_tab(tid)
                time.sleep(0.5)
        except Exception as e:
            log(account_id, f"JS 点击等待异常: {e}")
            
        # 如果都捕捉不到，返回 True 让后续流程尝试被动捕捉
        return True
    
    # 如果没找到 OKX Wallet 元素，尝试兜底逻辑 (原来的 _post_login_actions 可能会处理，或者这里返回 False)
    return False


def _post_login_actions(page: ChromiumPage, home_url: str, main_tab_id: str, account_id: str) -> bool:
    """
    登录完成后的后续步骤（点击 OKX Wallet）。
    返回是否已成功处理弹窗。
    """
    _switch_to_main_and_open(page, main_tab_id, home_url, account_id)
    import time
    time.sleep(2)
    okx_ele = page.ele(
        "t:span@@class=WalletName-sc-5a8fc7d8-5 ccpuDh@@tx():OKX Wallet",
        timeout=8,
    )
    if okx_ele:
        try:
            # 增加超时时间到 15 秒
            new_tab = okx_ele.click.for_new_tab(timeout=15)
            if new_tab:
                try:
                    page.activate_tab(new_tab.tab_id)
                except Exception:
                    pass
                return _handle_okx_popup_actions(page, account_id, timeout=30, popup_tab=new_tab)
        except Exception:
            pass

    _try_detect_and_click(
        page,
        "t:span@@class=WalletName-sc-5a8fc7d8-5 ccpuDh@@tx():OKX Wallet",
        account_id=account_id,
        timeout=8,
    )
    return False


def _handle_okx_popup_actions(page: ChromiumPage, account_id: str, timeout: int = 12, popup_tab: Optional[object] = None) -> bool:
    """
    捕捉 OKX 弹窗并点击“连接/确认”按钮。
    """
    try:
        if not popup_tab:
            new_tab_id = page.wait.new_tab(timeout=timeout)
            if not new_tab_id:
                log(account_id, "未检测到新弹窗，尝试在当前页点击确认/连接。")
                popup_tab = page
            else:
                page.activate_tab(new_tab_id)
                popup_tab = page.get_tab(new_tab_id)

        if not popup_tab:
            log(account_id, "获取OKX确认弹窗失败。")
            return False
        # 多种选择器重试点击“连接/确认”
        selectors = [
            "xpath://button[.//div[normalize-space()='连接']]",
            "xpath://div[normalize-space()='连接']/ancestor::button",
            "xpath://button[.//*[normalize-space()='连接']]",
            "xpath://button[.//div[normalize-space()='Connect']]",
            "xpath://div[normalize-space()='Connect']/ancestor::button",
            "xpath://button[contains(normalize-space(),'Connect')]",
            "t:span@@class=btn-content _action-button__content_j3bvq_12@@tx():确认",
            "t:div@@class=_typography-text_1os1p_1 _typography-text-ellipsis_1os1p_61 "
            "_typography-text-adaptive_1os1p_37@@tx():确认",
            "xpath://button[.//div[normalize-space()='确认']]",
            "xpath://div[normalize-space()='确认']/ancestor::button",
            "xpath://button[.//*[normalize-space()='确认']]",
        ]
        js_click_script = """
        const texts = ['Connect', '连接', '确认'];
        const btns = Array.from(document.querySelectorAll('button'));
        for (const b of btns) {
            const t = (b.innerText || '').trim();
            if (texts.some(x => t.includes(x))) {
                b.click();
                return true;
            }
        }
        return false;
        """

        def _try_confirm_in_tab(tab) -> bool:
            import time
            time.sleep(2)  # 等待弹窗加载

            # 策略1：直接用 JS 精准点击（文档推荐方式）
            js_script = """
            try {
                // 1. 找所有 div
                let divs = document.querySelectorAll('div');
                for (let d of divs) {
                    // 匹配 class 和 文本
                    if (d.innerText.trim() === '确认' && d.className.includes('_typography-text')) {
                        // 找到 div 后，尝试点击它
                        d.click();
                        
                        // 同时尝试点击它的父级 button (如果有)
                        let btn = d.closest('button');
                        if (btn) {
                            btn.click();
                            return true;
                        }
                        // 如果没有父级 button，说明可能 div 本身就是可点击的，或事件委托在更上层
                        // 尝试模拟鼠标事件
                        let event = new MouseEvent('click', {
                            bubbles: true,
                            cancelable: true,
                            view: window
                        });
                        d.dispatchEvent(event);
                        return true;
                    }
                }
                
                // 2. 如果上面没找到，尝试找所有 button 里的“确认”
                let btns = document.querySelectorAll('button');
                for (let b of btns) {
                    if (b.innerText.includes('确认')) {
                        b.click();
                        return true;
                    }
                }
            } catch(e) {
                return false;
            }
            return false;
            """
            
            # 执行 JS 点击
            try:
                res = tab.run_js(js_script)
                if res:
                    log(account_id, "JS 点击确认脚本执行成功。")
                    return True
                else:
                    log(account_id, "JS 点击脚本执行返回 False")
            except Exception as e:
                log(account_id, f"JS 执行异常: {e}")

            # 策略2：DrissionPage 元素定位点击 (备用)
            confirm_selectors = [
                "t:div@@class=_typography-text_1os1p_1 _typography-text-ellipsis_1os1p_61 _typography-text-adaptive_1os1p_37@@tx():确认",
                "xpath://button[contains(., '确认')]",
            ]
            for sel in confirm_selectors:
                try:
                    ele = tab.ele(sel, timeout=2)
                    if ele:
                        # 尝试点击父级 button
                        parent = ele.parent()
                        if parent.tag == 'button':
                             parent.click()
                        else:
                             ele.click()
                        return True
                except Exception:
                    continue
            
            return False

        def _click_connect_for_new_tab(tab) -> Optional[object]:
            connect_selectors = [
                "xpath://button[.//div[normalize-space()='连接']]",
                "xpath://button[.//*[normalize-space()='连接']]",
                "xpath://button[.//div[normalize-space()='Connect']]",
                "xpath://button[contains(normalize-space(),'Connect')]",
            ]
            for sel in connect_selectors:
                try:
                    ele = tab.ele(sel, timeout=3)
                    if ele:
                        log(account_id, f"尝试点击连接按钮并捕捉新弹窗: {sel}")
                        # 增加超时时间，等待新弹窗加载
                        new_tab = ele.click.for_new_tab(timeout=10)
                        if new_tab:
                            log(account_id, f"成功捕捉到新弹窗: {new_tab.tab_id}")
                            return new_tab
                        else:
                            log(account_id, "点击连接后未捕捉到新弹窗对象。")
                except Exception as e:
                    log(account_id, f"点击连接按钮异常: {e}")
                    continue
            return None

        # 优先用 click.for_new_tab 捕捉“连接”后的确认弹窗
        connect_tab = _click_connect_for_new_tab(popup_tab)
        if connect_tab:
            try:
                page.activate_tab(connect_tab.tab_id)
            except Exception:
                pass
            if _try_confirm_in_tab(connect_tab):
                log(account_id, "连接弹窗确认成功，尝试检测后续弹窗...")
                
                # 循环检测并处理后续弹窗（可能有二次甚至三次确认）
                for i in range(2): 
                    # 增加超时时间到 8 秒，给后续弹窗足够的加载时间
                    next_tab_id = page.wait.new_tab(timeout=8)
                    if next_tab_id:
                         log(account_id, f"检测到第 {i+2} 次确认弹窗，尝试点击确认...")
                         try:
                             page.activate_tab(next_tab_id)
                             next_tab_obj = page.get_tab(next_tab_id)
                             if _try_confirm_in_tab(next_tab_obj):
                                 log(account_id, f"第 {i+2} 次确认成功。")
                             else:
                                 log(account_id, f"第 {i+2} 次确认点击失败或无需确认。")
                         except Exception as e:
                             log(account_id, f"处理后续弹窗异常: {e}")
                    else:
                         log(account_id, "未检测到更多后续弹窗，继续。")
                         break
                return True
            else:
                 log(account_id, "连接弹窗确认点击未成功或未找到元素。")

        for i in range(3):
            for sel in selectors:
                if _try_detect_and_click(popup_tab, sel, account_id, timeout=6):
                    # 如果是连接按钮，点击后等待新弹窗再点确认
                    if "Connect" in sel or "连接" in sel:
                        new_tab_id = page.wait.new_tab(timeout=timeout)
                        if new_tab_id:
                            page.activate_tab(new_tab_id)
                            new_tab = page.get_tab(new_tab_id)
                            if new_tab and _try_confirm_in_tab(new_tab):
                                return True
                    # 若按钮仍存在，尝试强制点击其父级按钮
                    try:
                        popup_tab.wait(1)
                        still_there = popup_tab.ele(sel, timeout=1)
                        if still_there:
                            btn = popup_tab.ele("xpath://div[normalize-space()='确认']/ancestor::button", timeout=1)
                            if btn:
                                btn.run_js("this.click();")
                    except Exception:
                        pass
                    return True
            try:
                popup_tab.wait(1)
            except Exception:
                pass
        log(account_id, "已尝试多种方式，仍未成功点击确认。")
        return False
    except Exception as e:
        log(account_id, f"处理OKX确认弹窗异常: {e}")
        return False


def _handle_okx_confirm_only(page: ChromiumPage, account_id: str, timeout: int = 12) -> bool:
    """
    使用新标签页捕捉方式，仅点击一次“确认”。
    """
    try:
        new_tab_id = page.wait.new_tab(timeout=timeout)
        if not new_tab_id:
            log(account_id, "未检测到确认弹窗。")
            return False
        page.activate_tab(new_tab_id)
        tab = page.get_tab(new_tab_id)
        if not tab:
            log(account_id, "获取确认弹窗失败。")
            return False
        return _try_detect_and_click(
            tab,
            "xpath://button[.//div[normalize-space()='确认']]",
            account_id,
            timeout=8,
        ) or _try_detect_and_click(
            tab,
            "t:span@@class=btn-content _action-button__content_j3bvq_12@@tx():确认",
            account_id,
            timeout=8,
        )
    except Exception as e:
        log(account_id, f"处理确认弹窗异常: {e}")
        return False

def _switch_to_main_and_open(page: ChromiumPage, main_tab_id: str, target_url: str, account_id: str):
    """
    切回主标签页并打开目标网址，确保前台显示任务页。
    """
    try:
        # DrissionPage 4.1.1.2 使用 activate_tab() 激活指定标签页
        page.activate_tab(main_tab_id)
        main_tab = page.get_tab(main_tab_id)
        if main_tab:
            main_tab.get(target_url)
            log(account_id, f"已切回任务页面: {target_url}")
            return
    except Exception as e:
        log(account_id, f"切回主标签页失败，尝试直接在当前页打开: {e}")

    # 兜底：在当前页打开
    page.get(target_url)
    log(account_id, f"已切回任务页面: {target_url}")


def _get_remaining_clicks(page: ChromiumPage) -> Optional[int]:
    """
    从按钮文本中提取剩余次数，如 'Place Long (94/100)' -> 94。
    """
    try:
        ele = page.ele("xpath://div[contains(normalize-space(),'Place Long')]", timeout=1)
        if not ele:
            ele = page.ele("xpath://div[contains(normalize-space(),'Place Short')]", timeout=1)
        if not ele:
            return None
        text = ele.text
        # 解析括号里的数字
        import re
        m = re.search(r"\((\d+)\s*/\s*\d+\)", text)
        if not m:
            return None
        return int(m.group(1))
    except Exception:
        return None


def _is_countdown_state(page: ChromiumPage) -> bool:
    """
    判断是否进入倒计时状态，如 '100 chances in 06:30:15'。
    """
    try:
        ele = page.ele("xpath://div[contains(normalize-space(),'chances in')]", timeout=1)
        return bool(ele)
    except Exception:
        return False


def _attempt_confirm_in_tab(tab, account_id: str) -> bool:
    """
    尝试在指定标签页点击确认/连接/签名按钮。
    """
    # 策略1：直接用 JS 精准点击
    js_script = """
    try {
        // 1. 找所有 div
        let divs = document.querySelectorAll('div');
        for (let d of divs) {
            // 匹配 class 和 文本
            let t = (d.innerText || '').trim();
            if ((t === '确认' || t === '签名' || t === 'Confirm' || t === 'Sign') && d.className.includes('_typography-text')) {
                d.click();
                let btn = d.closest('button');
                if (btn) { btn.click(); return true; }
                let event = new MouseEvent('click', {bubbles: true, cancelable: true, view: window});
                d.dispatchEvent(event);
                return true;
            }
        }
        
        // 2. 尝试找所有 button 里的“确认”或“签名”
        let btns = document.querySelectorAll('button');
        for (let b of btns) {
            let t = (b.innerText || '').trim();
            if (t === '确认' || t === '签名' || t === 'Confirm' || t === 'Sign' || t === 'Approve') {
                b.click();
                return true;
            }
        }
    } catch(e) { return false; }
    return false;
    """
    
    try:
        res = tab.run_js(js_script)
        if res:
            log(account_id, "JS 点击确认/签名成功。")
            return True
    except Exception:
        pass

    # 策略2：DrissionPage 元素定位点击
    selectors = [
        "t:div@@class=_typography-text_1os1p_1 _typography-text-ellipsis_1os1p_61 _typography-text-adaptive_1os1p_37@@tx():确认",
        "xpath://button[contains(., '确认')]",
        "xpath://button[contains(., 'Confirm')]",
        "xpath://button[contains(., 'Sign')]",
        "xpath://button[contains(., '签名')]",
        "t:span@@tx():确认", 
        "t:button@@tx():确认",
        "t:button@@tx():Confirm",
        "t:button@@tx():Sign"
    ]
    for sel in selectors:
        try:
            ele = tab.ele(sel, timeout=0.5)
            if ele:
                # 尝试点击父级 button
                try:
                    parent = ele.parent()
                    if parent.tag == 'button':
                         parent.click()
                    else:
                         ele.click()
                except:
                    ele.click(by_js=True)
                log(account_id, f"通过元素定位点击确认成功: {sel}")
                return True
        except Exception:
            continue
    
    return False

def _check_and_handle_popups(page: ChromiumPage, main_tab_id: str, account_id: str):
    """
    遍历所有标签页（除主页外），寻找并处理钱包弹窗。
    """
    try:
        # 获取最新标签页列表
        tab_ids = page.tab_ids
        for tid in tab_ids:
            if tid == main_tab_id:
                continue
            
            try:
                tab = page.get_tab(tid)
                title = tab.title
                url = tab.url
                # 宽松匹配，只要是 OKX 相关的或者看起来像弹窗的
                if "okx" in url or "extension" in url or "Wallet" in title or "Notification" in title or "签名" in title or "Sign" in title or "Request" in title:
                     if _attempt_confirm_in_tab(tab, account_id):
                         log(account_id, f"已在弹窗 {tid} 点击确认。")
            except Exception:
                pass
    except Exception:
        pass


def _wait_for_place_open_and_click(page: ChromiumPage, target_url: str, main_tab_id: str, account_id: str, max_clicks: Optional[int] = None, max_total_seconds: int = 900) -> bool:
    """
    监控状态元素：
    - 先等待出现 Place Success! 作为成功点击提示
    - 成功后检查 Placing Open，再随机点击 Long/Short
    - 若提供 max_clicks 则按次数停止，否则从按钮文本里读取剩余次数
    """
    _switch_to_main_and_open(page, main_tab_id, target_url, account_id)
    import time
    log(account_id, "已切回页面，等待 2 秒加载...")
    time.sleep(2)
    clicks = 0
    last_progress = time.time()
    last_remaining = None
    remaining = max_clicks
    # 首次进入先等待 Placing Open 可点击再触发一次点击
    log(account_id, "首次进入页面，等待 Placing Open 可点击后触发一次点击...")
    while True:
        # 检查弹窗
        _check_and_handle_popups(page, main_tab_id, account_id)
        
        if STOP_FLAG:
            log(account_id, "收到停止信号，停止监控。")
            return False
        if time.time() - last_progress > max_total_seconds:
            log(account_id, f"长时间无进展，触发超时({max_total_seconds}s)，将结束该窗口。")
            return False
        if _is_countdown_state(page):
            log(account_id, "检测到倒计时状态，结束监控。")
            return True
        placing_open = page.ele(
            "t:div@@class=flex items-center gap-2 text-xs capitalize text-emerald-400@@tx():Placing Open",
            timeout=1,
        )
        if placing_open:
            first_choice = random.choice(["long", "short"])
            if first_choice == "long":
                clicked = _try_detect_and_click(
                    page,
                    "t:div@@class=w-full py-3 rounded-lg font-medium text-center transition-all "
                    "flex items-center justify-center gap-2@@tx():Place Long",
                    account_id=account_id,
                    timeout=6,
                ) or _try_detect_and_click(
                    page,
                    "xpath://div[contains(normalize-space(),'Place Long')]",
                    account_id=account_id,
                    timeout=6,
                )
            else:
                clicked = _try_detect_and_click(
                    page,
                    "t:div@@class=w-full py-3 rounded-lg font-medium text-center transition-all "
                    "flex items-center justify-center gap-2@@tx():Place Short",
                    account_id=account_id,
                    timeout=6,
                ) or _try_detect_and_click(
                    page,
                    "xpath://div[contains(normalize-space(),'Place Short')]",
                    account_id=account_id,
                    timeout=6,
                )
            if clicked:
                last_progress = time.time()
            break
        time.sleep(0.1)

    # 首次点击后，检测是否生效；若卡住则重试点击
    initial_click_attempts = 1
    initial_click_last_try = time.time()
    initial_click_timeout = 10
    initial_click_max_attempts = 3
    while True:
        # 检查弹窗
        _check_and_handle_popups(page, main_tab_id, account_id)

        if STOP_FLAG:
            return False
        if time.time() - last_progress > max_total_seconds:
            log(account_id, f"长时间无进展，触发超时({max_total_seconds}s)，将结束该窗口。")
            return False
        if _is_countdown_state(page):
            log(account_id, "检测到倒计时状态，结束监控。")
            return True
        success = page.ele(
            "t:div@@class=text-white font-semibold text-base@@tx():Place Success!",
            timeout=0.2,
        )
        settling = page.ele("xpath://div[contains(normalize-space(),'Settling')]", timeout=0.2)
        if success:
            log(account_id, "检测到 Place Success!，开始等待 Settling...")
            stage = "wait_settling"
            stage_start = time.time()
            last_progress = time.time()
            break
        if settling:
            log(account_id, "未检测到 Place Success，但检测到 Settling，继续等待 Settling 结束...")
            stage = "wait_settling"
            stage_start = time.time()
            last_progress = time.time()
            break

        placing_open = page.ele(
            "t:div@@class=flex items-center gap-2 text-xs capitalize text-emerald-400@@tx():Placing Open",
            timeout=0.2,
        )
        if placing_open and time.time() - initial_click_last_try > initial_click_timeout:
            if initial_click_attempts < initial_click_max_attempts:
                initial_click_attempts += 1
                initial_click_last_try = time.time()
                log(account_id, f"首次点击未生效，重试第 {initial_click_attempts} 次点击。")
                retry_choice = random.choice(["long", "short"])
                if retry_choice == "long":
                    _try_detect_and_click(
                        page,
                        "t:div@@class=w-full py-3 rounded-lg font-medium text-center transition-all "
                        "flex items-center justify-center gap-2@@tx():Place Long",
                        account_id=account_id,
                        timeout=6,
                    ) or _try_detect_and_click(
                        page,
                        "xpath://div[contains(normalize-space(),'Place Long')]",
                        account_id=account_id,
                        timeout=6,
                    )
                else:
                    _try_detect_and_click(
                        page,
                        "t:div@@class=w-full py-3 rounded-lg font-medium text-center transition-all "
                        "flex items-center justify-center gap-2@@tx():Place Short",
                        account_id=account_id,
                        timeout=6,
                    ) or _try_detect_and_click(
                        page,
                        "xpath://div[contains(normalize-space(),'Place Short')]",
                        account_id=account_id,
                        timeout=6,
                    )
                last_progress = time.time()
            else:
                log(account_id, "首次点击多次未生效，继续进入监控流程。")
                stage = "wait_next_open"
                stage_start = time.time()
                break
        time.sleep(0.1)

    # 继续沿用上一步的阶段（已设为 wait_settling）
    none_count = 0
    while True:
        # 检查弹窗
        _check_and_handle_popups(page, main_tab_id, account_id)

        if STOP_FLAG:
            log(account_id, "收到停止信号，停止监控。")
            return False
        if time.time() - last_progress > max_total_seconds:
            log(account_id, f"长时间无进展，触发超时({max_total_seconds}s)，将结束该窗口。")
            return False
        try:
            # 如果进入倒计时，表示次数已用完
            if _is_countdown_state(page):
                log(account_id, "检测到倒计时状态，结束监控。")
                return True

            # 每次循环都从页面文本读取剩余次数，避免点击失败导致计数错误
            remaining = _get_remaining_clicks(page)
            if remaining is None:
                none_count += 1
                if none_count % 20 == 0:
                    log(account_id, "无法解析剩余次数，重试中...")
                if none_count >= 60:
                    log(account_id, "连续无法解析剩余次数，视为已结束。")
                    return False
                time.sleep(0.5)
                continue
            none_count = 0
            if last_remaining is not None and remaining < last_remaining:
                last_progress = time.time()
            last_remaining = remaining
            if remaining <= 0:
                log(account_id, "剩余次数为 0，结束监控。")
                return True
            placing_open = page.ele(
                "t:div@@class=flex items-center gap-2 text-xs capitalize text-emerald-400@@tx():Placing Open",
                timeout=0.1,
            )
            settling = page.ele("xpath://div[contains(normalize-space(),'Settling')]", timeout=0.1)
            success = page.ele(
                "t:div@@class=text-white font-semibold text-base@@tx():Place Success!",
                timeout=0.1,
            )

            if stage in ("wait_first_open", "wait_next_open"):
                if placing_open:
                    log(account_id, "检测到 Placing Open，随机点击 Long/Short。")
                    choice = random.choice(["long", "short"])
                    if choice == "long":
                        if _try_detect_and_click(
                            page,
                            "t:div@@class=w-full py-3 rounded-lg font-medium text-center transition-all "
                            "flex items-center justify-center gap-2@@tx():Place Long",
                            account_id=account_id,
                            timeout=6,
                        ) or _try_detect_and_click(
                            page,
                            "xpath://div[contains(normalize-space(),'Place Long')]",
                            account_id=account_id,
                            timeout=6,
                        ):
                            clicks += 1
                            last_progress = time.time()
                    else:
                        if _try_detect_and_click(
                            page,
                            "t:div@@class=w-full py-3 rounded-lg font-medium text-center transition-all "
                            "flex items-center justify-center gap-2@@tx():Place Short",
                            account_id=account_id,
                            timeout=6,
                        ) or _try_detect_and_click(
                            page,
                            "xpath://div[contains(normalize-space(),'Place Short')]",
                            account_id=account_id,
                            timeout=6,
                        ):
                            clicks += 1
                            last_progress = time.time()
                    # 点击后稍等再读取一次剩余次数，避免显示滞后
                    time.sleep(0.5)
                    remaining_after = _get_remaining_clicks(page)
                    if remaining_after is None:
                        remaining_after = remaining
                    log(account_id, f"已点击次数: {clicks}，剩余可点击: {remaining_after}")
                    stage = "wait_success"
                    stage_start = time.time()
                else:
                    time.sleep(0.1)
                continue

            if stage == "wait_success":
                if success:
                    log(account_id, "检测到 Place Success!，等待 Settling 出现...")
                    stage = "wait_settling"
                    stage_start = time.time()
                    last_progress = time.time()
                elif settling:
                    log(account_id, "未检测到 Place Success，但检测到 Settling，继续等待 Settling 结束...")
                    stage = "wait_settling"
                    stage_start = time.time()
                    last_progress = time.time()
                elif time.time() - stage_start > 60:
                    log(account_id, "等待 Place Success 超时，继续等待下一个 Placing Open...")
                    stage = "wait_next_open"
                    stage_start = time.time()
                time.sleep(0.05)
                continue

            if stage == "wait_settling":
                if settling:
                    # log(account_id, "检测到 Settling，等待 Settling 结束...")
                    stage = "wait_settling_clear"
                    stage_start = time.time()
                elif time.time() - stage_start > 60:
                    log(account_id, "等待 Settling 超时，继续等待下一个 Placing Open...")
                    stage = "wait_next_open"
                    stage_start = time.time()
                time.sleep(0.05)
                continue

            if stage == "wait_settling_clear":
                if not settling:
                    log(account_id, "Settling 已结束，等待下一个 Placing Open...")
                    stage = "wait_next_open"
                    stage_start = time.time()
                elif time.time() - stage_start > 60:
                    log(account_id, "Settling 结束等待超时，继续等待下一个 Placing Open...")
                    stage = "wait_next_open"
                    stage_start = time.time()
                time.sleep(0.05)
                continue

            time.sleep(0.2)
        except Exception as e:
            log(account_id, f"检测状态异常: {e}")
            time.sleep(0.3)
    log(account_id, f"已完成点击 {clicks} 次，结束监控。")
    return True


def _claim_all_rewards(page: ChromiumPage, tasks_url: str, main_tab_id: str, account_id: str) -> bool:
    """
    进入任务页面，点击所有包含“Claim Reward”的按钮。
    """
    _switch_to_main_and_open(page, main_tab_id, tasks_url, account_id)
    import time
    time.sleep(2)
    while True:
        # 检查弹窗
        _check_and_handle_popups(page, main_tab_id, account_id)

        if STOP_FLAG:
            return False
        buttons = page.eles("xpath://button[contains(normalize-space(),'Claim Reward')]")
        if not buttons:
            log(account_id, "未找到 Claim Reward 按钮，结束。")
            return True
        log(account_id, f"检测到 {len(buttons)} 个 Claim Reward 按钮，开始逐个点击。")
        for btn in buttons:
            # 检查弹窗
            _check_and_handle_popups(page, main_tab_id, account_id)
            
            try:
                btn.scroll.to_see()
            except Exception:
                pass
            try:
                btn.click()
            except Exception:
                try:
                    btn.click(by_js=True)
                except Exception as e:
                    log(account_id, f"点击 Claim Reward 失败: {e}")
            time.sleep(0.3)
        time.sleep(1)

def run_account_task(
    account: AccountInfo,
    url: str = "https://hub.aixcrypto.ai/#prediction-market",
    click_selector: Optional[str] = None,
    api_key: str = "",
    reuse_existing: bool = True,
):
    """
    单个账号的任务流程。
    """
    account_id = account.id
    
    if is_account_completed(account_id):
        log(account_id, "当前周期任务已完成，跳过。")
        return

    base_dir = os.path.dirname(os.path.abspath(__file__))
    if getattr(sys, 'frozen', False):
        base_dir = os.path.dirname(sys.executable)
        
    manager = AdsBrowserManager(excel_path=os.path.join(base_dir, "shuju.xlsx"), api_key=api_key)

    # 优先复用已打开的窗口（避免频繁关闭/启动）
    debug_port_file = os.path.join(base_dir, f"last_debug_port_{account_id}.txt")
    page = None
    addr = None
    
    # 强制在多线程模式下不复用 "last_debug_port.txt" 这种全局文件，而是只认 account_id 专属的
    # 原代码可能有逻辑漏洞导致不同线程读到同一个端口
    
    if reuse_existing and os.path.exists(debug_port_file):
        try:
            with open(debug_port_file, "r", encoding="utf-8") as f:
                saved_addr = f.read().strip()
            if saved_addr:
                if _is_port_open(saved_addr):
                    page = ChromiumPage(addr_or_opts=saved_addr, timeout=10)
                    _ = page.tab_ids  # 简单触发一次连接校验
                    addr = saved_addr
                    log(account_id, f"已复用现有窗口连接: {addr}")
                else:
                    # log(account_id, f"端口不可用: {saved_addr}")
                    try:
                        os.remove(debug_port_file)
                    except Exception:
                        pass
        except Exception:
            try:
                os.remove(debug_port_file)
            except Exception:
                pass

    if not page:
        page, addr = manager.start_browser_with_addr(account)
        if page and addr:
            try:
                with open(debug_port_file, "w", encoding="utf-8") as f:
                    f.write(addr)
            except Exception as e:
                log(account_id, f"保存调试端口失败: {e}")
    
    if not page:
        log(account_id, f"启动窗口失败，无法获取 ChromiumPage 对象")
        return

    log(account_id, f"已连接窗口 ({addr})")
    try:
        page.get(url)
        log(account_id, f"已打开网址: {url}")

        # 记录任务页主标签，供后续切回
        main_tab_id = page.tab_id

        # 进入网站后解锁OKX钱包
        wallet = OKXWallet(page)
        if wallet.unlock():
            log(account_id, "OKX 钱包解锁成功。")
        else:
            log(account_id, "OKX 钱包解锁失败，请检查插件是否已安装/弹窗是否出现。")

        # 登录状态检测与弹窗确认
        home_url = "https://hub.aixcrypto.ai/#home"
        
        # 重新判断一下登录状态
        _switch_to_main_and_open(page, main_tab_id, home_url, account_id)
        import time
        time.sleep(2)
        current_state = _check_wallet_login_state(page, account_id)
        
        if current_state == "logged_in":
             log(account_id, "当前已是登录状态，跳过后续钱包连接/弹窗处理。")
        else:
            # 未登录，执行登录流程
            okx_result = _login_if_needed(page, url, home_url, main_tab_id, account_id)
            
            # 判断返回值：可能是 Tab 对象，也可能是 True (点击了但没捕捉到)，也可能是 False
            if okx_result:
                time.sleep(2)
                popup_tab = okx_result if not isinstance(okx_result, bool) else None
                _handle_okx_popup_actions(page, account_id, timeout=30, popup_tab=popup_tab)
            else:
                 # 再次检查是否已登录 (可能点击 Connect Wallet 后自动登录了)
                 if _check_wallet_login_state(page, account_id, timeout=3) == "logged_in":
                     log(account_id, "检测到已自动登录，跳过补救措施。")
                 else:
                     # 尝试补救点击 OKX Wallet
                     if not _post_login_actions(page, home_url, main_tab_id, account_id):
                        time.sleep(2)
                        _handle_okx_popup_actions(page, account_id, timeout=25)

        # 无论是否已登录，在开始做任务前，都进行几轮残留弹窗清理
        # 针对用户反馈的 "只会在打开窗口后触发，只要把弹窗处理完后续是不会遇到这种情况"
        log(account_id, "任务开始前：深度检查并清理残留的钱包签名/确认弹窗...")
        for _ in range(3):
            _check_and_handle_popups(page, main_tab_id, account_id)
            time.sleep(1.5)

        if click_selector:
            _try_detect_and_click(page, click_selector, account_id)

        # 监控状态并点击 Long/Short
        place_done = _wait_for_place_open_and_click(page, url, main_tab_id, account_id)
        if STOP_FLAG:
            log(account_id, "收到停止信号，未记录完成状态。")
            return
        claim_done = _claim_all_rewards(page, "https://hub.aixcrypto.ai/#tasks", main_tab_id, account_id)
        if STOP_FLAG:
            log(account_id, "收到停止信号，未记录完成状态。")
            return

        # 任务全部完成，记录状态
        if place_done and claim_done:
            save_completed_task(account_id)
            log(account_id, "任务全部完成，已记录状态。")
        else:
            log(account_id, "任务未完整完成，未记录状态。")
        
    except Exception as e:
        log(account_id, f"任务执行异常: {e}")
    finally:
        # 关闭浏览器
        try:
            manager.close_browser(account_id)
        except Exception:
            pass
        try:
            if os.path.exists(debug_port_file):
                os.remove(debug_port_file)
        except Exception:
            pass


if __name__ == "__main__":
    # 初始化管理器读取账号
    if getattr(sys, 'frozen', False):
        base_dir = os.path.dirname(sys.executable)
    else:
        base_dir = os.path.dirname(os.path.abspath(__file__))
    excel_path = os.path.join(base_dir, "shuju.xlsx")
    manager = AdsBrowserManager(excel_path=excel_path, api_key=ADSPOWER_API_KEY)
    all_accounts = manager.get_account_list()
    
    if not all_accounts:
        print("未读取到任何账号，请检查 shuju.xlsx")
        sys.exit(1)

    print(f"共读取到 {len(all_accounts)} 个账号。")
    print("请选择运行模式:")
    print("1. 单窗口测试 (默认第2个账号)")
    print("2. 批量运行 (多线程)")
    
    mode = input("请输入数字 (1/2): ").strip()
    
    if mode == "1":
        # 单窗口测试
        target_account = all_accounts[1] if len(all_accounts) > 1 else all_accounts[0]
        print(f"开始单窗口测试: {target_account.id}")
        run_account_task(target_account, api_key=ADSPOWER_API_KEY)
        
    elif mode == "2":
        # 批量运行
        try:
            thread_count = int(input("请输入并发线程数 (建议 1-5): ").strip())
        except ValueError:
            thread_count = 1
        
        print(f"即将开始批量运行，线程数: {thread_count}")
        
        # 跑所有账号
        target_accounts = all_accounts 
        
        # 使用信号量限制并发数
        import threading
        semaphore = threading.Semaphore(thread_count)

        def _run_batch(accounts: List[AccountInfo], round_name: str):
            log("SYSTEM", f"{round_name}：开始执行 {len(accounts)} 个账号。")
            with ThreadPoolExecutor(max_workers=thread_count) as executor:
                futures = []
                for account in accounts:
                    semaphore.acquire() # 获取信号量

                    # 再次检查是否已完成 (防止在排队期间状态已变更)
                    if is_account_completed(account.id):
                        log(account.id, "在排队期间检测到任务已完成，跳过。")
                        semaphore.release()
                        continue

                    def task_wrapper(acc, key):
                        try:
                            run_account_task(acc, api_key=key)
                        except Exception as e:
                            log(acc.id, f"任务wrapper异常: {e}")
                        finally:
                            semaphore.release() # 释放信号量

                    future = executor.submit(task_wrapper, account, ADSPOWER_API_KEY)
                    futures.append(future)

                    import time
                    time.sleep(3) # 稍微错开启动时间，避免并发请求AdsPower API导致拥堵

                # 等待所有任务完成
                for future in futures:
                    try:
                        future.result()
                    except Exception as e:
                        print(f"线程执行异常: {e}")

        # 第一轮
        _run_batch(target_accounts, "第一轮")

        # 第二轮：只补跑未完成
        remaining_accounts = [acc for acc in target_accounts if not is_account_completed(acc.id)]
        if remaining_accounts and not STOP_FLAG:
            _run_batch(remaining_accounts, "第二轮补跑")
                
    else:
        print("无效输入，退出。")
