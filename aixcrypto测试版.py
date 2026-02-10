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

# 内置 OKX 解锁模块（避免依赖单独的 okx_wallet.py）
class OKXWallet:
    """
    OKX钱包自动化操作模块
    """
    DEFAULT_PASSWORD = "DD112211"

    def __init__(self, page: ChromiumPage, log=None):
        """
        初始化，传入已打开OKX弹窗的ChromiumPage对象。
        :param log: 可选，日志回调 log(msg)，若传入则详细日志会通过此回调输出
        """
        self.page = page
        self.log = log

    def unlock(self, password: str = None) -> bool:
        """
        输入密码并点击解锁按钮，完成钱包解锁。
        :param password: 钱包密码，默认用类属性DEFAULT_PASSWORD
        :return: 解锁是否成功
        """
        if password is None:
            password = self.DEFAULT_PASSWORD

        import time
        import traceback

        def _log(msg):
            text = "[OKX解锁] %s" % msg
            if self.log:
                self.log(text)
            else:
                print(text)

        # 1. 记录操作前的标签页
        before_tabs = set(self.page.tab_ids)
        _log("步骤1: 打开插件前 tab_ids=%s" % (list(before_tabs)[:5] if len(before_tabs) > 5 else list(before_tabs)))

        okx_url = "chrome-extension://mcohilncbfahbmgdjkbpemcciiolgcge/popup.html"
        self.page.get(okx_url)
        time.sleep(2)

        after_tabs = set(self.page.tab_ids)
        new_tab_ids = after_tabs - before_tabs
        _log("步骤2: 打开插件后 tab_ids=%s, 新tab=%s" % (list(after_tabs)[:5] if len(after_tabs) > 5 else list(after_tabs), list(new_tab_ids)))

        unlock_tab = None
        if new_tab_ids:
            unlock_tab_id = new_tab_ids.pop()
            unlock_tab = self.page.get_tab(unlock_tab_id)
            _log("使用新弹窗 tab_id=%s" % unlock_tab_id)
        else:
            unlock_tab = self.page.latest_tab
            _log("无新弹窗，使用 latest_tab")

        if not hasattr(unlock_tab, 'ele'):
            _log("【失败】unlock_tab 无 ele 方法")
            return False

        try:
            tab_url = getattr(unlock_tab, 'url', None) or ''
            _log("步骤3: 当前操作页 URL=%s" % (tab_url[:80] if tab_url else "(无)"))

            # 有些电脑会打开到 offscreen.html（无界面），需要强制打开 popup.html
            if tab_url.endswith("/offscreen.html"):
                _log("检测到 offscreen 页面，尝试强制打开 popup.html")
                try:
                    before_tabs2 = set(self.page.tab_ids)
                    self.page.run_js("window.open(arguments[0], '_blank');", okx_url)
                    time.sleep(2)
                    after_tabs2 = set(self.page.tab_ids)
                    new_tabs2 = list(after_tabs2 - before_tabs2)
                    if new_tabs2:
                        unlock_tab = self.page.get_tab(new_tabs2[0])
                        tab_url = getattr(unlock_tab, 'url', None) or ''
                        _log("已切到 popup tab_id=%s URL=%s" % (unlock_tab.tab_id, tab_url[:80]))
                except Exception as e:
                    _log("offscreen 切换 popup 失败: %s" % e)
                # 兜底：直接新开标签页访问 popup
                if tab_url.endswith("/offscreen.html"):
                    try:
                        new_tab = self.page.new_tab(okx_url + "#/unlock")
                        if new_tab:
                            unlock_tab = new_tab
                            tab_url = getattr(unlock_tab, 'url', None) or ''
                            _log("已新开 popup 标签 URL=%s" % (tab_url[:80] if tab_url else "(无)"))
                    except Exception as e:
                        _log("新开 popup 标签失败: %s" % e)

            # 查找密码框
            password_selectors = [
                ('xpath', 'xpath://input[@data-testid="okd-input" and @type="password"]'),
                ('css', 'css:input[data-testid=okd-input][type=password]'),
                ('tag', 'tag:input@@data-testid=okd-input@@type=password'),
                ('placeholder', 'xpath://input[@type="password" and @placeholder="请输入密码"]'),
                ('xpath_generic', 'xpath://input[@type="password"]'),
                ('css_generic', 'css:input[type=password]'),
            ]
            password_input = None
            used_selector = None
            for attempt in range(3):
                for name, sel in password_selectors:
                    password_input = unlock_tab.ele(sel, timeout=5)
                    if password_input:
                        used_selector = name
                        break
                if password_input:
                    break
                _log("第 %d 轮未找到密码框，2秒后重试..." % (attempt + 1))
                time.sleep(2)

            if not password_input:
                # 未找到密码框：可能是扩展未加载、加载慢，也可能是已经解锁
                if not tab_url.startswith("chrome-extension://"):
                    _log("【失败】未进入 OKX 扩展页面（当前非 chrome-extension://），请检查扩展是否安装/是否被策略禁用。")
                    return False

                # 尝试等待/刷新 popup 页，避免空白页导致误判
                retry_loaded = False
                for i in range(3):
                    try:
                        page_text = unlock_tab.ele("tag:body", timeout=3).text or ""
                    except Exception:
                        page_text = ""

                    if page_text.strip():
                        retry_loaded = True
                        break

                    _log("扩展页面文本为空，尝试刷新/重开 (%d/3)..." % (i + 1))
                    try:
                        unlock_tab.get(okx_url + "#/unlock")
                    except Exception:
                        try:
                            unlock_tab.run_js("location.reload()")
                        except Exception:
                            pass
                    time.sleep(2)
                    # 每轮再尝试新开标签
                    if not page_text.strip():
                        try:
                            new_tab = self.page.new_tab(okx_url + "#/unlock")
                            if new_tab:
                                unlock_tab = new_tab
                                tab_url = getattr(unlock_tab, 'url', None) or ''
                                _log("重开 popup 标签 URL=%s" % (tab_url[:80] if tab_url else "(无)"))
                        except Exception:
                            pass

                if not retry_loaded:


                    _log("扩展页面文本为空，尝试使用 JS 查找密码框（含 iframe/shadow）...")
                    try:
                        js_result = unlock_tab.run_js("""
                        return (function(pwd){
                          function findInputInDoc(doc){
                            if(!doc) return null;
                            try{
                              var el = doc.querySelector('input[type="password"]');
                              if(el) return el;
                              var all = doc.querySelectorAll('*');
                              for (var i=0;i<all.length;i++){
                                var n = all[i];
                                if(n && n.shadowRoot){
                                  var found = findInputInDoc(n.shadowRoot);
                                  if(found) return found;
                                }
                              }
                            }catch(e){}
                            return null;
                          }
                          function findBtnInDoc(doc){
                            if(!doc) return null;
                            try{
                              return doc.querySelector('button[data-testid="okd-button"][type="submit"]') ||
                                     doc.querySelector('button[type="submit"]');
                            }catch(e){}
                            return null;
                          }
                          var input = findInputInDoc(document);
                          var btn = findBtnInDoc(document);
                          try{
                            var iframes = document.querySelectorAll('iframe');
                            for (var i=0;i<iframes.length && !input;i++){
                              try{
                                var idoc = iframes[i].contentDocument;
                                if(idoc){
                                  input = findInputInDoc(idoc);
                                  if(input && !btn) btn = findBtnInDoc(idoc);
                                }
                              }catch(e){}
                            }
                          }catch(e){}
                          if(!input) return {found:false};
                          try{
                            input.focus();
                            input.value = pwd;
                            input.dispatchEvent(new Event('input', {bubbles:true}));
                            input.dispatchEvent(new Event('change', {bubbles:true}));
                          }catch(e){}
                          if(btn){
                            try{ btn.click(); }catch(e){}
                          }
                          return {found:true};
                        })(arguments[0]);
                        """, password)
                    except Exception as e:
                        js_result = {"found": False}
                        _log("JS 查找密码框异常: %s" % e)

                    if js_result and js_result.get("found"):
                        _log("JS 已尝试填写密码并点击解锁，等待结果...")
                        time.sleep(2)
                        try:
                            still_locked = unlock_tab.run_js("""
                            return (function(){
                              function findInputInDoc(doc){
                                if(!doc) return null;
                                try{
                                  var el = doc.querySelector('input[type="password"]');
                                  if(el) return el;
                                  var all = doc.querySelectorAll('*');
                                  for (var i=0;i<all.length;i++){
                                    var n = all[i];
                                    if(n && n.shadowRoot){
                                      var found = findInputInDoc(n.shadowRoot);
                                      if(found) return found;
                                    }
                                  }
                                }catch(e){}
                                return null;
                              }
                              if(findInputInDoc(document)) return true;
                              try{
                                var iframes = document.querySelectorAll('iframe');
                                for (var i=0;i<iframes.length;i++){
                                  try{
                                    var idoc = iframes[i].contentDocument;
                                    if(findInputInDoc(idoc)) return true;
                                  }catch(e){}
                                }
                              }catch(e){}
                              return false;
                            })();
                            """)
                        except Exception:
                            still_locked = False
                        if not still_locked:
                            _log("JS 解锁成功（密码框已消失）")
                            return True
                        _log("JS 解锁后密码框仍存在，判定未解锁。")
                        return False

                    _log("【失败】扩展页面文本为空，且 JS 未找到密码框。将等待手动解锁...")
                    # 等待用户手动解锁（部分机器无法自动读取扩展 DOM）
                    for _ in range(15):
                        time.sleep(2)
                        try:
                            if unlock_tab.tab_id not in self.page.tab_ids:
                                _log("检测到解锁弹窗已关闭，视为手动解锁成功。")
                                return True
                        except Exception:
                            pass
                        try:
                            cur_url = getattr(unlock_tab, "url", "") or ""
                            if "#/unlock" not in cur_url and "popup.html" in cur_url:
                                _log("检测到弹窗已离开 unlock 页面，视为手动解锁成功。")
                                return True
                        except Exception:
                            pass
                    _log("【失败】等待手动解锁超时，判定未解锁。")
                    return False

                blocked_keywords = ("ERR_BLOCKED_BY_CLIENT", "This site can’t be reached", "无法访问此网站", "ERR_FAILED")
                if any(k in page_text for k in blocked_keywords):
                    _log("【失败】OKX 扩展页面加载失败（疑似被阻止或扩展不可用）。")
                    return False

                # 判断是否仍处于“锁定”界面
                lock_keywords = ("解锁", "Unlock", "请输入密码", "输入密码", "Password")
                if any(k in page_text for k in lock_keywords):
                    _log("【失败】页面含锁定提示但未找到密码框，判定为未解锁。")
                    return False

                _log("未找到密码框且无锁定提示，判定为“已解锁”。")
                return True

            _log("步骤4: 已找到密码框(选择器=%s)，开始输入密码" % used_selector)
            password_input.click()
            time.sleep(0.2)
            password_input.input(password)
            time.sleep(0.2)
            try:
                password_input.run_js("this.value = arguments[0]; this.dispatchEvent(new Event('input', { bubbles: true }));", password)
                _log("已用JS补写密码并触发input事件")
            except Exception as js_e:
                _log("JS补写密码异常(可忽略): %s" % js_e)
            time.sleep(0.3)

            unlock_btn_selectors = [
                ('xpath_okd', 'xpath://button[@data-testid="okd-button" and @type="submit"]'),
                ('css_okd', 'css:button[data-testid=okd-button][type=submit]'),
                ('tag_okd', 'tag:button@@data-testid=okd-button@@type=submit'),
                ('xpath_submit', 'xpath://button[@type="submit"]'),
            ]
            unlock_btn = None
            btn_sel_used = None
            for name, sel in unlock_btn_selectors:
                unlock_btn = unlock_tab.ele(sel, timeout=5)
                if unlock_btn:
                    btn_sel_used = name
                    break
            if not unlock_btn:
                _log("【失败】未找到解锁按钮。当前页URL=%s" % (tab_url[:80] if tab_url else "(无)"))
                return False

            _log("步骤5: 已找到解锁按钮(选择器=%s)，点击" % btn_sel_used)
            unlock_btn.click()
            time.sleep(2)

            password_input_after = unlock_tab.ele('tag:input@@data-testid=okd-input@@type=password', timeout=3)
            if password_input_after:
                _log("【失败】点击后密码框仍存在，可能密码错误或未真正解锁")
                return False
            _log("步骤6: 解锁成功(密码框已消失)")
            return True

        except Exception as e:
            _log("【失败】异常: %s" % e)
            _log(traceback.format_exc())
            return False

# 版本号（用于自动更新比较）
__version__ = "2026.02.10.11"

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
# 性能调试开关（默认开启；可通过环境变量 AIX_PERF_DEBUG=0 关闭）
PERF_DEBUG = str(os.environ.get("AIX_PERF_DEBUG", "1")).strip().lower() in ("1", "true", "yes", "on")

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

def set_perf_debug(enabled: bool):
    """
    运行时切换性能调试日志开关。
    """
    global PERF_DEBUG
    PERF_DEBUG = bool(enabled)

def perf_log(account_id: str, msg: str):
    """
    仅在开启 PERF_DEBUG 时输出性能调试日志。
    """
    if PERF_DEBUG:
        log(account_id, f"[PERF] {msg}")

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
        import re
        values = []
        # 同时读取 Long/Short，避免某个按钮文本滞后导致误判
        long_eles = page.eles("xpath://div[contains(normalize-space(),'Place Long')]")
        short_eles = page.eles("xpath://div[contains(normalize-space(),'Place Short')]")
        for ele in (long_eles + short_eles):
            try:
                text = ele.text or ""
            except Exception:
                text = ""
            m = re.search(r"\((\d+)\s*/\s*\d+\)", text)
            if m:
                values.append(int(m.group(1)))

        if not values:
            return None
        # 使用最小值更保守，避免读取到旧文本导致“已完成却不退出”
        return min(values)
    except Exception:
        return None


def _is_countdown_state(page: ChromiumPage) -> bool:
    """
    判断是否进入倒计时状态，如 '100 chances in 06:30:15'。
    """
    try:
        ele = page.ele("xpath://div[contains(normalize-space(),'chances in')]", timeout=0.2)
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


def _check_network_error(page: ChromiumPage, account_id: str) -> bool:
    """
    检查页面是否出现网络连接错误（如 ERR_SOCKS_CONNECTION_FAILED, 无法访问此网站 等）。
    如果发现错误，等待 10 秒并刷新，返回 True 表示触发了重试。
    """
    try:
        # 获取页面文本（可能需要切换到 body 或 html）
        # 注意：某些错误页面可能没有 body 标签，或者结构特殊
        # 这里尝试获取整个页面的文本内容
        text = page.html
        if not text:
            return False
            
        error_keywords = [
            "ERR_SOCKS_CONNECTION_FAILED",
            "ERR_CONNECTION_RESET",
            "ERR_NAME_NOT_RESOLVED",
            "ERR_CONNECTION_TIMED_OUT",
            "无法访问此网站",
            "This site can’t be reached",
            "There is no internet connection",
            "Connection failed",
            "ERR_NETWORK_CHANGED",
            "ERR_INTERNET_DISCONNECTED"
        ]
        
        for kw in error_keywords:
            if kw in text:
                log(account_id, f"检测到网络错误关键信息: '{kw}'，等待 10 秒后刷新重试...")
                import time
                time.sleep(10)
                try:
                    page.refresh()
                except Exception as e:
                    log(account_id, f"刷新页面异常: {e}")
                time.sleep(5) # 刷新后等待加载
                return True
        return False
    except Exception as e:
        # log(account_id, f"检查网络错误异常: {e}")
        return False

def _wait_for_place_open_and_click(page: ChromiumPage, target_url: str, main_tab_id: str, account_id: str, max_clicks: Optional[int] = None, max_total_seconds: int = 900) -> bool:
    """
    新策略（低刷新、低干扰）：
    1) 先判断市场状态 Live / Offline
    2) Live 时：等待 Placing Open -> 点击 -> 等待 Place Success! -> 等待 You Won!/You Lost
    3) Offline 时：进入断线处理，每 30 秒刷新一次，直到恢复 Live
    """
    _switch_to_main_and_open(page, main_tab_id, target_url, account_id)
    import time
    log(account_id, "已切回页面，等待 2 秒加载...")
    time.sleep(2)

    def _get_market_status() -> str:
        try:
            # 优先匹配状态徽标文本
            live = page.ele(
                "xpath://span[contains(@class,'text-emerald-400') and normalize-space()='Live']",
                timeout=0.08,
            ) or page.ele("xpath://span[normalize-space()='Live']", timeout=0.08)
            if live:
                return "live"

            offline = page.ele(
                "xpath://span[contains(@class,'text-red-400') and normalize-space()='Offline']",
                timeout=0.08,
            ) or page.ele("xpath://span[normalize-space()='Offline']", timeout=0.08)
            if offline:
                return "offline"
            return "unknown"
        except Exception:
            return "unknown"

    def _wait_until_live(refresh_seconds: int = 30, market_window_seconds: int = 300) -> bool:
        log(account_id, "检测到市场 Offline，进入断线恢复流程。")
        window_start = time.time()
        last_refresh = 0.0
        while True:
            if STOP_FLAG:
                log(account_id, "收到停止信号，停止监控。")
                return False

            status = _get_market_status()
            if status == "live":
                log(account_id, "市场状态已恢复为 Live。")
                return True

            now = time.time()
            if now - last_refresh >= refresh_seconds:
                try:
                    page.refresh()
                    log(account_id, f"市场 Offline，按 {refresh_seconds}s 周期刷新并继续等待 Live...")
                except Exception as e:
                    log(account_id, f"Offline 刷新异常: {e}")
                last_refresh = now

            if now - window_start >= market_window_seconds:
                log(account_id, "Offline 已持续约 5 分钟，继续按 30 秒刷新等待恢复 Live。")
                window_start = now

            time.sleep(1)

    clicks = 0
    last_progress = time.time()
    stage = "wait_open"
    stage_start = time.time()
    none_count = 0
    round_remaining_before_click = None
    seen_success_in_round = False
    next_popup_check_at = 0.0

    while True:
        if STOP_FLAG:
            log(account_id, "收到停止信号，停止监控。")
            return False

        # 保留总超时保护；有进展时会重置 last_progress
        if time.time() - last_progress > max_total_seconds:
            log(account_id, f"长时间无进展，触发超时({max_total_seconds}s)，将结束该窗口。")
            return False

        # 弹窗处理节流
        now_ts = time.time()
        if now_ts >= next_popup_check_at:
            _check_and_handle_popups(page, main_tab_id, account_id)
            next_popup_check_at = now_ts + 0.3

        # 先判市场状态
        market_status = _get_market_status()
        if market_status == "offline":
            if not _wait_until_live(refresh_seconds=30, market_window_seconds=300):
                return False
            # 恢复 Live 后重置阶段继续
            last_progress = time.time()
            stage = "wait_open"
            stage_start = time.time()
            round_remaining_before_click = None
            seen_success_in_round = False
            continue

        try:
            # 进入倒计时代表本轮（本日）次数已结束，直接退出
            if _is_countdown_state(page):
                log(account_id, "检测到倒计时状态，结束监控。")
                return True

            # 每轮读取剩余次数，避免按点击数误差
            remaining = _get_remaining_clicks(page)
            if remaining is None:
                none_count += 1
                if none_count % 20 == 0:
                    log(account_id, "无法解析剩余次数，重试中...")
                if none_count >= 120:
                    log(account_id, "长时间无法解析剩余次数，结束本窗口。")
                    return False
                time.sleep(0.1)
                continue
            none_count = 0
            if remaining <= 0:
                log(account_id, "剩余次数为 0，结束监控。")
                return True

            placing_open = page.ele(
                "t:div@@class=flex items-center gap-2 text-xs capitalize text-emerald-400@@tx():Placing Open",
                timeout=0.05,
            )
            success = page.ele(
                "t:div@@class=text-white font-semibold text-base@@tx():Place Success!",
                timeout=0.05,
            )
            won = page.ele("xpath://*[contains(normalize-space(),'You Won')]", timeout=0.05)
            lost = page.ele("xpath://*[contains(normalize-space(),'You Lost')]", timeout=0.05)

            if stage == "wait_open":
                # Live + Placing Open 才尝试点击
                if market_status == "live" and placing_open:
                    log(account_id, "市场 Live 且检测到 Placing Open，随机点击 Long/Short。")
                    choice = random.choice(["long", "short"])
                    clicked = False
                    if choice == "long":
                        clicked = _try_detect_and_click(
                            page,
                            "t:div@@class=w-full py-3 rounded-lg font-medium text-center transition-all "
                            "flex items-center justify-center gap-2@@tx():Place Long",
                            account_id=account_id,
                            timeout=4,
                        ) or _try_detect_and_click(
                            page,
                            "xpath://div[contains(normalize-space(),'Place Long')]",
                            account_id=account_id,
                            timeout=4,
                        )
                    else:
                        clicked = _try_detect_and_click(
                            page,
                            "t:div@@class=w-full py-3 rounded-lg font-medium text-center transition-all "
                            "flex items-center justify-center gap-2@@tx():Place Short",
                            account_id=account_id,
                            timeout=4,
                        ) or _try_detect_and_click(
                            page,
                            "xpath://div[contains(normalize-space(),'Place Short')]",
                            account_id=account_id,
                            timeout=4,
                        )
                    if clicked:
                        clicks += 1
                        last_progress = time.time()
                        stage = "wait_result"
                        stage_start = time.time()
                        round_remaining_before_click = remaining
                        seen_success_in_round = False
                        log(account_id, "已点击，直接等待本轮结果（胜负/次数变化）。")
                        time.sleep(0.2)
                        continue

            elif stage == "wait_result":
                # Success 仅做辅助日志，不作为硬条件
                if success and not seen_success_in_round:
                    seen_success_in_round = True
                    log(account_id, "检测到 Place Success!，继续等待胜负结果。")
                    last_progress = time.time()

                if won or lost:
                    result_text = "You Won!" if won else "You Lost"
                    log(account_id, f"本轮结果: {result_text}，继续下一轮。")
                    last_progress = time.time()
                    stage = "wait_open"
                    stage_start = time.time()
                    round_remaining_before_click = None
                    seen_success_in_round = False
                    continue

                # 胜负文案偶发缺失时，用“剩余次数下降”判定本轮完成
                if round_remaining_before_click is not None and remaining < round_remaining_before_click:
                    log(account_id, f"检测到剩余次数下降 {round_remaining_before_click}->{remaining}，视为本轮完成，继续下一轮。")
                    last_progress = time.time()
                    stage = "wait_open"
                    stage_start = time.time()
                    round_remaining_before_click = None
                    seen_success_in_round = False
                    continue

                # 某些情况下结果文案不出现，但界面已回到开盘，视为本轮已结束
                if placing_open and time.time() - stage_start > 4:
                    log(account_id, "结果文案未出现但已重新开盘，视为本轮结束，继续下一轮。")
                    last_progress = time.time()
                    stage = "wait_open"
                    stage_start = time.time()
                    round_remaining_before_click = None
                    seen_success_in_round = False
                    continue

                if time.time() - stage_start > 40:
                    log(account_id, "等待本轮结果超时，重置到下一轮等待。")
                    stage = "wait_open"
                    stage_start = time.time()
                    round_remaining_before_click = None
                    seen_success_in_round = False
                    continue

            if PERF_DEBUG:
                perf_log(
                    account_id,
                    f"stage={stage}, market={market_status}, remaining={remaining}, "
                    f"open={bool(placing_open)}, success={bool(success)}, won={bool(won)}, lost={bool(lost)}",
                )

            time.sleep(0.1)
        except Exception as e:
            log(account_id, f"检测状态异常: {e}")
            time.sleep(0.2)


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
    if PERF_DEBUG:
        log(account_id, "性能调试日志已开启（AIX_PERF_DEBUG=1）。")
    try:
        page.get(url)
        log(account_id, f"已打开网址: {url}")

        # 记录任务页主标签，供后续切回
        main_tab_id = page.tab_id

        # 进入网站后解锁OKX钱包（不传 log，兼容旧版 okx_wallet；新版 okx_wallet 可接受 log 参数，此处不传则详细日志走 print）
        wallet = OKXWallet(page, log=lambda msg: log(account_id, msg))
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
