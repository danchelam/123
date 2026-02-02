"""
OKX钱包自动化操作模块
分为解锁、连接、签名、确认等接口，便于主流程灵活调用。
"""

from DrissionPage import ChromiumPage
from typing import Optional

class OKXWallet:
    """
    OKX钱包自动化操作模块
    """
    DEFAULT_PASSWORD = "DD112211"

    def __init__(self, page: ChromiumPage):
        """
        初始化，传入已打开OKX弹窗的ChromiumPage对象。
        """
        self.page = page  # 保存page对象，供后续方法使用

    def unlock(self, password: str = None) -> bool:
        """
        输入密码并点击解锁按钮，完成钱包解锁。
        【参考DPai.py重构】通过对比操作前后的标签页列表来精确查找弹窗，并使用更具体的定位符。
        :param password: 钱包密码，默认用类属性DEFAULT_PASSWORD
        :return: 解锁是否成功
        """
        if password is None:
            password = self.DEFAULT_PASSWORD
            
        import time
        
        # 记录操作前的标签页信息
        before_tabs = set(self.page.tab_ids)
        main_tab_id = self.page.tab_id
        
        # 1. 在主标签页打开插件URL，这可能会直接在该标签页打开，或弹出一个新窗口
        okx_url = "chrome-extension://mcohilncbfahbmgdjkbpemcciiolgcge/popup.html"
        self.page.get(okx_url)
        time.sleep(2) # 等待页面反应

        # 2. 对比标签页列表，查找新弹出的解锁窗口
        after_tabs = set(self.page.tab_ids)
        new_tab_ids = after_tabs - before_tabs
        
        unlock_tab = None
        if new_tab_ids:
            unlock_tab_id = new_tab_ids.pop()
            unlock_tab = self.page.get_tab(unlock_tab_id)
            print(f"检测到新的OKX解锁窗口, Tab ID: {unlock_tab_id}")
        else:
            # 如果没有新窗口，说明是在当前页或最新页操作
            unlock_tab = self.page.latest_tab
            print("未检测到新窗口, 尝试在当前/最新标签页操作...")

        if not hasattr(unlock_tab, 'ele'):
            print("【解锁失败】未能获取到有效的OKX解锁标签页对象。")
            return False

        print(f"尝试在Tab ID: {unlock_tab.tab_id} 上进行解锁操作...")
        
        try:
            # 3. 查找密码输入框（多轮等待，避免该电脑加载慢或选错 Tab 导致误判为“已解锁”）
            password_selectors = [
                'tag:input@@data-testid=okd-input@@type=password',
                'xpath://input[@data-testid="okd-input" and @type="password"]',
                'css:input[data-testid=okd-input][type=password]',
                'xpath://input[@type="password" and @placeholder="请输入密码"]',
                'xpath://input[@type="password"]',
                'css:input[type=password]',
            ]
            password_input = None
            for attempt in range(3):
                for sel in password_selectors:
                    password_input = unlock_tab.ele(sel, timeout=5)
                    if password_input:
                        break
                if password_input:
                    break
                time.sleep(2)
                print(f"第 {attempt + 1} 次未找到密码框，等待后重试...")

            if not password_input:
                # 未找到密码框：可能是扩展未加载、加载慢，也可能是已经解锁
                current_url = getattr(unlock_tab, "url", "") or ""
                if not current_url.startswith("chrome-extension://"):
                    print("【解锁失败】未进入 OKX 扩展页面（当前非 chrome-extension://），请检查扩展是否安装/是否被策略禁用。")
                    return False

                try:
                    page_text = unlock_tab.ele("tag:body", timeout=3).text or ""
                except Exception:
                    page_text = ""

                if not page_text.strip():
                    # 部分机器扩展页会出现“空白文本”，此时不应误判为已解锁
                    time.sleep(2)
                    try:
                        page_text = unlock_tab.ele("tag:body", timeout=3).text or ""
                    except Exception:
                        page_text = ""
                    if not page_text.strip():
                        print("【解锁失败】扩展页面文本为空，判定未解锁（可能未加载完成或页面异常）。")
                        return False

                blocked_keywords = ("ERR_BLOCKED_BY_CLIENT", "This site can’t be reached", "无法访问此网站", "ERR_FAILED")
                if any(k in page_text for k in blocked_keywords):
                    print("【解锁失败】OKX 扩展页面加载失败（疑似被阻止或扩展不可用）。")
                    return False

                lock_keywords = ("解锁", "Unlock", "请输入密码", "输入密码", "Password")
                if any(k in page_text for k in lock_keywords):
                    print("【解锁失败】页面含锁定提示但未找到密码框，判定为未解锁。")
                    return False

                # 未找到密码框且无锁定提示，按“已解锁”处理
                print("未找到密码框且无锁定提示，判定为“已解锁”。")
                return True

            # 确保焦点在输入框后输入，并仅在确认输入+点击解锁成功后才返回 True
            print("已找到密码输入框，正在输入密码并点击解锁...")
            password_input.click()
            time.sleep(0.2)
            password_input.input(password)
            time.sleep(0.5)

            unlock_btn = unlock_tab.ele('tag:button@@data-testid=okd-button@@type=submit', timeout=10)
            if not unlock_btn:
                print("【解锁失败】在弹窗中未找到OKX解锁按钮。")
                return False
            unlock_btn.click()
            time.sleep(2)  # 等待解锁生效

            # 确认解锁成功：密码框应消失（或解锁按钮消失），避免误判
            password_input_after = unlock_tab.ele('tag:input@@data-testid=okd-input@@type=password', timeout=3)
            if password_input_after:
                print("【解锁失败】点击解锁后密码框仍存在，可能未真正解锁，请检查密码或扩展。")
                return False
            print("OKX解锁操作完成（已确认密码输入并解锁成功）。")
            return True
            
        except Exception as e:
            print(f"【解锁失败】OKX解锁操作时发生未知异常: {e}")
            return False

    def click_connect(self) -> bool:
        """
        查找并点击“连接”按钮。
        :return: 操作是否成功
        """
        pass

    def click_sign(self) -> bool:
        """
        查找并点击“签名”按钮。
        :return: 操作是否成功
        """
        pass

    def click_confirm(self, before_tabs: set) -> bool:
        """
        Waits for a new tab to appear by comparing the current tabs with a provided set of tabs from before an action.
        Once the new tab is found, it finds and clicks the '确认' button.
        """
        import time
        print("准备检测OKX确认弹窗 (基于传入的前置状态)...")
        
        new_tab = None
        
        for i in range(15): 
            current_tabs = set(self.page.tab_ids)
            new_tab_ids = current_tabs - before_tabs
            
            if new_tab_ids:
                new_tab_id = new_tab_ids.pop()
                print(f"检测到新的OKX弹窗, Tab ID: {new_tab_id}")
                time.sleep(1)
                try:
                    new_tab = self.page.get_tab(new_tab_id)
                    if new_tab:
                        break
                except Exception as e:
                    print(f"获取新Tab对象时出错: {e}, 继续尝试...")
            
            time.sleep(1)
        
        if not new_tab:
            print("轮询检测超时，未发现新弹窗。")
            return False
        
        if not hasattr(new_tab, 'ele'):
            print(f"未能获取到有效的Tab对象，获取到的是：{new_tab}")
            return False
        
        try:
            time.sleep(2)
            print(f"弹窗URL: {new_tab.url}")
            
            final_locator = "xpath://*[text()='确认']"
            confirm_button = new_tab.ele(final_locator, timeout=15)
            
            if confirm_button:
                print("在OKX弹窗中找到'确认'按钮，开始循环点击...")
                clicked_successfully = False
                for i in range(5):
                    try:
                        btn_to_click = new_tab.ele(final_locator, timeout=2)
                        if btn_to_click and btn_to_click.states.is_displayed:
                            print(f"尝试第 {i+1} 次点击'确认'按钮...")
                            btn_to_click.click(by_js=True)
                            time.sleep(1) 
                        else:
                            print("'确认'按钮已不再可见，视为点击成功。")
                            clicked_successfully = True
                            break
                    except Exception as click_err:
                        print(f"点击时发生异常（可能弹窗已关闭），视为点击成功: {click_err}")
                        clicked_successfully = True
                        break
                
                if not clicked_successfully:
                    print("已达到最大点击次数，按钮可能依然存在。")
                
                return True
            else:
                print("在OKX弹窗中未找到'确认'按钮。")
                return False
        
        except Exception as e:
            print(f"处理OKX确认弹窗时发生异常: {e}")
            return False 

# 移除了 handle_spin_confirmation 方法，保持库的通用性。
# 原有的 click_confirm 方法保持不变。 