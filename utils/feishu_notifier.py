# @Project: https://github.com/Jedore/ctp.examples
# @File:    feishu_notifier.py
# @Time:    2026/03/11
# @Author:  Assistant
# @Description: 飞书机器人通知模块

import json
import urllib.request
import urllib.error
from datetime import datetime
from typing import Optional, List, Dict

# 飞书 webhook 地址
FEISHU_WEBHOOK = "https://open.feishu.cn/open-apis/bot/v2/hook/6afaaa96-9685-4de8-8136-4de3b7eb4b42"


class FeishuNotifier:
    """飞书通知类"""
    
    def __init__(self, webhook_url: str = FEISHU_WEBHOOK):
        self.webhook_url = webhook_url
        self.last_notify_time = {}  # 记录每个信号最后通知时间
    
    def send_text(self, text: str) -> bool:
        """
        发送文本消息
        
        Args:
            text: 消息内容
            
        Returns:
            bool: 发送是否成功
        """
        payload = {
            "msg_type": "text",
            "content": {
                "text": text
            }
        }
        return self._send_payload(payload)
    
    def send_post(self, title: str, content_items: List[Dict]) -> bool:
        """
        发送 Post 消息（富文本）
        
        Args:
            title: 标题
            content_items: 内容列表，每项是一个 dict
            
        Returns:
            bool: 发送是否成功
        """
        payload = {
            "msg_type": "post",
            "content": {
                "post": {
                    "zh_cn": {
                        "title": title,
                        "content": [content_items]
                    }
                }
            }
        }
        return self._send_payload(payload)
    
    def send_volatility_switch_signal(self, symbol: str, details: dict) -> bool:
        """
        发送波动率切换信号
        
        Args:
            symbol: 合约代码
            details: 波动率详情
            
        Returns:
            bool: 发送是否成功
        """
        # 检查冷却时间（5 分钟内不重复发送同一合约）
        now = datetime.now()
        last_time = self.last_notify_time.get(symbol)
        if last_time and (now - last_time).total_seconds() < 7200:
            print(f"[飞书通知] {symbol} 在冷却期内，跳过发送")
            return False
        
        # 构建消息
        title = "🔔 高波切低波信号预警"
        
        # 获取信号状态
        is_switch = details.get('is_switch', False)
        percentile = details.get('percentile', 0)
        hv_ratio = details.get('hv_ratio', 0)
        ewma_declining = details.get('ewma_declining', False)
        current_hv = details.get('current_hv', 0)
        fast_hv = details.get('fast_hv', 0)
        slow_hv = details.get('slow_hv', 0)
        
        # 状态图标
        switch_icon = "✅" if is_switch else "❌"
        ewma_icon = "📉" if ewma_declining else "📊"
        
        # 构建内容
        content = [
            {"tag": "text", "text": f"合约：{symbol} "},
            {"tag": "text", "text": f"{switch_icon}\n"},
            {"tag": "text", "text": f"当前波动率：{current_hv:.2f}%\n"},
            {"tag": "text", "text": f"快速 HV: {fast_hv:.2f}%\n"},
            {"tag": "text", "text": f"慢速 HV: {slow_hv:.2f}%\n"},
            {"tag": "text", "text": f"HV 比率：{hv_ratio:.3f} (阈值：0.6)\n"},
            {"tag": "text", "text": f"波动率百分位：{percentile:.1%} (阈值：25%)\n"},
            {"tag": "text", "text": f"EWMA 趋势：{ewma_icon} {'下降' if ewma_declining else '未下降'}\n"},
            {"tag": "text", "text": f"\n检测时间：{now.strftime('%Y-%m-%d %H:%M:%S')}"},
        ]
        
        # 如果是切换信号，添加醒目提示
        if is_switch:
            content.insert(0, {"tag": "text", "text": "⚠️ "})
            content.insert(1, {"tag": "text", "text": "高波切低波信号触发!\n"})
            content.insert(2, {"tag": "text", "text": "\n"})
        
        success = self.send_post(title, content)
        
        if success:
            self.last_notify_time[symbol] = now
            print(f"[飞书通知] {symbol} 信号已发送")
        else:
            print(f"[飞书通知] {symbol} 信号发送失败")
        
        return success
    
    def send_test_signal(self) -> bool:
        """
        发送测试信号
        
        Returns:
            bool: 发送是否成功
        """
        test_details = {
            'is_switch': True,
            'percentile': 0.20,
            'hv_ratio': 0.55,
            'ewma_declining': True,
            'current_hv': 15.5,
            'fast_hv': 12.0,
            'slow_hv': 21.8
        }
        
        print("[飞书通知] 发送测试信号...")
        return self.send_volatility_switch_signal("TEST", test_details)
    
    def send_high_volatility_alert(self, symbol: str, details: dict) -> bool:
        """
        发送高波动率告警
        
        Args:
            symbol: 合约代码
            details: 高波动率详情
            
        Returns:
            bool: 发送是否成功
        """
        # 构建消息
        title = "⚠️ 波动率偏高预警"
        
        # 获取数据
        current_hv = details.get('current_hv', 0)
        hv_75_percentile = details.get('hv_75_percentile', 0)
        hv_ratio = details.get('hv_ratio', 0)
        reason = details.get('reason', '')
        
        # 构建内容
        content = [
            {"tag": "text", "text": f"合约：{symbol}\n"},
            {"tag": "text", "text": f"当前波动率：{current_hv:.2f}%\n"},
            {"tag": "text", "text": f"75 分位波动率：{hv_75_percentile:.2f}%\n"},
            {"tag": "text", "text": f"比率：{hv_ratio:.2f} (阈值：1.2)\n"},
            {"tag": "text", "text": f"\n{reason}\n"},
            {"tag": "text", "text": f"\n⚠️ 提示：注意行情回弱\n"},
            {"tag": "text", "text": f"检测时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"},
        ]
        
        success = self.send_post(title, content)
        
        if success:
            print(f"[飞书通知] {symbol} 高波动率告警已发送")
        else:
            print(f"[飞书通知] {symbol} 高波动率告警发送失败")
        
        return success
    
    def send_strategy_signal(self, symbol: str, signal_data: dict) -> bool:
        """
        发送策略开仓信号
        
        Args:
            symbol: 合约代码
            signal_data: 策略信号数据
                - signal_type: 信号类型 (ENTRY_LONG/EXIT_LONG)
                - price: 价格
                - stop_loss: 止损价
                - position_size: 手数
                - reason: 信号原因
                - time: 信号时间
            
        Returns:
            bool: 发送是否成功
        """
        # 检查冷却时间（5 分钟内不重复发送同一合约的同类型信号）
        now = datetime.now()
        signal_key = f"{symbol}_{signal_data.get('signal_type', 'UNKNOWN')}"
        last_time = self.last_notify_time.get(signal_key)
        if last_time and (now - last_time).total_seconds() < 7200:
            print(f"[飞书通知] {signal_key} 在冷却期内，跳过发送")
            return False
        
        # 构建消息
        signal_type = signal_data.get('signal_type', 'ENTRY_LONG')
        if signal_type == 'ENTRY_LONG':
            title = "📈 策略开仓信号 - 做多"
            signal_icon = "🟢"
        elif signal_type == 'EXIT_LONG':
            title = "📉 策略平仓信号 - 平多"
            signal_icon = "🔴"
        else:
            title = "📊 策略信号"
            signal_icon = "⚪"
        
        # 获取数据
        price = signal_data.get('price', 0)
        stop_loss = signal_data.get('stop_loss', 0)
        position_size = signal_data.get('position_size', 1)
        reason = signal_data.get('reason', '')
        signal_time = signal_data.get('time', now.strftime('%Y-%m-%d %H:%M:%S'))
        
        # 构建内容
        content = [
            {"tag": "text", "text": f"{signal_icon} 合约：{symbol}\n"},
            {"tag": "text", "text": f"信号类型：{signal_type}\n"},
            {"tag": "text", "text": f"入场价格：{price:.2f}\n"},
            {"tag": "text", "text": f"止损价格：{stop_loss:.2f}\n"},
            {"tag": "text", "text": f"开仓手数：{position_size} 手\n"},
            {"tag": "text", "text": f"\n信号原因：{reason}\n"},
            {"tag": "text", "text": f"\n信号时间：{signal_time}"},
        ]
        
        success = self.send_post(title, content)
        
        if success:
            self.last_notify_time[signal_key] = now
            print(f"[飞书通知] {symbol} 策略信号已发送")
        else:
            print(f"[飞书通知] {symbol} 策略信号发送失败")
        
        return success

    def send_breakout_signal(self, symbol: str, details: dict) -> bool:
        """
        发送突破信号通知

        Args:
            symbol: 合约代码
            details: 突破信号详情

        Returns:
            bool: 发送是否成功
        """
        now = datetime.now()
        signal_key = f"{symbol}_BREAKOUT"
        last_time = self.last_notify_time.get(signal_key)
        if last_time and (now - last_time).total_seconds() < 7200:
            print(f"[飞书通知] {signal_key} 在冷却期内，跳过发送")
            return False

        title = "🚀 波动率挤压突破信号"
        direction = details.get('direction', 'unknown')
        squeeze = details.get('squeeze', {})
        candle = details.get('candlestick', {})
        time_filter = details.get('time_filter', {})

        content = [
            {"tag": "text", "text": f"合约：{symbol}\n"},
            {"tag": "text", "text": f"方向：{direction}\n"},
            {"tag": "text", "text": f"突破强度：{squeeze.get('breakout_strength', 0):.2%}\n"},
            {"tag": "text", "text": f"ATR 百分位：{squeeze.get('atr_percentile', 0):.2%}\n"},
            {"tag": "text", "text": f"ATR 扩张：{squeeze.get('atr_expanding', False)}\n"},
            {"tag": "text", "text": f"成交量确认：{squeeze.get('vol_confirmed', False)}\n"},
            {"tag": "text", "text": f"K 线类型：{candle.get('candle_type', 'unknown')} 实体比例：{candle.get('body_ratio', 0):.2%}\n"},
            {"tag": "text", "text": f"交易时段：{time_filter.get('trading_session', 'unknown')}\n"},
            {"tag": "text", "text": f"检测时间：{now.strftime('%Y-%m-%d %H:%M:%S')}\n"},
        ]

        success = self.send_post(title, content)
        if success:
            self.last_notify_time[signal_key] = now
            print(f"[飞书通知] {symbol} 突破信号已发送")
        else:
            print(f"[飞书通知] {symbol} 突破信号发送失败")

        return success

    def _send_payload(self, payload: dict) -> bool:
        """
        发送请求到飞书

        Args:
            payload: 消息 payload

        Returns:
            bool: 发送是否成功
        """
        try:
            data = json.dumps(payload).encode('utf-8')
            req = urllib.request.Request(
                self.webhook_url,
                data=data,
                headers={'Content-Type': 'application/json'}
            )

            with urllib.request.urlopen(req, timeout=10) as response:
                result = json.loads(response.read().decode('utf-8'))

                if result.get('code') == 0 or result.get('StatusCode') == 0:
                    print(f"[飞书通知] 发送成功")
                    return True
                else:
                    print(f"[飞书通知] 发送失败：{result}")
                    return False

        except urllib.error.URLError as e:
            print(f"[飞书通知] 网络错误：{e}")
            return False
        except Exception as e:
            print(f"[飞书通知] 未知错误：{e}")
            return False


def send_feishu_breakout_signal(symbol: str, details: dict) -> bool:
    """
    快捷函数：发送突破信号

    Args:
        symbol: 合约代码
        details: 突破信号数据

    Returns:
        bool: 发送是否成功
    """
    notifier = FeishuNotifier()
    return notifier.send_breakout_signal(symbol, details)


# NOTE: _send_payload 方法应当属于 FeishuNotifier 类，已经在类中定义，
# 这里不再重复定义。

def send_feishu_test() -> bool:
    """
    快捷函数：发送测试消息
    
    Returns:
        bool: 发送是否成功
    """
    notifier = FeishuNotifier()
    return notifier.send_test_signal()


def send_feishu_signal(symbol: str, details: dict) -> bool:
    """
    快捷函数：发送波动率切换信号
    
    Args:
        symbol: 合约代码
        details: 波动率详情
        
    Returns:
        bool: 发送是否成功
    """
    notifier = FeishuNotifier()
    return notifier.send_volatility_switch_signal(symbol, details)


def send_feishu_high_volatility_alert(symbol: str, details: dict) -> bool:
    """
    快捷函数：发送高波动率告警
    
    Args:
        symbol: 合约代码
        details: 高波动率详情
        
    Returns:
        bool: 发送是否成功
    """
    notifier = FeishuNotifier()
    return notifier.send_high_volatility_alert(symbol, details)


def send_feishu_strategy_signal(symbol: str, signal_data: dict) -> bool:
    """
    快捷函数：发送策略开仓信号
    
    Args:
        symbol: 合约代码
        signal_data: 策略信号数据
        
    Returns:
        bool: 发送是否成功
    """
    notifier = FeishuNotifier()
    return notifier.send_strategy_signal(symbol, signal_data)


if __name__ == '__main__':
    # 测试发送
    print("=" * 50)
    print("飞书通知测试")
    print("=" * 50)
    
    # 发送测试消息
    success = send_feishu_test()
    
    if success:
        print("\n✓ 测试消息发送成功!")
    else:
        print("\n✗ 测试消息发送失败，请检查网络或 webhook 配置")