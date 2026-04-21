"""
通知推送模块 - warnings/notifier.py
发送邮件、微信等推送通知
"""

import sys
import os
import smtplib
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models import get_session, Warning, Config
from utils import get_logger

logger = get_logger(__name__)


class Notifier:
    """通知推送器"""
    
    def __init__(self):
        self.session = get_session()
        self._load_config()
    
    def _load_config(self):
        """加载推送配置"""
        configs = self.session.query(Config).all()
        
        self.email_config = {
            'smtp_server': 'smtp.qq.com',
            'smtp_port': 465,
            'sender': '',
            'password': '',
            'receiver': ''
        }
        
        self.wechat_config = {
            'sckey': ''
        }
        
        for c in configs:
            if c.config_key == 'email_smtp_server':
                self.email_config['smtp_server'] = c.config_value
            elif c.config_key == 'email_sender':
                self.email_config['sender'] = c.config_value
            elif c.config_key == 'email_password':
                self.email_config['password'] = c.config_value
            elif c.config_key == 'email_receiver':
                self.email_config['receiver'] = c.config_value
            elif c.config_key == 'wechat_sckey':
                self.wechat_config['sckey'] = c.config_value
    
    def send_email(self, subject, content):
        """
        发送邮件
        Args:
            subject: 邮件主题
            content: 邮件内容
        Returns:
            bool: 是否成功
        """
        if not self.email_config['sender'] or not self.email_config['password']:
            logger.warning("邮件配置不完整，跳过发送")
            return False
        
        try:
            msg = MIMEMultipart()
            msg['From'] = self.email_config['sender']
            msg['To'] = self.email_config['receiver']
            msg['Subject'] = subject
            
            msg.attach(MIMEText(content, 'plain', 'utf-8'))
            
            with smtplib.SMTP_SSL(self.email_config['smtp_server'], self.email_config['smtp_port']) as server:
                server.login(self.email_config['sender'], self.email_config['password'])
                server.send_message(msg)
            
            logger.info(f"邮件已发送: {subject}")
            return True
            
        except Exception as e:
            logger.error(f"发送邮件失败: {e}")
            return False
    
    def send_wechat(self, title, content):
        """
        发送微信消息（使用Server酱）
        Args:
            title: 消息标题
            content: 消息内容
        Returns:
            bool: 是否成功
        """
        if not self.wechat_config['sckey']:
            logger.warning("微信配置不完整，跳过发送")
            return False
        
        try:
            url = f"https://sctapi.ftqq.com/{self.wechat_config['sckey']}.send"
            data = {
                'title': title,
                'desp': content
            }
            
            response = requests.post(url, data=data, timeout=10)
            
            if response.status_code == 200:
                result = response.json()
                if result.get('code') == 0:
                    logger.info(f"微信消息已发送: {title}")
                    return True
                else:
                    logger.warning(f"微信发送失败: {result.get('message')}")
                    return False
            else:
                logger.warning(f"微信发送失败: HTTP {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"发送微信消息失败: {e}")
            return False
    
    def send_warning(self, warning):
        """
        发送预警通知
        Args:
            warning: Warning对象
        Returns:
            bool: 是否成功
        """
        if warning.is_sent:
            return False
        
        # 构建消息内容
        level_emoji = {
            'high': '🔴',
            'medium': '🟡',
            'low': '🟢'
        }
        
        title = f"{level_emoji.get(warning.level, '⚠️')} 【理财助手预警】{warning.name}"
        
        content = f"""
标的：{warning.name} ({warning.code})
时间：{warning.warning_time.strftime('%Y-%m-%d %H:%M:%S')}
类型：{warning.warning_type}
级别：{warning.level}

详情：
{warning.message}

建议：
{warning.suggestion if warning.suggestion else '请关注'}

---
本消息由个人AI理财助手自动发送
"""
        
        # 发送邮件
        email_success = self.send_email(title, content)
        
        # 发送微信
        wechat_success = self.send_wechat(title, content)
        
        # 更新发送状态
        if email_success or wechat_success:
            warning.is_sent = True
            warning.sent_at = datetime.now()
            warning.sent_method = 'email' if email_success else 'wechat'
            if wechat_success:
                warning.sent_method += '+wechat' if email_success else 'wechat'
            
            self.session.commit()
            logger.info(f"预警已发送: {warning.name} - {warning.warning_type}")
            return True
        
        return False
    
    def send_daily_report(self, report_data):
        """
        发送每日报告
        Args:
            report_data: 报告数据
        Returns:
            bool: 是否成功
        """
        title = f"📊 理财助手日报 - {datetime.now().strftime('%Y-%m-%d')}"
        
        content = f"""
{report_data.get('content', '')}

---
生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
        
        email_success = self.send_email(title, content)
        wechat_success = self.send_wechat(title, content)
        
        return email_success or wechat_success
    
    def send_future_signals_alert(self, signals_data):
        """
        发送未来信号告警 (加仓/止盈建议和风险预警)
        Args:
            signals_data: 从 /api/holdings/future-signals 返回的数据
        Returns:
            dict: {sent: bool, email: bool, wechat: bool, alerts_count: int}
        """
        if not signals_data:
            return {'sent': False, 'email': False, 'wechat': False, 'alerts_count': 0}
        
        risk_alerts = signals_data.get('risk_alerts', [])
        action_suggestions = signals_data.get('action_suggestions', [])
        unheld_recommendations = signals_data.get('unheld_recommendations', [])[:3]  # 仅取前3个推荐
        
        # 如果没有任何信号，不发送
        if not (risk_alerts or action_suggestions or unheld_recommendations):
            return {'sent': False, 'email': False, 'wechat': False, 'alerts_count': 0}
        
        # 构建消息标题
        alert_count = len(risk_alerts) + len(action_suggestions)
        title = f"📈 【理财助手未来信号】{datetime.now().strftime('%Y-%m-%d %H:%M')} - {alert_count}个操作"
        
        # 构建消息内容
        content = "═" * 50 + "\n"
        content += "📊 未来信号播报\n"
        content += "═" * 50 + "\n\n"
        
        # 添加风险预警
        if risk_alerts:
            content += "🔴 风险预警\n"
            content += "─" * 50 + "\n"
            for alert in risk_alerts[:5]:  # 最多显示5个
                level_text = "【严重】" if alert.get('level') == 'high' else "【中等】"
                content += f"{level_text} {alert.get('name', 'N/A')} ({alert.get('code', 'N/A')})\n"
                content += f"   ⬇️ 下跌概率: {alert.get('down_probability', 0):.1f}%\n"
                content += f"   📍 建议: 考虑减仓或设置止损\n"
            content += "\n"
        
        # 添加操作建议
        if action_suggestions:
            content += "🟡 操作建议\n"
            content += "─" * 50 + "\n"
            for action in action_suggestions[:10]:  # 最多显示10个
                action_emoji = "📈" if action.get('action') == 'take_profit' else "📊"
                action_text = "止盈" if action.get('action') == 'take_profit' else "加仓"
                content += f"{action_emoji} 【{action_text}】{action.get('name', 'N/A')} ({action.get('code', 'N/A')})\n"
                content += f"   💰 盈利率: {action.get('profit_rate', 0):.1f}%\n"
                content += f"   💭 理由: {action.get('reason', 'N/A')}\n"
            content += "\n"
        
        # 添加推荐资产
        if unheld_recommendations:
            content += "🟢 推荐资产 (今日TOP)\n"
            content += "─" * 50 + "\n"
            for rec in unheld_recommendations:
                content += f"⭐ {rec.get('name', 'N/A')} ({rec.get('code', 'N/A')})\n"
                content += f"   📊 类型: {rec.get('type', 'N/A')}\n"
                content += f"   🎯 20日目标价: ¥{rec.get('target_price_20d', 0):.2f}\n"
                content += f"   📈 上涨概率(20d): {rec.get('up_probability_20d', 0):.1f}%\n"
                content += f"   ⭐ 评分: {rec.get('score', 0):.1f}\n"
            content += "\n"
        
        content += "─" * 50 + "\n"
        content += f"📅 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        content += "💡 提示: 以上为AI分析结果，请结合自身情况判断\n"
        content += "本消息由个人AI理财助手自动发送\n"
        
        # 发送通知
        email_success = self.send_email(title, content) if self.email_config['sender'] else False
        wechat_success = self.send_wechat(title, content) if self.wechat_config['sckey'] else False
        
        sent = email_success or wechat_success
        if sent:
            logger.info(f"未来信号告警已发送: {alert_count} 个操作信号")
        
        return {
            'sent': sent,
            'email': email_success,
            'wechat': wechat_success,
            'alerts_count': alert_count
        }
    
    def test_push(self):
        """
        测试推送
        Returns:
            bool: 是否成功
        """
        title = "🔔 理财助手测试推送"
        content = f"""
这是一条测试消息。

如果您收到此消息，说明推送配置正确。

时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
        
        email_success = self.send_email(title, content)
        wechat_success = self.send_wechat(title, content)
        
        return {
            'email': email_success,
            'wechat': wechat_success
        }
    
    def close(self):
        """关闭资源"""
        self.session.close()


# 测试代码
if __name__ == '__main__':
    notifier = Notifier()
    
    # 测试推送
    result = notifier.test_push()
    print(f"测试结果: {result}")
    
    notifier.close()