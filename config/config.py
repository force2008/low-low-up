# @Project: https://github.com/Jedore/ctp.examples
# @File:    config.py
# @Time:    21/07/2024 14:29
# @Author:  Jedore
# @Email:   jedorefight@gmail.com
# @Addr:    https://github.com/Jedore

import os
import platform
import sys
import argparse

# 可以使用监控平台 http://openctp.cn 查看前置服务是否正常
# 账户需要到 openctp 公众号, 发送"注册"

# TTS 提供的环境
envs = {
    "7x24": {
        "td": "tcp://724.openctp.cn:30001",
        "md": "tcp://724.openctp.cn:30011",
        "user_id": "17156",
        "password": "123456",
        "broker_id": "9999",
        "authcode": "",
        "appid": "",
        "user_product_info": "",
    },
    "online": {
        "td": "tcp://116.62.52.86:11001",
        "md": "tcp://101.226.253.150:41213",
        "user_id": "yqj0929",
        "password": "041354",
        "broker_id": "0268",
        "authcode": "zTvCzT7YVtrJwdvs",
        "appid": "client_yqj_v1.0",
        "user_product_info": "",
    },
    # 仿真
    "simu": {
        "td": "tcp://trading.openctp.cn:30002",
        "md": "",
        "user_id": "",
        "password": "",
        "broker_id": "9999",
        "authcode": "",
        "appid": "",
        "user_product_info": "",
    },
    # 仿真vip
    "simu-vip": {
        "td": "tcp://vip.openctp.cn:30003",
        "md": "",
        "user_id": "",
        "password": "",
        "broker_id": "9999",
        "authcode": "",
        "appid": "",
        "user_product_info": "",
    },
}


def get_env_config():
    """
    获取当前环境的配置
    
    优先级：
    1. 命令行参数（如：python script.py online）
    2. 环境变量 CTP_ENV（可设置为：online, 7x24, simu, simu-vip）
    3. 操作系统自动判断（Linux -> online, Windows -> 7x24）
    4. 默认 7x24
    
    Returns:
        dict: 环境配置字典
    """
    # 1. 检查命令行参数
    if len(sys.argv) > 1:
        env_name = sys.argv[1].lower()
        if env_name in envs:
            return envs[env_name]
    
    # 2. 优先使用环境变量
    env_name = os.getenv("CTP_ENV")
    if env_name and env_name in envs:
        return envs[env_name]
    
    # 3. 根据操作系统自动判断（已修改：所有系统默认使用 7x24）
    system = platform.system().lower()
    # 移除操作系统判断，所有系统默认使用 7x24
    # if system == "linux":
    #     # Linux 系统默认使用线上环境
    #     return envs["online"]
    # elif system == "windows":
    #     # Windows 系统默认使用开发环境
    #     return envs["7x24"]
    
    # 4. 默认使用 7x24
    return envs["7x24"]
