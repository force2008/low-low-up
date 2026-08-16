# -*- coding: utf-8 -*-
"""
源账户到目标 CTP 账户的映射配置

用于多账户仓位同步：
  - 每个源账户对应一个或多个目标 CTP 账户
  - run_pipeline.py 以单进程方式运行，执行一次导出后，
    依次为每个源账户生成 hold-std 文件，并分别同步到对应目标账户
  - 如需只同步某个账户，只需在配置中保留该账户即可

配置说明：
  - env_name: 目标账户所属 CTP 环境，可选，默认使用 run_pipeline.py 启动时传入的环境
  - user_id/password: 目标账户登录信息（必填）
  - broker_id/authcode/appid/user_product_info: 可选，默认从 config.envs[env_name] 继承
  - ratio / ration / position_ratio: 该目标账户的持仓同步比例，可选。
    默认为 1.0（即标准仓位）。例如 ration=2 表示该账户同步两倍标准仓位。
    字段优先级：ratio > ration > position_ratio；都没有则使用命令行 --ratio（默认 1.0）。

示例：
    ACCOUNT_TARGETS = {
        "wangk0402": [
            {"env_name": "simu", "user_id": "17882", "password": "123456", "ration": 2},
        ],
        "zhouzhou": [
            {"env_name": "simu", "user_id": "17883", "password": "123456", "ratio": 0.5},
        ],
    }

注意：
  同一个源账户映射到多个目标账户时，请写在一个列表里，例如：
    "wangk0402": [
        {"env_name": "online", "user_id": "sxk0812", "password": "..."},
        {"env_name": "online", "user_id": "yq02", "password": "..."},
    ]
  不要写成重复的 dict key，否则后面的会覆盖前面的。
"""

ACCOUNT_TARGETS = {
    # "wangk0402": [
    #     {"env_name": "simu", "user_id": "16599", "password": "123456"},
    # ],
    # "zhouzhou": [
    #     {"env_name": "simu", "user_id": "17872", "password": "123456"},
    # ],
    # "wangxy0617": [
    #     {"env_name": "simu", "user_id": "17882", "password": "123456"},
    # ],
    "wangxy0617": [
        {"env_name": "online", "user_id": "yqj0929", "password": "041354","ratio":2},
    ],
    "wangk0402": [
        {"env_name": "online", "user_id": "sxk0812", "password": "sxk123456","ratio":1},
    ],
    "WQ1017":[
        {"env_name": "online", "user_id": "yq02", "password": "yq123456","ratio":1},
    ]
}
