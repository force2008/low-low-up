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
  - ratio / ration / position_ratio: 该目标账户的跟单模式/比例（三选一即可，ratio 优先级最高）。
    值的语义：
      · 值 > 0：正常跟单，按比例缩放（ration=2 → 两倍标准仓位；0.5 → 半仓跟单）
      · 值 == 0：【清仓模式】目标持仓强制置 0，
        后续比对会判定为"实际持仓超额"，走平仓分支按限价挂单：
          多平 → 挂卖一价 BidPrice1；空平 → 挂买一价 AskPrice1。
        所有已持有的跟单品都会被平掉，exclude 里的品种不受影响（两边都跳过，继续手工持有）。
      · 值 == -N，例如 -1 / -2 / -3.5：【对冲模式】
        取 source_account 原始持仓 hold-std-{source_account}.json：
          - 方向**全部反转**（买→卖，卖→买，2↔3）；
          - 手数再乘 |ratio|（-1 → ×1；-2 → ×2）。
        例：source wangk0402 持 FG 多 1 手，ration=-1 → target 持 FG 空 1 手；ration=-2 → 空 2 手。
        相当于"跟单账户做 wangk0402 原始账户的对手方对冲"。
    未配置时回退到命令行 --ratio（默认 1.0，正常跟单）。
  - exclude / exclude_products / exclude_symbols: 该目标账户**不跟单**的品种列表，可选。
    可以写列表或逗号分隔字符串，例：exclude=["SC","FG"] 或 exclude="sc, FG"。
    命中规则：合约代码**前缀**匹配即跳过。
    效果：① hold-std 里的该品种目标持仓不会开仓；
          ② CTP 端已有的该品种老持仓也不参与对比（不会被当成"超额"触发强制平仓）。
    这意味着这些品种完全由用户自己手工管理，跟单系统对它们"既不买也不卖"。

示例：
    ACCOUNT_TARGETS = {
        "wangk0402": [
            {"env_name": "simu", "user_id": "17882", "password": "123456", "ration": 2},
            # 清仓示例：某时刻临时把 17882 全平掉
            # {"env_name": "simu", "user_id": "17882", "password": "123456", "ratio": 0},
        ],
        "zhouzhou": [
            # 对冲示例：zhouzhou 原始账户多 1 手 FG，目标账户就空 1 手 FG
            {"env_name": "simu", "user_id": "17883", "password": "123456", "ratio": -1},
        ],
        "WQ1017": [
            # exclude 同时生效：yuqj0821 不对 SC/FG 跟单，其余按 1 倍
            {"env_name": "online", "user_id": "yuqj0821", "password": "yqj123456",
             "ratio": 1, "exclude": ["sc", "FG"]},
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
        {"env_name": "online", "user_id": "fy0228", "password": "fy123456","ratio":2},
        {"env_name": "online", "user_id": "sxk0812", "password": "sxk123456","ratio":2},
        {"env_name": "online", "user_id": "yq02", "password": "yq123456","ratio":2}
    ],
    "wangk0402": [
        # {"env_name": "online", "user_id": "sxk0812", "password": "sxk123456","ratio":1},
        # {"env_name": "online", "user_id": "yq02", "password": "yq123456","ratio":1}
    ],
    "WQ1017":[
        {"env_name": "online", "user_id": "yuqj0821", "password": "yqj123456","ratio":1, "exclude": ["sc","FG"]},
    ]
}
