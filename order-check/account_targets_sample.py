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

  ------------------------------------------------------------------
  【跟单模式 / 比例】三选一即可，ratio 优先级最高：
  - ratio / ration / position_ratio
    值的语义：
      · 值 > 0：正常跟单，按比例缩放（ration=2 → 两倍标准仓位；0.5 → 半仓跟单）
      · 值 == 0：【清仓模式】目标持仓强制置 0，
        后续比对会判定为"实际持仓超额"，走平仓分支按限价挂单：
          多平 → 挂卖一价 BidPrice1；空平 → 挂买一价 AskPrice1。
        所有已持有的跟单品都会被平掉，exclude 里的品种不受影响（两边都跳过，继续手工持有）。
        清仓模式下会跳过「反探单过滤」的 deny/非主流通/min_qty/min_notional（因为要把现有持仓都平掉）。
      · 值 == -N，例如 -1 / -2 / -3.5：【对冲模式】
        取 source_account 原始持仓 hold-std-{source_account}.json：
          - 方向**全部反转**（买→卖，卖→买，2↔3）；
          - 手数再乘 |ratio|（-1 → ×1；-2 → ×2）。
        例：source wangk0402 持 FG 多 1 手，ration=-1 → target 持 FG 空 1 手；ration=-2 → 空 2 手。
        相当于"跟单账户做 wangk0402 原始账户的对手方对冲"。
    未配置时回退到命令行 --ratio（默认 1.0，正常跟单）。

  ------------------------------------------------------------------
  【过滤 / 反探单字段】所有字段均为可选，未配置时使用 run_pipeline.py 顶部的全局安全默认值。
  同名字段支持多种拼写（例如 allow_level = allow_contract_level，写哪个都行）：

  1) exclude / exclude_products / exclude_symbols
     语义：该目标账户**完全不碰**的品种列表。
     写法：列表或逗号分隔字符串，例：exclude=["SC","FG"] 或 exclude="sc, FG"。
     命中规则：合约代码**品种前缀**匹配即跳过（大小写不敏感）。
     效果（双端跳过，避免误平老仓）：
       ① 目标侧：hold-std 里该品种目标持仓不开仓；
       ② 实际侧：CTP 端已有的该品种老持仓不参与对比（不会被当成"超额"触发强制平仓）。
     ⇒ 这些品种完全由用户手工管理，跟单系统对它们"既不买也不卖"。

  2) allow_contract_level / allow_level / contract_level
     语义：只跟单哪些"级别的合约"。从 main_contracts_by_product.json 的 main / main2 / main3 / main4 里取具体合约代码。
     默认值（全局）：{"main", "main2", "main3", "main4"} —— 主力+次主+次次主+第4主，共 4 级。
     例：allow_contract_level=["main"] 只跟绝对主力；
         allow_contract_level=["main","main2","main3","main4"] 4 个级别都跟（=全局默认）。
     ⇒ 关键防御：源账号在冷门月份下探单时，该合约大概率不在 main~main4 里，直接被拦掉。

  3) deny / deny_products / blacklist / blacklist_products
     语义：品种级黑名单（冷门/不活跃品种，**任何情况都不开新仓**）。
     叠加规则：账号级 deny 会**追加**到 run_pipeline.py 顶部的 GLOBAL_DENY_PRODUCTS（已含 WR/FB/BB 等一批冷门）。
     例：deny=["WR","FB"] 或 deny="wr, fb"。
     与 exclude 的区别：deny 只拦"开新仓"，如果账号里已经手工持有 WR 老仓且 WR 不在 exclude 里，
       比对阶段会把这部分老仓当成"超额"触发平仓。若要 deny 且老仓也保留，请把 deny 的品种也写进 exclude。
     清仓模式(ratio=0) 下 deny 不生效（因为要把所有持仓都平掉，包括 deny 品种的老仓）。

  4) big_notional_exemption / big_exemption
     语义：大单豁免阈值（元）。如果某个合约因为"非主流通"被第 2 条拦了，
       但估算成交额 >= 此阈值，仍然放行（避免换月大资金移仓/真实大仓被误杀）。
     默认全局：1_000_000（100 万）。
     例：big_notional_exemption=2_000_000 改成 200 万才放行。

  5) min_qty_hand / min_qty
     语义：单合约目标持仓手数门槛（低于视为探单，跳过）。None=不做手数门槛。
     例：min_qty_hand=2 → 1 手单子视为探单被拦，2 手及以上过。

  6) min_notional / min_amount
     语义：单合约目标持仓名义成交额门槛（元，低于视为探单，跳过）。None=不做金额门槛。
     估算公式 = qty × LatestPrice_or_PreClosePrice × VolumeMultiple（取 main_contracts_by_product.json 的乘数，缺省回退 1.0）。
     例：min_notional=50000 → 名义金额小于 5 万的单子视为探单被拦。

  7) random_delay_enabled / random_delay / enable_random_delay
     语义：下单前是否加 0~N 毫秒的均匀随机延迟（摧毁时序指纹，避免从多账号报单时间差反推出谁在跟单）。
     默认全局：False（先不开，观察 1~2 天过滤没问题后再打开）。

  8) random_delay_max_ms / random_delay_ms / max_delay_ms
     语义：随机延迟上限（毫秒），random_delay_enabled=True 时生效。
     默认全局：3000（0~3 秒均匀随机）。

  9) passive / passive_mode / enable_passive
     语义：是否启用【被动挂单模式】（专为套利跟单账户设计，不主动吃单，用排队价挂单赚滑点）。
     默认：0 / False（不启用，保持原 aggressive 主动吃单 + 30 秒撤单重挂逻辑）。
     启用后行为：
       · 定价：开仓、普通调仓平仓 全部走被动排队价
           买开=买一 BidPrice1 排队；卖开=卖一 AskPrice1 排队；
           多平=卖一 AskPrice1 排队；空平=买一 BidPrice1 排队。
       · 撤单重挂：挂单后 passive_wait_seconds 秒内（默认 5 分钟）即使盘口价格偏离也**不撤单重挂**，
           给足时间让排队价自然成交；
           超过仍未成交 → 撤单 → 以「向对手盘方向进 1 tick」的新价重新排队挂，
           每 passive_wait_seconds 秒一轮，逐档咬盘口直到成交。
     不受影响的独立策略：
       · exclude 品种退出平仓（is_exclude_exit）—— 永远用 aggressive 主动吃单（时间优先）；
       · ratio==0 清仓模式（is_liquidate_mode）—— 永远用 passive 排队价（不急成交多赚滑点）。

  10) passive_wait_seconds / passive_wait_sec / passive_timeout
     语义：被动挂单模式下的「排队等待窗口」秒数。挂单后在此窗口内不撤单，让排队价自然成交；
          超时仍未成交则逐档向对手盘进 1 tick 重挂。
     默认：300（5 分钟），合法值 >= 10（小于 10 则兜底回默认 300 避免过短）。
  10.1) passiveQueueMax / passive_queue_max 与 passiveTimeMax / passive_time_max
     语义：passive【弱队改吃单】阈值（仅 passive=1 时生效）。passive 挂单在**提交时**和
          **超时重挂时**，先预判成交前景，命中任一条件即放弃排队、**改用对价吃掉单**：
            · passiveQueueMax：该价位盘口排队量（手）≥ 此值 → 改吃单（默认 1000）；
               买开/买平看 BidVolume1；卖开/卖平看 AskVolume1。
            · passiveTimeMax ：预估成交时间 = 排队量 ÷ 成交速率（手/秒）> 此值（秒）→ 改吃单（默认 30）。
               成交速率取自行情累计成交量 Volume 的最近 60 秒滚动窗口差分；拿不到速率时只应用排队量硬阈值。
          任一值 <= 0 表示关闭对应规则（如只想要 30 秒规则、不要 1000 手规则，就设 passiveQueueMax=0）。
     默认：QueueMax=1000 手、TimeMax=30 秒（全局默认见 run_pipeline.py 的 PASSIVE_QUEUE_MAX / PASSIVE_TIME_MAX）。
     说明：
       · 命中时日志打印 [passive降级吃单][开/平/超时重挂] ... → 放弃排队，改用对价；
       · 该策略**不影响** ratio==0 清仓模式的排队平仓，也不影响 exclude 退出平仓（各按原策略走）；
       · 本阈值是**启动期**参数，修改后需重启 pipeline 才生效。

  11) allowOption / allow_option
     语义：【期权跟单开关】。源账户（如 yq1015）的期权持仓默认**不跟单**。
     默认：0 / False（屏蔽期权）——目标侧 hold-std 里的期权不开仓，实际侧的期权持仓也不参与对齐
          （既不跟开、也不会被当成"超额"触发平仓），完全交给用户手工管理。
     写法：allowOption=1 / "1" / "true" / "yes" / "on" 均视为开启；0 / "false" / "no" / "off" 视为关闭。
     识别方式：按合约代码形态判断（大小写不敏感）：
       · DCE/GFEX/CFFEX 带短横：m2601-C-3000 / jd2702-C-3500 / lh2705-P-10400 / IO2603-C-4000
       · CZCE/SHFE/INE 无短横：SR401C6000 / TA601C5000 / cu2601C68000 / rb2601P3800
     例：某个跟单账户想连期权一起跟，加 "allowOption": 1 即可（不加=不跟期权）。
  12) followNewOnly / follow_new_only
     语义：【只跟新增持仓】。启用后，只跟源账户「相对基准快照的增量」，源账户的历史老仓**不跟**。
     默认：0 / False（照抄源账户全部持仓，含老仓）。
     写法：followNewOnly=1 / "1" / "true" / "yes" / "on" 均视为开启；0 / "false" / "no" / "off" 视为关闭。
     工作方式：
       · 基准快照：首次启用自动把源账户当时的持仓记为基准，写入 hold-baseline-{源账户}.json，
                   之后每轮都是「源当前持仓 − 基准快照」，只把超出的增量当目标；
       · 基准粒度：默认**按源账户共享**（一个源账户一个基准文件，其下所有目标账户共用同一基准）；
                   如需每个目标账户各自独立一份基准，见下方第 13 条 followNewOnlyPerTarget；
       · 目标侧老仓：目标最终持仓 = max(0, 源当前 − 源基准) × ratio。
                     启用那一刻，目标账户已镜像的老仓会被判为"超额"，按限价平掉，
                     最终只持有源基准之后的新增部分；
       · 重置：删除 hold-baseline-{源账户}.json 即可（下次同步会重新快照当前源持仓为新基准）。
     适用场景：接手一个已有多头老仓的源账户，只想跟它**以后新开的仓**，不想把历史老仓也复制过来。
  13) followNewOnlyPerTarget / follow_new_only_per_target
     语义：【只跟新增的基准粒度】。仅在 followNewOnly=1 时有意义。
     默认：0 / False —— 基准**按源账户共享**（hold-baseline-{源账户}.json，
                       同一源账户下所有目标账户共用同一份基准快照，兼容旧配置）。
     写 true 时 —— 基准**按目标账户各自独立**（hold-baseline-{源账户}__{目标账户}.json），
                   每个目标账户从「自己首次生效那一刻」起算基准，互不影响。
                   适用于「同一源账户先后接入多个目标账户」的场景：新接入的账户
                   不会被先接入账户更早的基准快照"带着跟老仓"，只跟它接入之后的新增。
     写法：followNewOnlyPerTarget=1 / "1" / "true" / "yes" / "on" 均视为开启；0 / "false" / "no" / "off" 视为关闭。
     注意：
       · 本开关是**启动期**参数（与 ratio 等可热加载字段不同），修改后需重启 pipeline 才生效；
       · 切换粒度后，旧的 hold-baseline-{源账户}.json 不会被删除，也不会迁移到新文件名；
         若希望新粒度下重新起算，删除对应 hold-baseline-{源账户}__{目标账户}.json 即可。
     例：同一源账户下两个目标账户，各自独立基准：
         {"user_id": "A", "password": "...", "ration": 2, "followNewOnly": 1, "followNewOnlyPerTarget": 1},
         {"user_id": "B", "password": "...", "ration": 2, "followNewOnly": 1, "followNewOnlyPerTarget": 1},

  ------------------------------------------------------------------
  【套利对子跟单】（自动识别，无需在本文件配置）
     源账户如果用郑商所套利指令（如 SPD MA701&MA702），柜台会落成两条分腿持仓
     （买 MA701 + 卖 MA702）。系统会自动从源账户的「所有委托」CSV 中识别 SPD 对子，
     写入 arb-pairs.json；凡「对子两腿同时存在且方向相反」的合约，自动**豁免
     「非主流通合约」过滤**（避免 MA702 这类次次主力腿被剔除，导致对冲被打破），
     再按两腿分别下普通委托（买 MA701 / 卖 MA702）。
     说明：识别是自动的，无需在此配置；如需手工维护，直接编辑 order-check/arb-pairs.json。

  ------------------------------------------------------------------
  【反探单 4 层过滤执行顺序】（仅 ration != 0 时生效）：
     第 1 层 deny 品种黑名单 → 第 2 层 allow_level 主流通合约校验（非主流通 + 未达 100 万豁免 = 拦）
     → 第 3 层 min_qty_hand 手数门槛 → 第 4 层 min_notional 成交额门槛
  命中任何一层 = 目标持仓里该合约被去掉，并在日志中打印 [deny]/[非主流通]/[min_qty]/[min_notional] 前缀日志。

示例：
    ACCOUNT_TARGETS = {
        "wangk0402": [
            {"env_name": "simu", "user_id": "17882", "password": "123456", "ration": 2},
            # 清仓示例：某时刻临时把 17882 全平掉（ratio=0，会跳过反探单过滤，把所有持仓平掉）
            # {"env_name": "simu", "user_id": "17882", "password": "123456", "ratio": 0},
        ],
        "zhouzhou": [
            # 对冲示例：zhouzhou 原始账户多 1 手 FG，目标账户就空 1 手 FG
            {"env_name": "simu", "user_id": "17883", "password": "123456", "ratio": -1},
        ],
        "WQ1017": [
            # 生产示例：yuqj0821 —— exclude + 反探单 7 字段完整写法
            # 说明：
            #   · exclude=["SC","FG"]：手工持有 SC/FG，系统既不开也不平（双端跳过）；
            #   · allow_contract_level=["main","main2"]：只跟主力+次主力（防冷门月探单）；
            #   · deny=["WR","FB"]：额外把 WR/FB 加到黑名单（叠加全局 GLOBAL_DENY_PRODUCTS）；
            #   · min_qty_hand=1：单合约至少 1 手才跟（默认 1，一般不用改）；
            #   · min_notional=30000：单合约名义金额至少 3 万才跟（防 1 手几千块的品种探单）；
            #   · random_delay_enabled=False：先观察过滤效果，暂时不打乱报单时序；
            #   · random_delay_max_ms=2000：将来打开随机延迟时上限 2 秒。
            {"env_name": "online", "user_id": "yuqj0821", "password": "yqj123456",
             "ratio": 1,
             "exclude": ["SC", "FG"],
             "allow_contract_level": ["main", "main2"],
             "deny": ["WR", "FB"],
             "min_qty_hand": 1,
             "min_notional": 30000,
             "random_delay_enabled": False,
             "random_delay_max_ms": 2000},
            #
            # ===== 套利跟单账户 passive 模式示例（专为套利账户设计） =====
            #   · passive=1：启用被动挂单模式，开仓/普通调仓平仓都用排队价，
            #                挂单后 5 分钟（300 秒）内即使盘口偏离也不撤；
            #                超过 5 分钟仍未成交 → 撤单 → 向对手盘方向进 1 tick 重挂，
            #                每 5 分钟一轮，逐档咬盘口直到成交；
            #   · passive_wait_seconds=300：5 分钟等待窗口（默认 300，可省略）。
            # {"env_name": "online", "user_id": "ta001", "password": "xxx",
            #  "ratio": 1, "passive": 1, "passive_wait_seconds": 300},
            #
            # ===== 只跟新增示例（只跟源账户以后新开的仓，历史老仓不跟） =====
            #   · followNewOnly=1：首次启用自动快照源账户当时持仓为基准，之后只跟增量；
            #                      目标账户已镜像的老仓会被判为超额平掉。
            # {"env_name": "online", "user_id": "ta002", "password": "xxx",
            #  "ratio": 1, "followNewOnly": 1},
        ],
        # 最小化写法示例：只写 exclude + ratio，其余字段全走 run_pipeline.py 顶部全局默认
        "minimal_demo": [
            {"env_name": "online", "user_id": "demo001", "password": "demo123456",
             "ratio": 1, "exclude": ["SC"]},
        ],
    }

注意：
  同一个源账户映射到多个目标账户时，请写在一个列表里，例如：
    "wangk0402": [
        {"env_name": "online", "user_id": "sxk0812", "password": "..."},
        {"env_name": "online", "user_id": "yq02", "password": "..."},
    ]
  不要写成重复的 dict key，否则后面的会覆盖前面的。

  deny 与 exclude 语义边界快速判断：
    · "这个品种我**永远不想跟单开新仓**，但老仓该平就平" → 写进 deny（或 GLOBAL_DENY_PRODUCTS）。
    · "这个品种我**要自己手工管（既不跟开也不跟平）**" → 必须写进 exclude。
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
