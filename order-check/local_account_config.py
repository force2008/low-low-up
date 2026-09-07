# 该配置是给操作账号监控用的
# 本地坐标配置文件
# 此文件包含平台相关的鼠标坐标，不同机器需要单独配置
# 建议将此文件加入 .gitignore

# 账号名称（用于匹配导出文件名前缀）
# ACCOUNT = "fy0228"
# ACCOUNT = "WQ1017"
ACCOUNT = "wangk0402"
# ACCOUNT = "jm0310"
#ACCOUNT = "zhouzhou"
# 目标应用程序窗口标题
APP_TITLE = "用户: fangye002"

# 菜单坐标
MENU_POSITION = (36, 40)  # 主菜单按钮
EXPORT_MENU_POSITION = (60, 59) # "导出全部数据"菜单项

# 浏览文件夹对话框中的文件夹点击序列
# 按顺序依次点击，换电脑时修改这里即可
FOLDER_CLICK_SEQUENCE = [
    ("我的电脑", (468, 435)),
    ("C盘", (483, 592)),
    ("ronghang",(500, 608)),
    ("data",  (544, 608)),
]

# 按钮坐标
SAVE_BUTTON_POSITION = (701, 680) # 保存对话框"确定"
OK_BUTTON_POSITION =(643, 556)   # 导出完成确认弹框"确定"

# 其他常量
EXPORT_DIALOG_TITLE = "导出数据"
SAVE_DIALOG_TITLE = "浏览文件夹"
DEFAULT_SAVE_PATH = r"C:\ronghang\data"
