# 本地坐标配置文件
# 此文件包含平台相关的鼠标坐标，不同机器需要单独配置
# 建议将此文件加入 .gitignore

# 账号名称（用于匹配导出文件名前缀）
# ACCOUNT = "fy0228"
# ACCOUNT = "WQ1017"
ACCOUNT = "wangk0402"
# ACCOUNT = "jm0310"
# 目标应用程序窗口标题
APP_TITLE = "用户: jm0312"

# 菜单坐标
MENU_POSITION = (18, 40)  # 主菜单按钮
EXPORT_MENU_POSITION = (33, 57) # "导出全部数据"菜单项

# 浏览文件夹对话框中的文件夹点击序列
# 按顺序依次点击，换电脑时修改这里即可
FOLDER_CLICK_SEQUENCE = [
    ("我的电脑", (482, 451)),
    ("C盘", (535, 607)),
    ("ronghang",(566, 627)),
    ("data",  (570, 647)),
]

# 按钮坐标
SAVE_BUTTON_POSITION = (677, 692) # 保存对话框"确定"
OK_BUTTON_POSITION =(649, 564)   # 导出完成确认弹框"确定"

# 其他常量
EXPORT_DIALOG_TITLE = "导出数据"
SAVE_DIALOG_TITLE = "浏览文件夹"
DEFAULT_SAVE_PATH = r"C:\ronghang\data"
