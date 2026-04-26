# from datetime import datetime
#
# from settings import stock_data_root
# import plotly.graph_objects as go
#
# import plotly.graph_objects as go
# import pandas as pd
# import os
# df = pd.read_csv(stock_data_root / 'akshare/00700_港股00700_20040616_20251010.csv', parse_dates=['date'])
#
# # 绘制交互式K线图
# fig = go.Figure(data=[go.Candlestick(
#     x=df['date'],
#     open=df['open'], high=df['high'],
#     low=df['low'], close=df['close']
# )])
# fig.update_layout(title='交互式K线图', xaxis_title='日期', yaxis_title='价格')
# fig.show()  # 在浏览器或Notebook中显示交互式图表
#
#
# # # 转换为 HTML 字符串（包含所有依赖）
# # html_str = fig.to_html(full_html=False)  # full_html=False 只生成图表部分，不包含完整 HTML 文档
#
#
# # 2. 生成包含当前时间的文件名
# current_time = datetime.now().strftime("%Y%m%d_%H%M%S")  # 格式：年月日_时分秒
# file_name = f"plot_{current_time}.html"  # 例如：plot_20251012_153045.html
#
# # 3. 保存为HTML文件
# fig.write_html(file_name)
#
# # 4. 输出保存信息
# print(f"图表已保存至：{os.path.abspath(file_name)}")
#
#
