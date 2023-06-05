import pandas as pd
import requests

def fetch_stock_data(stock_code):
    column = ['Date', 'Capacity', 'Turnover', 'Open', 'High', 'Low', 'Close', 'Change', 'Transcation']
    url = f'https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date=&stockNo={stock_code}'
    response = requests.get(url)
    data = pd.DataFrame(response.json()["data"])
    data.columns = column
    return data
    

# 輸入你要爬取的股票代碼
stock_code = '2330'
df = fetch_stock_data(stock_code)

# print(df)
# # 將數據保存為CSV文件
# df.to_csv(f'{stock_code}_history.csv', index=False)