import requests
import pandas as pd
from sqlalchemy import create_engine


engine = create_engine('mysql+pymysql://root@127.0.0.1:3306/Stock?charset=utf8')


TWSEequities = requests.get("http://isin.twse.com.tw/isin/C_public.jsp?strMode=2") # Listed Stock
TPExequities = requests.get("http://isin.twse.com.tw/isin/C_public.jsp?strMode=4") # OTC Market

for market in [TWSEequities, TPExequities]:
    df = pd.read_html(market.text)[0]
    df.columns = df.iloc[0]
    df = df.iloc[1:]
    df = df.dropna(thresh=3, axis=0)
    df[['code', 'company']] = df['有價證券代號及名稱'].str.split('　', 1, expand=True)
    df = df[df["CFICode"] == "ESVUFR"]
    df = df[["code", "company", "上市日", "市場別", "產業別"]]
    df.columns = ["Code", "Company", " ListingDate", "MarketType", "IndustryType"]
    df.to_sql(name='tb_taiwan_stock', con=engine, schema='Stock', if_exists='append', index=False)
    