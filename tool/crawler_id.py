import requests
import pandas as pd

strMode = 2 # 2:上市, 4:上櫃
for strMode in ["2","4"]:
    res = requests.get("http://isin.twse.com.tw/isin/C_public.jsp?strMode=%s"%(strMode))
    df = pd.read_html(res.text)[0]
    df.columns = df.iloc[0]
    df = df.iloc[1:]
    df = df.dropna(thresh=3, axis=0).dropna(thresh=3, axis=1)
    df[['代號', '名稱']] = df["有價證券代號及名稱"].str.split('　',expand=True)
    mask = (df["代號"].str.len() == 4)
    df = df.loc[mask]
    df.drop(columns=["有價證券代號及名稱","CFICode","國際證券辨識號碼(ISIN Code)","備註"],inplace=True)
    df.columns=["Date","Market","Industry","Code","Name"]
    print(df)