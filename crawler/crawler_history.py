import pandas as pd
import requests
import time
from sqlalchemy import create_engine, text, inspect, MetaData, Table
from sqlalchemy import Column, Date, Integer, Float, String

def get_codeList(db_engine):
    # get the stock code list from db
    return pd.read_sql(text("SELECT Code FROM Stock.tb_taiwan_stock;"), db_engine.connect())["Code"].tolist()

def check_code_schema(db_engine, code, schema="Stock_History"):
    # check the company table exist, if not create the table
    if not inspect(db_engine).has_table(f"tb_{code}", schema=schema):
        metadata = MetaData()
        Table(f"tb_history_{code}", metadata,
            Column("pk_id", Integer, primary_key=True, nullable=False, autoincrement=True), 
            Column("Date", Date),
            Column("Capacity", Integer), 
            Column("Turnover", Integer), 
            Column("Open", Float), 
            Column("High", Float),
            Column("Low", Float),
            Column("Close", Float),
            Column("Change", String(10)),
            Column("Transcation", Integer),
        )
        metadata.create_all(db_engine)

def fetch_stock_data(stock_code, date):
    """
    Date: 日期
    Capacity: 成交股數
    Turnover: 成交金額
    Open: 開盤價
    High: 最高價
    Low: 最低價
    Close: 收盤價
    Change: 帳跌價差
    Transcation: 成交筆數
    """
    column = ["Date", "Capacity", "Turnover", "Open", "High", "Low", "Close", "Change", "Transcation"]
    url = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date={date}&stockNo={stock_code}"

    response = requests.get(url)
    data = pd.DataFrame(response.json()["data"])
    data.columns = column
    return data
    
def preprocess_data(data):
    # clean data type
    # change date time format
    for i in range(len(data["Date"])):
        data["Date"].iloc[i]=data["Date"].iloc[i].replace(data["Date"].iloc[i][0:3], str(int(data["Date"].iloc[i][0:3]) + 1911))
    for col in ["Capacity", "Turnover","Transcation"]:
        data[col] = data[col].str.replace(",", "").astype(int)
    for col in ["Open", "High", "Low", "Close"]:
        data[col] = data[col].astype(float)
    return data

def insert_toDB(data):
    print(data)
    data.to_sql(name=f"tb_{code}", con=engine, schema="Stock_History", if_exists="append", index=False,
                dtype={
                    "pk_id": Integer,    
                    "Date": Date,
                    "Capacity": Integer,
                    "Turnover": Integer,
                    "Open": Float,
                    "High": Float,
                    "Low": Float,
                    "Close": Float,
                    "Change": String(10),
                    "Transcation": Integer,
                    })
    
if __name__ == "__main__":
    engine = create_engine("mysql+pymysql://root@127.0.0.1:3306/Stock?charset=utf8")
    codeList = get_codeList(engine)

    for code in codeList:
        print("Processing Code :" + code)
        check_code_schema(engine, code)
        current_date = time.strftime("%Y%m%d")
        df = fetch_stock_data(code, current_date)
        df = preprocess_data(df)
        insert_toDB(df)
        print(df)
        break
        # break
        # # 將數據保存為CSV文件
        # df.to_csv(f"{stock_code}_history.csv", index=False)