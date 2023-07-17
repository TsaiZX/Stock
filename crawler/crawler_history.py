import pandas as pd
import requests
import time
from sqlalchemy import create_engine, text, inspect, MetaData, Table
from sqlalchemy import Column, Date, Integer, Float, String

class StockCrawler:
    def __init__(self, 
                dbUser="root", 
                dbPassword=None, 
                dbHost="127.0.0.1:3306"):
        if dbPassword:
            self.dbEngine = create_engine(f"mysql+pymysql://{dbUser}:{dbPassword}@{dbHost}/Stock?charset=utf8")
        else:
            self.dbEngine = create_engine(f"mysql+pymysql://{dbUser}@{dbHost}/Stock?charset=utf8")

    def get_code_list(self):
        # get the stock code list from db
        return pd.read_sql(text("SELECT Code FROM Stock.tb_taiwan_stock;"), self.dbEngine.connect())["Code"].tolist()

    def check_code_schema(self, code, schema="Stock_History"):
        # check the company table exist, if not create the table
        if not inspect(self.dbEngine).has_table(f"tb_{code}", schema=schema):
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
            metadata.create_all(self.dbEngine)

    def fetch_price_data(self, stock_code, date):
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
    
    def clean_price_data(self, data):
        # clean data type
        # change date time format
        for i in range(len(data["Date"])):
            data["Date"].iloc[i]=data["Date"].iloc[i].replace(data["Date"].iloc[i][0:3], str(int(data["Date"].iloc[i][0:3]) + 1911))
        for col in ["Capacity", "Turnover","Transcation"]:
            data[col] = data[col].str.replace(",", "").astype(int)
        for col in ["Open", "High", "Low", "Close"]:
            data[col] = data[col].astype(float)
        return data

    def insert_toDB(self, data, dtype, schema):
        # insert data to database
        data.to_sql(name=f"tb_{code}", con=self.dbEngine, schema=schema, if_exists="append", index=False,dtype=dtype)
    
    def price_data(self, code, date):
        # Processing daily stock price data.
        df = self.fetch_price_data(code, date)
        df = self.clean_price_data(df)
        dtype = {
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
                }
        schema = "Stock_History"
        self.insert_toDB(df, dtype, schema)


if __name__ == "__main__":
    sc = StockCrawler()
    codeList = sc.get_code_list()

    for code in codeList:
        print("Processing Code :" + code)
        currentDate = time.strftime("%Y%m%d")
        sc.price_data(code, currentDate)
        break