
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Date
from sqlalchemy.dialects.mysql import VARCHAR


Base = declarative_base()

class MR_PredictInfo(Base):
    __tablename__ = 'tb_merchant_risk_info'

    pk_id = Column(Integer, primary_key=True)
    Code = Column(VARCHAR(100),default='undefine')
    Company = Column(VARCHAR(100))
    ListingDate = Column(Date)
    MarketType = Column(VARCHAR(100))
    IndustryType = Column(VARCHAR(100))