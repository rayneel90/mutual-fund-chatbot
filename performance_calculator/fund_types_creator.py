import pandas as pd
from sqlalchemy import create_engine
engine = create_engine("mysql://neel:pass@123@localhost/mutual_fund?c"
                       "harset=utf8mb4")

df = pd.read_sql('Select distinct Scheme_Code, Scheme_Name, Fund_Type, AMC '
                 'from amfi_dump', engine)
types = df.Fund_Type.str.replace(
    '(','-').str.replace(')','').str.split('-',expand=True)
types.columns = ['Open_Close','Type','SubType']
df = pd.concat([df,types],axis=1)  # type:pd.DataFrame

for i in range(30):
    df.iloc[i*1000:(i+1)*1000].to_sql('fund_master',engine,if_exists='append')

