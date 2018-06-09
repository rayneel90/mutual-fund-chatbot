import pandas as pd
from sqlalchemy import create_engine
engine = create_engine("mysql://neel:pass@123@localhost/mutual_fund?c"
                       "harset=utf8mb4")

################################################################################
#                          Create Fund Master                                  #
################################################################################
df = pd.read_sql('Select distinct Scheme_Code, Scheme_Name, Fund_Type, AMC '
                 'from amfi_dump', engine)
types = df.Fund_Type.str.replace(
    '(','-').str.replace(')','').str.split('-',expand=True)
types.columns = ['Open_Close','Type','SubType']
df = pd.concat([df,types],axis=1)  # type:pd.DataFrame

for i in range(30):
    df.iloc[i*1000:(i+1)*1000].to_sql('fund_master',engine,if_exists='append')


################################################################################
#                        Create Daily NAV                                      #
################################################################################
try:
    lst_dt = [i for i in engine.execute(
        "SELECT distinct Date FROM daily_nav order by Date DESC limit 1;")][0][0]
    sql = "SELECT Scheme_Code, Net_Asset_Value as NAV, Date FROM amfi_dump " \
          "WHERE Scheme_Code='{0}' and"+" Date>'{0}'".format(lst_dt)
except:
    sql = "SELECT Scheme_Code,Net_Asset_Value as NAV, Date FROM amfi_dump WHERE Scheme_Code='{0}'"

for sc in df.Scheme_Code:
    break
    temp = pd.read_sql(sql.format(sc),engine)
    temp.to_sql('daily_nav',engine,if_exists='append')