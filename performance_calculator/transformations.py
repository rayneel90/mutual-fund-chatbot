import pandas as pd
from sqlalchemy import create_engine
from MySQLdb import OperationalError
engine = create_engine("mysql://neel:pass@123@localhost/mutual_fund?c"
                       "harset=utf8mb4")
import pickle
################################################################################
#                          Create Fund Master                                  #
################################################################################
df = pd.read_sql('Select distinct Scheme_Code, Scheme_Name, Fund_Type, AMC '
                 'from amfi_dump', engine)
types = df.Fund_Type.str.replace(
    '(', '-').str.replace(')', '').str.split('-', expand=True)
types.columns = ['Open_Close', 'Type', 'SubType']
df = pd.concat([df, types], axis=1)  # type:pd.DataFrame
try:
    engine.execute('DROP TABLE fund_master;')
except:
    pass
for i in range(30):
    df.iloc[i*1000:(i+1)*1000].to_sql('fund_master', engine, if_exists='append')


################################################################################
#                        Create Daily NAV                                      #
################################################################################
try:
    engine.execute('DROP TABLE daily_nav')
except:
    pass
lst = pd.date_range('01-01-2005', '01-01-2019',freq='y')
sql = "Select * from amfi_dump where Date between '{0}' and '{1}'"
for i in range(1, len(lst)):
    df = pd.read_sql(sql.format(lst[i-1], lst[i]), engine)
    print(df.shape)
    df = df.drop_duplicates()
    print(df.shape)
    with open('amfi_dump_{0}'.format(i),'wb') as fil:
        pickle.dump(df,fil)
    df = df[['Scheme_Code', 'Net_Asset_Value', 'Date']]
    df.to_sql('daily_nav', engine, index=False, if_exists='append',
              chunksize=1000)

################################################################################
#                                                              #
################################################################################

