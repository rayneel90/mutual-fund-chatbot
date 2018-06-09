import pandas as pd
from sqlalchemy import create_engine
from dateutil.relativedelta import relativedelta

engine = create_engine("mysql://neel:pass@123@localhost/mutual_fund?c"
                       "harset=utf8mb4")

master = pd.read_sql('SELECT * from fund_master',engine)


def get_risk_return(days:int=0, months:int=0, years:int=0):
    if not days+months+years:
        raise ValueError('Must provide positive integer value for at least ')
    dt_limit = pd.datetime.today().date()-relativedelta(days=days,
                                                 months=months,
                                                     years=years)
    df = pd.read_sql("SELECT Net_Asset_Value, Scheme_Code, Date FROM amfi_dump WHERE Date>'{0}'".format(dt_limit),engine)
    for sc in df.Scheme_Code.unique():
        subset = df[df.Scheme_Code == sc]
for i in master.Scheme_Code:
    daily_nav = pd.read_sql('SELECT Net_Asset_Value, Date FROM amfi_dump '
                            'WHERE Scheme_Code=100046'.format(i), engine)
    daily_nav = daily_nav.sort_values('Date')
    daily_nav.to_sql('daily_nav',engine,if_exists='append')
    today = daily_nav.Date.max()
    nav_last_month = daily_nav[daily_nav.Date>today-relativedelta(months=1)]
    nav_2_mnth =
