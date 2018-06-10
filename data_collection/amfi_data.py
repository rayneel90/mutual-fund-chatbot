import requests
import pandas as pd
from sqlalchemy import create_engine
from dateutil.relativedelta import relativedelta
engine = create_engine("mysql://neel:pass@123@localhost/mutual_fund")


def collect_historical_data(start='04-01-2006', end=pd.datetime.today()):
    dt_range = pd.date_range(start=start,
                             end=end - relativedelta(days=1)).tolist()
    while dt_range:
        dt = dt_range.pop(0).strftime('%d-%b-%Y')
        url_tmplt = 'http://portal.amfiindia.com/DownloadNAVHistoryReport_Po.' \
                    'aspx?frmdt={0}&todt={1}'
        txt = requests.get(url_tmplt.format(dt, dt)).text
        if "DOCTYPE html" in txt:
            return None
        dat = [i.strip() for i in txt.split('\n')]
        dat = [i for i in dat if i]
        header= [pos for pos,val in enumerate(dat) if ';' not in val]
        types = [i for i in header if i+1 in header]
        types.append(len(dat)+1)
        fund_names = {types[i]: [j for j in header if types[i] < j < types[i+1]]
                      for i in range(len(types)-1)}
        df_list=[]
        for key, val in fund_names.items():
            while val:
                fnm = val.pop(0)
                lst = []
                for row in dat[fnm+1:]:
                    if ';' not in row:
                        break
                    lst.append([i.strip() for i in row.split(';')])
                df = pd.DataFrame(lst)
                df.columns = [i.strip().replace(' ','_') for i in dat[0].split(';')]
                df['Fund_Type'] = dat[key]
                df['AMC'] = dat[fnm]
                df.Date = pd.to_datetime(df.Date)
                df_list.append(df)
        df = pd.concat(df_list)
        df.to_sql('amfi_dump', engine, if_exists='append', index=False)
        print(dt)


try:
    lst_dt = [i for i in engine.execute(
        "SELECT distinct Date FROM amfi_dump order by Date DESC limit 1;")][0][0]
    collect_historical_data(start=lst_dt+relativedelta(days=1))
except:
    collect_historical_data()


