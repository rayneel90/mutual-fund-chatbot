import requests
from bs4 import BeautifulSoup
import pickle
import pandas as pd
from sqlalchemy import create_engine
engine = create_engine("mysql://neel:pass@123@localhost/mutual_fund?c"
                       "harset=utf8mb4")


def collect_snapshot(a):
    url_tmplt = 'https://www.valueresearchonline.com/funds/newsnapsh' \
               'ot.asp?schemecode={0}'
    lst = []
    for i in range(a, 40000):
        try:
            req = requests.get(url_tmplt.format(i))
            txt = BeautifulSoup(req.text, 'lxml')
            fnm = txt.find('h1').text.strip().split('\n')[0]
            rd = txt.find('a', {'class': 'active'}).text
            df = pd.read_html(str(
                txt.find('table', {'class': 'fund-snapshot-basic-details'})))[0]
            df[0] = df[0].str.replace(' ', '_')
            dat = dict(df.to_dict(orient='split')['data'])
            dat.update({'Scheme_Code': fnm, 'RD': rd, 'SL': i})
            lst.append(dat)
            print('pass', i)
            with open('../data/valueresearch_temp_dump.pkl', 'wb')as fil:
                pickle.dump(lst, fil)
        except IndexError:
            print('fail', i)
        except AttributeError:
            print('fail', i)


def collect_portfolio(a):
    url_tmplt = 'https://www.valueresearchonline.com/funds/portfolio' \
                   'vr.asp?schemecode={0}'
    port_agg = []
    port_hold = []
    for i in range(a, 40000):
        try:
            req = requests.get(url_tmplt.format(i))
            txt = BeautifulSoup(req.text, 'lxml')
            fnm = txt.find('h1').text.strip().split('\n')[0]
            df = pd.read_html(str(
                txt.find('table', {'id': 'fund-snapshot-portfolio-agg'})))[0]
            df = df.set_index(0)
            df.columns = df.iloc[1]
            df = df.iloc[2:]
            dat = df.to_dict()['Fund']
            dat['Scheme_Name']=fnm
            port_agg.append(dat)
            df2 = pd.read_html(str(
                txt.find('table', {'id': 'fund-snapshot-port-holdings'})))[0]
        except IndexError:
            print('fail', i)




collect_snapshot(6607)
collect_portfolio(101)