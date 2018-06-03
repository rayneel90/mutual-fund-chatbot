import requests
from bs4 import BeautifulSoup as BS
import pickle
import pandas as pd
url_tmplt = 'https://www.valueresearchonline.com/funds/newsnapsh' \
           'ot.asp?schemecode={0}'
lst=[]
for i in range(101,40000):
    try:
        req = requests.get(url_tmplt.format(i))
        txt = BS(req.text, 'lxml')
        fnm = txt.find('h1').text.strip().split('\n')[0]
        RD = txt.find('a', {'class': 'active'}).text
        df = pd.read_html(str(txt.find('table',
                                       {'class':'fund-snapshot-basic-details'})
                              ))[0]
        df[0] = df[0].str.replace(' ','_')
        dat = dict(df.to_dict(orient='split')['data'])
        dat['Scheme_Code'] = fnm; dat['RD'] = RD
        dat['SL']=i
        lst.append(dat)
        print('pass',i)
        with open('data/valueresearch_temp_dump.pkl', 'wb')as fil:
            pickle.dump(lst, fil)
    except:
        print('fail',i)
