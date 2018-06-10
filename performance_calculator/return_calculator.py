import pandas as pd
from sqlalchemy import create_engine
from dateutil.relativedelta import relativedelta
engine = create_engine("mysql://neel:pass@123@localhost/mutual_fund?c"
                       "harset=utf8mb4")

master = pd.read_sql('SELECT * from fund_master', engine)

def printProgressBar (iteration, total, prefix = 'Neel', suffix = '',
                      decimals = 1, length = 100, fill = '█'):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end = '\r')
    # Print New Line on Complete
    if iteration == total:
        print()


def get_risk_return(sc, days:int=0, months:int=0, years: int=0):
    if not days+months+years:
        raise ValueError('Must provide positive integer value for at least ')
    dt_lim = pd.datetime.today().date()-relativedelta(days=days,
                                                        months=months,
                                                        years=years)
    dat = pd.read_sql("SELECT Net_Asset_Value, Date FROM daily_nav WHERE "
                      "Scheme_Code='{0}' and Date > '{1}'".format(sc, dt_lim),
                      engine)
    if dat.shape[0]<3:
        return None, None
    dat.Net_Asset_Value = dat.Net_Asset_Value.astype(float)
    period = (days/360) + (months/12) +years
    nav = dat.sort_values('Date').Net_Asset_Value.tolist()
    if period < 1:
        perc_ret = (nav[-1]-nav[0])/(nav[0]*period)*100
    if period > 1:
        perc_ret = ((nav[-1]/nav[0])**(1/period)-1)*100
    dat['daily_ret'] = dat['Net_Asset_Value'].diff()
    cv = dat.daily_ret.diff().std()/dat.Net_Asset_Value.mean()*100
    return perc_ret,cv


opn_schemes = master[master.Open_Close=='Open Ended Schemes '].to_dict(orient='record')
month1 = []
i=0
for rec in opn_schemes:
    ret, risk = get_risk_return(rec['Scheme_Code'],months=1)
    month1.append({
        'Scheme_Code':rec['Scheme_Code'] ,
        'Scheme_Name': rec['Scheme_Name'],
        'Type': rec['Type'],
        'SubType': rec['SubType'],
        'Return': ret,
        'Risk': risk,
    })
    printProgressBar(i,len(opn_schemes))
    i = i + 1
pd.DataFrame(month1).to_sql('risk_ret_1_month', engine, index=False,
                            if_exists='replace')