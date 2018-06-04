import pandas as pd
from sqlalchemy import create_engine
import re
from jellyfish import jaro_distance as jd
engine = create_engine("mysql://neel:pass@123@localhost/mutual_fund?c"
                       "harset=utf8mb4")
vr_dump = pd.read_sql('SELECT Scheme_Code, `Benchmark:`, `Fund_House:`'
                      'from valuresearch_dump', engine)
amfi = pd.read_sql('SELECT distinct Scheme_Code, Scheme_Name, AMC from amfi_'
                   'dump', engine)
lst = []
for amc in set(amfi.AMC).intersection(vr_dump['Fund_House:']):
    amfi_temp = amfi[amfi.AMC==amc]
    vr_temp = vr_dump[vr_dump['Fund_House:']==amc]
    lst.extend([(i, j, jd(i, j)) for i in set(amfi_temp.Scheme_Name)
    for j in set(vr_temp.Scheme_Code)])
