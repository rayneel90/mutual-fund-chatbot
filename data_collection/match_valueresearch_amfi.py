import pandas as pd
from sqlalchemy import create_engine
import re
engine = create_engine("mysql://neel:pass@123@localhost/mutual_fund?c"
                       "harset=utf8mb4")
vr_dump = pd.read_sql('SELECT Scheme_Code, `Benchmark:` from valuresearch_dump',
                  engine).to_dict(orient='record')
amfi = pd.read_sql('SELECT distinct Scheme_Code, Scheme_Name from amfi_dump',
                   engine)

ref = dict(amfi.to_dict(orient='split')['data'])
ref = {key: re.sub('[^A-Za-z0-9]',' ',val).lower().split() for key,val in ref.items()}

while vr_dump:
    re
