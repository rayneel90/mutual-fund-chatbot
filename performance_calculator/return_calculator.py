import pandas as pd
from sqlalchemy import create_engine
engine = create_engine("mysql://neel:pass@123@localhost/mutual_fund?c"
                       "harset=utf8mb4")

df = pd.read_sql('SELECT * from mutual')