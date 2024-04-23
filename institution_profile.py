import datetime
from edgar import *
import pandas as pd
from neon_connector.neon_connector import NeonConnector
from dotenv import load_dotenv
import os 

load_dotenv()
connection_string = os.getenv('DATABASE_URL')
nc = NeonConnector(connection_string)

set_identity('Ger Sen wilsenp@gmail.com')

today = datetime.date.today()
current_year = today.year
df = get_filings(form="13F-HR", year=range(current_year - 1, current_year)).to_pandas()
df = df[['cik', 'company']]
df = df.drop_duplicates()
df.rename(columns={'company': 'institution'}, inplace=True)
df['updated_on'] = pd.Timestamp.now()

# df.to_csv('insti')
recs = nc.convert_df_to_records(df, int_cols=['cik'], json_cols=['institution','updated_on'])
print(len(recs), recs)
# nc.batch_upsert(target_table="institution_profile", records=recs, conflict_columns=['cik'])
# nc.batch_upsert(target_table="institution_holding", records=recs, conflict_columns=['cik'])