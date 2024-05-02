import datetime
from edgar import *
import pandas as pd
from neon_connector.neon_connector import NeonConnector
from dotenv import load_dotenv
import os 
import json

load_dotenv()
connection_string = os.getenv('DATABASE_URL')
nc = NeonConnector(connection_string)

set_identity('Ger Sen wilsenp@gmail.com')

# today = datetime.date.today()
# current_year = today.year
# df = get_filings(form="13F-HR", year=range(current_year - 1, current_year)).to_pandas()
# df = df[['cik', 'company']]
# df = df.drop_duplicates()
# df.rename(columns={'company': 'institution'}, inplace=True)

# def get_institution_holding_data(df_chunk):
#     results = []
#     for index, row in df_chunk.iterrows():
#         print(row['cik'], row['institution'])
#         data = find(row['cik']).get_filings(form="13F-HR")[0].obj()
#         accesion_number = data.filing.accession_no
#         report_period = data.primary_form_information.report_period
#         filing_date = data.filing.filing_date
#         total_value = data.primary_form_information.summary_page.total_value
#         total_holding = data.primary_form_information.summary_page.total_holdings
#         results.append([row['cik'], row['institution'], accesion_number, report_period, filing_date, total_value, total_holding])
#     return pd.DataFrame(results, columns=['cik', 'institution', 'last_accesion_number', 'last_report_period', 'last_filing_date', 'total_value', 'total_holding'])

# batch_size = 100
# chunks = [df[i:i + batch_size] for i in range(0, len(df), batch_size)]
# processed_chunks = []
# for chunk in chunks:
#     processed_chunk = get_institution_holding_data(chunk)
#     processed_chunks.append(processed_chunk)

# new_columns = pd.concat(processed_chunks, ignore_index=True)

# final_df = pd.merge(df, new_columns, on=['cik', 'institution'])
# final_df[['last_report_period', 'last_filing_date']] = final_df[['last_report_period', 'last_filing_date']].apply(pd.to_datetime)
# final_df = final_df[(final_df['last_report_period'] >= pd.Timestamp(datetime.datetime(current_year - 1, 1, 1))) & (final_df['last_report_period'] <= pd.Timestamp(datetime.datetime(current_year, 12, 31)))]
# final_df['updated_on'] = pd.Timestamp.now()
# final_df.to_csv('institutional_profile.csv', index=False)

