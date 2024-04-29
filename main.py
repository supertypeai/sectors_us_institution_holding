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

# INSTITUTION FILING

# institution_profle_data = nc.select_query("SELECT cik FROM institution_profile")
# all_cik = [entry['cik'] for entry in institution_profle_data]
# print(all_cik)

# consecutive_failures = 0
# for cik in all_cik[6500:7500]:
#     filing_data = []
#     for i in range(len(find(cik).get_filings(form="13F-HR"))):
#         try:
#             data = find(cik).get_filings(form="13F-HR")[i].obj()
#             filing_data.append({"cik": cik,
#                                 "accession_number" :data.filing.accession_no,
#                                 "report_period" :data.primary_form_information.report_period,
#                                 "filing_date": data.filing.filing_date,
#                                 "total_value": int(data.primary_form_information.summary_page.total_value),
#                                 "total_holding": int(data.primary_form_information.summary_page.total_holdings)
#                                 })
#             print(f"success {cik} {i}")
#             consecutive_failures = 0
#         except:
#             print(f"fail {cik} {i}")
#             consecutive_failures += 1
#             if consecutive_failures > 2:
#                 print(f"Skipping CIK: {cik}")
#                 break 

#     nc.batch_upsert(target_table="form_13f_filing", records=filing_data, conflict_columns=['accession_number'])
#     print(f"====== {cik} is inserted ======")


# INSTITUTION HOLDING

sql_query = """
WITH ranked_filings AS (
    SELECT 
        filing_id, 
        cik, 
        filing_date, 
        accession_number,
        ROW_NUMBER() OVER (PARTITION BY cik ORDER BY filing_date DESC) AS rank_num
    FROM 
        form_13f_filing
)
SELECT 
    filing_id, 
    cik, 
    filing_date, 
    accession_number
FROM 
    ranked_filings
WHERE 
    rank_num <= 2
"""

latest_filings_list = nc.select_query(sql_query)
print(latest_filings_list)

for filing in latest_filings_list[10:]:
    df = find(filing['accession_number']).obj().infotable.sort_values('Value', ascending = False)[['Ticker','Value','SharesPrnAmount']]
    df['filing_id'] = filing['filing_id']
    df[['Value','SharesPrnAmount']] = df[['Value','SharesPrnAmount']].astype(int)
    df = df.groupby(['Ticker']).sum().reset_index().sort_values('Value', ascending = False)
    total_shares = df['Value'].sum()
    df['percentage'] = df['Value'] / total_shares * 100
    df = df.rename(columns={'Ticker': 'symbol', 'Value': 'value','SharesPrnAmount':'share'})
    df['filing_date'] = filing['filing_date'] 
    json_string = df.to_json(orient='records')
    recs = json.loads(json_string)
    try:
        nc.batch_upsert(target_table="form_13f_holdings", records=recs, conflict_columns=['filing_id','symbol'])
    except:
        df['filing_date'] = df['filing_date'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
        json_string = df.to_json(orient='records')
        recs = json.loads(json_string)
        nc.batch_upsert(target_table="form_13f_holdings", records=recs, conflict_columns=['filing_id','symbol'])
    print(recs)

