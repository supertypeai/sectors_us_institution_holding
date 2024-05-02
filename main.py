import datetime
from edgar import *
import pandas as pd
from neon_connector.neon_connector import NeonConnector
from dotenv import load_dotenv
import os 
import json
import time

load_dotenv()
connection_string = os.getenv('DATABASE_URL')
nc = NeonConnector(connection_string)

set_identity('Ger Sen wilsenp@gmail.com')

# INSTITUTION FILING
institution_profle_data = nc.select_query("SELECT cik FROM institution_profile")
all_cik = [entry['cik'] for entry in institution_profle_data]
print(all_cik)

filing_data = []
for cik in all_cik:
    try:
        data = find(cik).get_filings(form="13F-HR")[0].obj()
        filing_data.append({"cik": cik,
                            "accession_number" :data.filing.accession_no,
                            "report_period" :data.primary_form_information.report_period,
                            "filing_date": data.filing.filing_date,
                            "total_value": float(data.primary_form_information.summary_page.total_value),
                            "total_holding": int(data.primary_form_information.summary_page.total_holdings)
                            })
        nc.batch_upsert(target_table="form_13f_filing", records=filing_data, conflict_columns=['filing_id'])
    except:
        print(f"{cik} failed to insert")
        time.sleep(5)

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
    rank_num <= 1
"""

latest_filings_list = nc.select_query(sql_query)
print(latest_filings_list)

for filing in latest_filings_list:
    df = find(filing['accession_number']).obj().infotable.sort_values('Value', ascending = False)[['Ticker','Value','SharesPrnAmount']]
    df['filing_id'] = filing['filing_id']
    df[['Value','SharesPrnAmount']] = df[['Value','SharesPrnAmount']].astype(int)
    df = df.groupby(['Ticker']).sum().reset_index().sort_values('Value', ascending = False)
    total_shares = df['Value'].sum()
    df['percentage'] = df['Value'] / total_shares * 100
    df = df.rename(columns={'Ticker': 'symbol', 'Value': 'value','SharesPrnAmount':'share'})
    df['filing_date'] = filing['filing_date'] 
    df['filing_date'] = df['filing_date'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
    json_string = df.to_json(orient='records')
    recs = json.loads(json_string)
    try:
        nc.batch_upsert(target_table="form_13f_holdings", records=recs, conflict_columns=['filing_id','symbol'])
        print(filing['accession_number'], 'success')
    except:
        print(filing['accession_number'], 'fail')
        
    time.sleep(1)


