import datetime
from edgar import *
import pandas as pd

set_identity('Ger Sen wilsenp@gmail.com')

today = datetime.date.today()
current_year = today.year
df = get_filings(form="13F-HR", year=range(current_year-5,current_year)).to_pandas()
df = df[['cik','company']]
df = df.drop_duplicates()
df.rename(columns={'company': 'institution'}, inplace=True)

def get_instituion_holding_data(row):
  data = find(row['cik']).get_filings(form="13F-HR")[0].obj()
  accesion_number = data.filing.accession_no
  report_period = data.primary_form_information.report_period
  filing_date = data.filing.filing_date
  total_value = data.primary_form_information.summary_page.total_value
  total_holding = data.primary_form_information.summary_page.total_holdings
  return pd.Series([accesion_number, report_period, filing_date, total_value, total_holding], index = ['last_accesion_number','last_report_period','last_filing_date','total_value','total_holding'])

new_columns = df.apply(get_instituion_holding_data, axis=1)

final_df = pd.concat([df, new_columns], axis=1)
final_df[['last_report_period','last_filing_date']] = final_df[['last_report_period','last_filing_date']].apply(pd.to_datetime)
final_df['updated_on'] = pd.Timestamp.now()
final_df.to_csv('institutional_profile.csv', index = False)