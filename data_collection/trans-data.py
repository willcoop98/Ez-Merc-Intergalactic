from faker import Faker
from datetime import datetime
import pandas as pd

# Import client.csv and job.csv
client_df = pd.read_csv('client.csv')
job_df = pd.read_csv('job.csv')

fkr = Faker()
trans_id = []
pay_amount = []
date_completed = []


# Append Faker data to trans_id, pay_amount, and date_completed lists
def transdata(num_rows):
    for i in range(num_rows):
        trans_id.append('tt' + str(fkr.random_number(digits=5, fix_len=True)))
        pay_amount.append(fkr.pricetag())
        date_completed.append(fkr.future_date(end_date=datetime(3000, 1, 2)))


transdata(600)
trans_dataframe = pd.DataFrame(zip(trans_id, pay_amount, date_completed), columns=["Transaction ID", "Payment Amount",
                                                                                   "Date Completed"])
trans_dataframe['Client ID'] = client_df['Client ID']
trans_dataframe['Job ID'] = job_df['Target ID']

trans_dataframe.to_csv('transaction.csv', index=False)
