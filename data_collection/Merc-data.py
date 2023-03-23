from faker import Faker
from bs4 import BeautifulSoup
import pandas as pd
import requests

# Import handler_id, alien_id
handler_df = pd.read_csv('handler.csv')
alien_df = pd.read_csv('alien_species.csv')

fkr = Faker()
merc_id = []
alias = []

# Scrape data from screenrant.com using BeautifulSoup
merc_content = requests.get(url='https://screenrant.com/star-wars-bounty-hunters-movies-shows-canon-explained/')
merc_page = BeautifulSoup(merc_content.text, 'html.parser')
merc_text = merc_page.find_all('h2')

# Append screenrant data and merc_id data imported form Faker to lists
for element in merc_text:
    merc_id.append('mm' + str(fkr.random_number(digits=5, fix_len=True)))
    alias.append(element.text)

merc_dataframe = pd.DataFrame(zip(merc_id, alias), columns=['Mercenary ID', 'Alias'])
merc_dataframe['Handler ID'] = handler_df['handler_id'].sample(n=64, replace=True, ignore_index=True)
merc_dataframe['Species ID'] = alien_df['Species ID'].sample(n=64, replace=True, ignore_index=True)

merc_dataframe.to_csv('mercenary.csv', index=False)
