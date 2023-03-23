from faker import Faker
from bs4 import BeautifulSoup
import pandas as pd
import requests
import re

fkr = Faker()
species_id = []
species = []

# Scrape content from swse.fandom.com using BeautifulSoup
species_content = requests.get(url='https://swse.fandom.com/wiki/Species')
species_page = BeautifulSoup(species_content.text, 'html.parser')
species_tables = species_page.find('table', class_='wikitable')


# Find species_text in each data cell in species_tables and append to species list and fill species_id list with data
# created in Faker
for row in species_tables.tbody.find_all('tr'):
    columns = row.find_all('td')
    if columns:
        species_text = re.sub('\*', '', columns[0].text.strip())
        species.append(species_text)
        species_id.append('ss' + str(fkr.random_number(digits=5, fix_len=True)))

species_data_frame = pd.DataFrame(zip(species_id, species), columns=['Species ID', 'Species'])

species_data_frame.to_csv('alien_species.csv', index=False)
