import requests
from faker import Faker
import pandas as pd
from bs4 import BeautifulSoup
import re

fkr = Faker()
planet_id = []
planet_name = []

# Scrape content from screenrant.com using BeautifulSoup
starwars_content = requests.get(url='https://screenrant.com/star-wars-rogue-one-planets-ranked-jedha-scarif-tatooine/')
starwars_page = BeautifulSoup(starwars_content.text, 'html.parser')
find_planets = starwars_page.find_all('h2')

# Clean screenrant data and append names and ids to lists
for element in find_planets:
    element = re.sub('\d+\.\s', '', element.text.strip())
    planet_name.append(element)
    planet_id.append('pp' + str(fkr.random_number(digits=5, fix_len=True)))

planet_data_frame = pd.DataFrame(zip(planet_id, planet_name), columns=['Planet ID', 'Planet'])

planet_data_frame.to_csv('planets.csv', index=False)
