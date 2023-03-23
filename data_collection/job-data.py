from faker import Faker
import pandas as pd

# Import planets.csv, alien_species.csv, client.csv, and mercenary.csv
planet_df = pd.read_csv('planets.csv')
alien_df = pd.read_csv('alien_species.csv')
client_df = pd.read_csv('client.csv')
merc_df = pd.read_csv('mercenary.csv')

fkr = Faker()
target_id = []
target_fname = []
target_lname = []
target_occ = []


# Append data created in Faker to lists defined above
def targetdata(num_rows):
    for element in range(num_rows):
        target_id.append('tt' + str(fkr.random_number(digits=5, fix_len=True)))
        target_fname.append(fkr.last_name())
        target_lname.append(fkr.language_name())
        target_occ.append(fkr.job())


targetdata(600)
target_dataframe = pd.DataFrame(zip(target_id, target_fname, target_lname, target_occ),
                                columns=['Target ID', 'Target First Names', 'Target Last Names', 'Occupation'])
target_dataframe['Planet ID'] = planet_df['Planet ID'].sample(n=600, replace=True, ignore_index=True)
target_dataframe['Client ID'] = client_df['Client ID']
target_dataframe['Mercenary ID'] = merc_df['Mercenary ID'].sample(n=600, replace=True, ignore_index=True)
target_dataframe['Species ID'] = alien_df['Species ID'].sample(n=600, replace=True, ignore_index=True)

target_dataframe.to_csv('job.csv', index=False)
