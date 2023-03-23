import pandas as pd

# Import planets.csv
planet_df = pd.read_csv('planets.csv')

# Fill handler_data list with dictionaries created by me
handler_data = [{'handler_id': 'hh00',
                 'first_name': 'Greef',
                 'last_name': 'Karga'},
                {'handler_id': 'hh01',
                 'first_name': 'Morgan',
                 'last_name': 'Northerniz'},
                {'handler_id': 'hh02',
                 'first_name': 'Jabba',
                 'last_name': 'The Hutt'},
                {'handler_id': 'hh03',
                 'first_name': 'Hando',
                 'last_name': 'Malteez'},
                {'handler_id': 'hh04',
                 'first_name': 'Ben',
                 'last_name': 'Kanobi'},
                {'handler_id': 'hh05',
                 'first_name': 'Mr.',
                 'last_name': 'X'},
                {'handler_id': 'hh06',
                 'first_name': 'Archie',
                 'last_name': 'Williams'},
                {'handler_id': 'hh07',
                 'first_name': 'Green',
                 'last_name': 'Blueman'}
                ]

handler_data_frame = pd.DataFrame(handler_data)
handler_data_frame['Planet ID'] = planet_df['Planet ID'].sample(n=8, replace=True, ignore_index=True)

handler_data_frame.to_csv('handler.csv', index=False)
