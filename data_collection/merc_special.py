import pandas as pd

# Combine IDs from mercenary and specialize
merc_df = pd.read_csv('csv_files/mercenary.csv')
special_df = pd.read_csv('csv_files/special.csv')

merc_special_dataframe = pd.DataFrame()
merc_special_dataframe['Mercenary ID'] = merc_df['Mercenary ID']
merc_special_dataframe['Specialization ID'] = special_df['Specialization ID'].sample(n=64, replace=True,
                                                                                     ignore_index=True)

merc_special_dataframe.to_csv('csv_files/merc_special.csv', index=False)

