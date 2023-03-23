import pandas as pd

# Fill specialization_data list with dictionaries created by me
specialization_data = [{'Specialization ID': 'ss00',
                        'Specialize': 'Smuggling'},
                       {'Specialization ID': 'ss01',
                        'Specialize': 'Heavy Weaponry'},
                       {'Specialization ID': 'ss02',
                        'Specialize': 'Light Weaponry'},
                       {'Specialization ID': 'ss03',
                        'Specialize': 'Poison'},
                       {'Specialization ID': 'ss04',
                        'Specialize': 'Hand-to-hand Combat'},
                       {'Specialization ID': 'ss05',
                        'Specialize': 'Piloting'},
                       {'Specialization ID': 'ss06',
                        'Specialize': 'Explosives'},
                       {'Specialization ID': 'ss07',
                        'Specialize': 'Hacking'},
                       {'Specialization ID': 'ss08',
                        'Specialize': 'Medic'}
                       ]

specialization_data_frame = pd.DataFrame(specialization_data)

specialization_data_frame.to_csv('special.csv', index=False)
