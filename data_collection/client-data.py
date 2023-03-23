from faker import Faker
import pandas as pd
import re
import openai
openai.api_key = 'XXXXXXXX'

# Import handler.csv and alien_species.csv
handler_df = pd.read_csv('handler.csv')
alien_df = pd.read_csv('alien_species.csv')

fkr = Faker()
client_id = []
coms_num = []
client_fname = []
client_lname = []


# Query ChatGPT API and return an answer
def get_answer(question):
    question = openai.Completion.create(
        engine='text-davinci-002',
        prompt=question,
        max_tokens=100,
        n=1,
        stop=None,
        temperature=0.5
    )
    return question.choices[0].text.strip()


# Take answer from get_answer() method and clean whitespace, numbers, periods, and newlines
def clean_answer(answer):
    return re.sub('[\d+\.]', '', answer)


# Append to client_fname and client_lname with returns from ChatGPT methods
# and to client_id and coms_num lists with data created from Faker
def clientdata(num_rows, query, query1):
    for element in range(num_rows):
        answer = get_answer(query)
        client_fname.append(clean_answer(answer))
        answer1 = get_answer(query1)
        client_lname.append(clean_answer(answer1))
        client_id.append('cc' + str(fkr.random_number(digits=5, fix_len=True)))
        coms_num.append(fkr.password(length=14, special_chars=False))


# Queries to generate data from ChatGPT
q_first_names = 'Generate 1 random science fiction character first name'
q_last_names = 'Generate 1 random science fiction character last name'

clientdata(600, q_first_names, q_last_names)
client_dataframe = pd.DataFrame(zip(client_id, client_fname, client_lname, coms_num), columns=['Client ID',
                                                                                               'First Name',
                                                                                               'Last Name',
                                                                                               'Comms Number'])
client_dataframe['Handler ID'] = handler_df['handler_id'].sample(n=600,  replace=True, ignore_index=True)
client_dataframe['Species ID'] = alien_df['Species ID'].sample(n=600, replace=True, ignore_index=True)

client_dataframe.to_csv('client.csv', index=False)
