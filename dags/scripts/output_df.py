cimport pandas as pd
pd.options.display.max_columns = None
from airflow.models import Variable
from scraper import getLink
import requests 

g
def final_df():
    yr_mk_path = Variable.get('yr_mk_price')
    model_path = Variable.get('model_csv')
    description_path = Variable.get('description_csv')
    link_path = Variable.get('link_csv')

    yr_mk_price = pd.read_csv(yr_mk_path)
    model = pd.read_csv(model_path)
    description = pd.read_csv(description_path)
    link = pd.read_csv(link_path)

    df = pd.concat([yr_mk_price, model, description, link], axis=1)

    df = df[['Price', 'Year', 'Make', 'Model', 'Description', 'Url']]

    new_df = df.query('Year>= 1960 & Year <1980 & Make == ["Ford", "Dodge", "GMC", "Mercury", "Lincoln", "Chrysler", "Chevrolet"]')

    csv_path = Variable.get('final_df')

    return new_df.to_csv(csv_path,
              index=None, header=True, encoding='utf-8')