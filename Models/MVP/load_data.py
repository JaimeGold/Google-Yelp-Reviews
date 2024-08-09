import pandas as pd
from google.cloud import bigquery
import os

def run_query(query):
  query_job = client.query(query)
  rows = query_job.to_dataframe()
  return rows

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials.json"

project_id = "braided-grammar-430922"
client = bigquery.Client()

query = "select * from datanexus.google_yelp_business limit 20000"
empresas = run_query(query)

gmap_ids = empresas['gmap_id'].unique()
gmap_ids_str = ', '.join(map(lambda x: f"'{x}'", gmap_ids))

query = f"select * from datanexus.google_yelp_reviews WHERE id in ({gmap_ids_str}) limit 20000"
reviews = run_query(query)

reviews.rename(columns={'id': 'gmap_id'}, inplace=True)

data = pd.merge(empresas, reviews, on='gmap_id')

data.head()
rating_avg =data.groupby('gmap_id').agg(
    rating_avg=('rating', 'mean'),  
    text_combined=('text', lambda x: ' '.join(x))
).reset_index()
print(rating_avg.head())

rating_avg.columns = ['gmap_id', 'rating', 'text']

data = pd.merge(empresas, rating_avg, on='gmap_id')

data.to_pickle('data//data.pkl')