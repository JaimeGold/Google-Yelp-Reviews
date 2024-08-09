import pandas as pd
from google.cloud import storage, bigquery

def transform(event, context):

    # Obtenemos la ruta
    bucket_name = event['bucket']
    file_name = event['name']
    
    # Inicializa el cliente de GCS
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    
    # Descarga el archivo
    temp_file_path = f'/tmp/{file_name}'
    blob.download_to_filename(temp_file_path)
    print(f'File downloaded to: {temp_file_path}')

    # Carga el contenido del archivo .json en un DataFrame de pandas

    df = etl_process(bucket_name, file_name)

    load_bigquery(df, bucket_name, file_name)

def etl_process(bucket_name, file_name):
    # Leemos el JSON
    df = pd.read_json(f'gs://{bucket_name}/{file_name}', lines=True)

    # Realizamos un mask para de la columna categories que contenga 'restaurant'
    df['category'] = df['category'].apply(lambda lista: [x.lower() for x in lista] if isinstance(lista, list) else lista)
    mask = df['category'].apply(lambda x: isinstance(x, list) and 'restaurant' in x)
    states = ['CA', 'TX', 'NY', 'PA', 'FL']
    mask = mask & (df['address'].str.contains('|'.join(states)))
    df = df[mask]
        
    # Eliminamos las columnas innecesarias
    df.drop(columns=['relative_results', 'price', 'state', 'url'], inplace=True)

    # Rellenamos los nulos
    df['address'].fillna('', inplace=True)
    df['description'].fillna('', inplace=True)
    df['hours'] = df['hours'].apply(lambda x: x if isinstance(x, list) else [])
    df['MISC'].fillna({}, inplace=True)

    # Spliteamos la columna address
    splited = df["address"].apply(lambda x: x.split(","))
    df["state"] = splited.apply(lambda x: x[-1].split(" ")[1] if len(x) > 1 else "")
    df["postal_code"] = splited.apply(lambda x: x[-1].split(" ")[-1] if len(x) > 1 else "")
    df["city"] = splited.apply(lambda x: x[-2] if len(x) > 1 else "")
    df["address"] = splited.apply(lambda x: x[0] if len(x) > 1 else "")

    del splited

    # Eliminamos los duplicados
    columns_to_convert = ['category', 'hours', 'MISC']
    df[columns_to_convert] = df[columns_to_convert].map(str)
    df.drop_duplicates(inplace=True)
    
    # Transformamos la columna tipo de dato
    df['name'] = df['name'].astype(str)
    df['address'] = df['address'].astype(str)
    df['gmap_id'] = df['gmap_id'].astype(str)
    df['description'] = df['description'].astype(str)
    df['latitude'] = df['latitude'].astype(float)
    df['longitude'] = df['longitude'].astype(float)
    df['avg_rating'] = df['avg_rating'].astype(float)
    df['num_of_reviews'] = df['num_of_reviews'].astype(int)
    df["city"] = df["city"].astype(str)
    df["state"] = df["state"].astype(str)
    df["postal_code"] = df["postal_code"].astype(str)

    df.reset_index(drop=True, inplace=True)
    
    return df

def load_bigquery(df, bucket, name):

    # Cliente de bigquery
    client = bigquery.Client()

    # Crear el dataset
    dataset_id = 'braided-grammar-430922-b4.datanexus'
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = 'southamerica-west1'
    dataset = client.create_dataset(dataset, exists_ok=True)
    
    # Ruta de la tabla
    table_id = f'{dataset_id}.metadata_google'

    # Define el esquema de la tabla
    schema = [
    bigquery.SchemaField('name', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('address', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('gmap_id', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('description', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('latitude', 'FLOAT', mode='REQUIRED'),
    bigquery.SchemaField('longitude', 'FLOAT', mode='REQUIRED'),
    bigquery.SchemaField('category', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('avg_rating', 'FLOAT', mode='REQUIRED'),
    bigquery.SchemaField('num_of_reviews', 'INTEGER', mode='REQUIRED'),
    bigquery.SchemaField('MISC', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('city', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('state', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('postal_code', 'STRING', mode='REQUIRED'),
    ]
    
    # Configuracion del trabajo
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )

    # Carga los datos en la tabla de BigQuery
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result() 

    #Remover duplicados de BigQuery
    remove_duplicates(client, dataset_id, 'reviews_google')


def remove_duplicates(client, dataset_id, table_name):
    # Nombre de la tabla temporal
    temp_table_id = f'{dataset_id}.temp_{table_name}'

    # Consulta SQL para crear una tabla temporal con registros únicos
    query = f'''
    CREATE OR REPLACE TABLE `{temp_table_id}` AS
    SELECT DISTINCT * FROM `{dataset_id}.{table_name}`
    '''

    # Ejecuta la consulta para crear la tabla temporal
    query_job = client.query(query)
    query_job.result()
    print(f'Temporary table {temp_table_id} created with unique records.')

    # Sobrescribir la tabla original con los datos únicos
    query = f'''
    CREATE OR REPLACE TABLE `{dataset_id}.{table_name}` AS
    SELECT * FROM `{temp_table_id}`
    '''

    # Ejecuta la consulta para sobrescribir la tabla original
    query_job = client.query(query)
    query_job.result()
    print(f'Table {dataset_id}.{table_name} overwritten with unique records.')
    
    # eliminar la tabla temporal
    client.delete_table(temp_table_id, not_found_ok=True)
    print(f'Temporary table {temp_table_id} deleted.')