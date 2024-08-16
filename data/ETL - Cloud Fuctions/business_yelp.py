import pandas as pd
from google.cloud import bigquery, storage

#Esta funcion asemeja a la función main 
def transform(event, context):
    """
    Obtenemos la ruta del bucket y el nombre del archivo de entrada, y lo descargamos al directorio temporal. Luego, cargamos el contenido del archivo .pkl en un DataFrame de pandas. Luego, lo procesamos y lo cargamos en BigQuery.
    
    Args:
        event (dict): Contiene la ruta del bucket y el nombre del archivo de entrada.
        context (google.cloud.functions.Context): Objeto de contexto de Google Cloud Functions.
    """
    # Obtenemos la ruta
    bucket_name = event["bucket"]
    file_name = event["name"]
    
    # Inicializa el cliente de GCS
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    
    # Descarga el archivo
    temp_file_path = f'/tmp/{file_name}'
    blob.download_to_filename(temp_file_path)
    print(f"File downloaded to: {temp_file_path}")

    # Carga el contenido del archivo .pkl en un DataFrame de pandas
    df = pd.read_pickle(temp_file_path)

    df = etl_process(bucket_name, file_name)

    load_byquery(df, bucket_name, file_name)

def etl_process(bucket_name, file_name):
    """
    Transforma el archivo .pkl en un DataFrame de pandas. Luego, lo procesa y lo retorna.
    
    Args:
        bucket_name (str): Nombre del bucket donde se encuentra el archivo .pkl.
        file_name (str): Nombre del archivo .pkl.
    
    Returns:
        df (DataFrame): DataFrame procesado.
    """
    # Leemos el parquet
    df = pd.read_pickle(f"gs://{bucket_name}/{file_name}")
    df = df.iloc[:, list(range(14)) + list(range(28, df.shape[1]))]

    # Rellenan solo en columnas 'state'
    df.loc[df['city'].str.contains('Santa Barbara', na=False), 'state'] = 'CA'

    # Eliminamos los nulso de la columna 'category'
    df = df.dropna(subset=['categories'])
    
    # Eliminamos los duplicados
    columns_to_convert = ['attributes', 'categories', 'hours']
    df[columns_to_convert] = df[columns_to_convert].applymap(str)
    df = df.drop_duplicates()
    
    # Realizamos un mask para de la columna categories que contenga 'restaurant'
    mask = df['categories'].str.contains("restaurant", case=False, na=False)
    
    # A esa mask lo filtramos por varios state 
    states = ['CA', 'TX', 'NY', 'PA', 'FL']
    mask = mask & (df['state'].isin(states))
    
    # Aplicamos el mask
    df = df[mask]
    
    # Transformamos la columna tipo de dato
    df['latitude'] = df['latitude'].astype(float)
    df['longitude'] = df['longitude'].astype(float)
    df['stars'] = df['stars'].astype(float)
    df['is_open'] = df['is_open'].astype(bool)
    df['review_count'] = df['review_count'].astype(int)
    
    try:
        df['postal_code'] = df['postal_code'].astype(int)
    except ValueError:
        df['postal_code'] = df['postal_code'].astype(str)
    
    except Exception as e:
        raise e
    
    df.reset_index(drop=True, inplace=True)
    
    return df


def load_byquery(df, bucket, name):
    """
    Cargamos el DataFrame en BigQuery creando el dataset si no existe.
    
    Args:
        df (DataFrame): DataFrame a cargar.
        bucket (str): Nombre del bucket donde se encuentra el archivo .pkl.
        name (str): Nombre del archivo .pkl. 
    """
    # Cliente de bigquery
    client = bigquery.Client()

    # crear el dataset
    dataset_id = 'braided-grammar-430922-b4.datanexus'
    dataset = bigquery.Dataset(dataset_id)
    # dataset.location = 'US'
    dataset = client.create_dataset(dataset, exists_ok=True)

    # Ruta de la tabla
    dataset_id = 'braided-grammar-430922-b4.datanexus'
    table_id = f'{dataset_id}.business_yelp'

    # Define el esquema de la tabla
    schema = [
    bigquery.SchemaField("business_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("address", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("city", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("state", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("postal_code", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("latitude", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("longitude", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("stars", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("review_count", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("is_open", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("attributes", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("categories", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("hours", "STRING", mode="NULLABLE"),
]
    
    # Configuracion del trabajo
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )

    # Carga los datos en la tabla de BigQuery
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    
    
def remove_duplicates(client, dataset_id, table_name):
    """
    Elimina registros duplicados de una tabla de BigQuery con una consulta SQL.
    
    Args:
        client (bigquery.Client): Objeto de cliente de BigQuery.
        dataset_id (str): ID del dataset.
        table_name (str): Nombre de la tabla.
    """
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